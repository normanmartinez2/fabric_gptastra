"""Gold city aggregation. Local design only; execution is not authorized yet."""
import json
import re
from pathlib import PurePosixPath
import notebookutils
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DateType

def require(ok, message):
    if not ok:
        raise ValueError(message)

cfg = json.loads(config_json)
require(cfg == CONFIG_SNAPSHOT and cfg['environment'] == 'dev', 'Unreviewed configuration or non-DEV target')

def check_context():
    ctx = notebookutils.runtime.context
    require(ctx['currentWorkspaceId'] == cfg['workspace_id'], 'Wrong Notebook workspace')
    require(ctx['defaultLakehouseWorkspaceId'] == cfg['workspace_id'], 'Wrong Lakehouse workspace')
    require(ctx['defaultLakehouseId'] == EXPECTED_LAKEHOUSE_ID, 'Wrong Lakehouse ID')
    require(ctx['defaultLakehouseName'] == cfg['lakehouse_name'], 'Wrong Lakehouse name')

check_context()
def path(key, prefix):
    value = cfg['paths'][key].rstrip('/')
    p = PurePosixPath(value)
    require(not p.is_absolute() and '..' not in p.parts and '\\' not in value
            and str(p) == value and value.startswith(prefix + '/'), 'Unsafe path')
    return value

source = path('silver', 'Files/silver')
target = path('gold', 'Files/gold')
require(source != target, 'Input/output overlap')
spark.conf.set('spark.sql.ansi.enabled', 'true')
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'static')

# Read stored types, not a supplied schema that could hide schema drift.
silver = spark.read.option('mergeSchema', True).parquet(source).cache()
expected_input = [
    ('booking_id', StringType()), ('passenger_id', StringType()), ('flight_id', StringType()),
    ('airport_id', StringType()), ('amount', DecimalType(18, 2)), ('booking_date', DateType()),
    ('passenger_name', StringType()), ('passenger_gender', StringType()),
    ('passenger_nationality', StringType()), ('airport_name', StringType()),
    ('city', StringType()), ('airport_country', StringType())]
normalized = [re.sub(r'[^a-z0-9]+', '_', c.strip().lower()).strip('_') for c in silver.columns]
require(len(normalized) == len(set(normalized)), 'Ambiguous duplicate columns')
city_candidates = [c for c, n in zip(silver.columns, normalized) if 'city' in n.split('_')]
require(city_candidates == ['city'], 'City is missing or ambiguous; review lineage before proceeding')
require([(f.name, f.dataType) for f in silver.schema] == expected_input, 'Silver schema mismatch')
input_rows = silver.count()
require(silver.filter(F.col('booking_id').isNull() | (F.trim('booking_id') == '')).limit(1).count() == 0,
        'Missing booking identity')
require(silver.select('booking_id').distinct().count() == input_rows, 'Duplicate booking identity')

# Java Unicode whitespace class: blank includes tabs, line breaks and Unicode spaces.
# Do not trim/case-fold valid labels or combine different named cities implicitly.
blank = F.col('city').rlike(r'(?U)^\s*$')
null_rows = silver.filter(F.col('city').isNull()).count()
blank_rows = silver.filter(F.col('city').isNotNull() & blank).count()
eligible = silver.filter(F.col('city').isNotNull() & ~blank)
eligible_rows = eligible.count()
excluded_rows = null_rows + blank_rows
require(input_rows == eligible_rows + excluded_rows, 'Eligibility reconciliation failed')
metrics = dict(input_rows=input_rows, eligible_rows=eligible_rows, excluded_rows=excluded_rows,
               null_city_rows=null_rows, blank_city_rows=blank_rows, output_path=target)
print(json.dumps(metrics, sort_keys=True))
# Avoid overwriting a valid previous output with an all-excluded/empty batch.
require(eligible_rows > 0, 'No eligible bookings; Gold output not overwritten')

OUTPUT_SCHEMA = StructType([StructField('city', StringType(), True),
                            StructField('booking_count', LongType(), True)])
gold = (eligible.groupBy('city').agg(F.count(F.lit(1)).cast(LongType()).alias('booking_count'))
        .select(F.col('city').cast(StringType()).alias('city'),
                F.col('booking_count').cast(LongType()).alias('booking_count')).cache())

def validate_output(frame):
    require([(f.name, f.dataType) for f in frame.schema] ==
            [(f.name, f.dataType) for f in OUTPUT_SCHEMA], 'Gold output must be city string, booking_count long')
    require(frame.filter(F.col('city').isNull() | F.col('city').rlike(r'(?U)^\s*$') |
            F.col('booking_count').isNull() | (F.col('booking_count') <= 0)).limit(1).count() == 0,
            'Invalid city or booking count')
    count = frame.count()
    require(count == frame.select('city').distinct().count(), 'Duplicate output city')
    total = frame.agg(F.sum('booking_count')).first()[0]
    require(total == eligible_rows, 'sum(booking_count) differs from eligible Silver count')
    return count, total

output_rows, booking_total = validate_output(gold)
require(output_rows == eligible.select('city').distinct().count(), 'Missing city groups')
check_context()  # Verify the configured DEV target immediately before the only write.
gold.write.mode('overwrite').format('parquet').save(target)
written = spark.read.parquet(target)
require(validate_output(written) == (output_rows, booking_total), 'Gold readback metrics differ')
require(gold.exceptAll(written).limit(1).count() == 0 and
        written.exceptAll(gold).limit(1).count() == 0, 'Gold readback values differ')
metrics.update(output_rows=output_rows, booking_count_sum=booking_total, status='Succeeded',
               output_schema=OUTPUT_SCHEMA.simpleString())
print(json.dumps(metrics, sort_keys=True))
silver.unpersist()
gold.unpersist()
# Validation/write errors propagate and fail the Notebook activity.
