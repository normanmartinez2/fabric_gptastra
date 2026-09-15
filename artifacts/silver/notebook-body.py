"""Local-only Silver notebook body. Build the Fabric notebook; do not execute locally."""
import csv
import datetime
import decimal
import hashlib
import json
import re
from pathlib import Path, PurePosixPath
import notebookutils
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

def require(condition, message):
    if not condition:
        raise ValueError(message)

# CONFIG_SNAPSHOT and EXPECTED_LAKEHOUSE_ID are generated from project config
# and verified discovery, never independently entered environment defaults.
cfg = json.loads(config_json)
require(cfg == CONFIG_SNAPSHOT, 'Configuration differs from reviewed config/dev.json; regenerate notebook')
require(cfg['environment'] == 'dev', 'DEV only')

def verify_context():
    context = notebookutils.runtime.context
    require(context['currentWorkspaceId'] == cfg['workspace_id'], 'Wrong notebook workspace')
    require(context['defaultLakehouseWorkspaceId'] == cfg['workspace_id'], 'Wrong Lakehouse workspace')
    require(context['defaultLakehouseId'] == EXPECTED_LAKEHOUSE_ID, 'Wrong default Lakehouse ID')
    require(context['defaultLakehouseName'] == cfg['lakehouse_name'], 'Wrong default Lakehouse name')

verify_context()
spark.conf.set('spark.sql.ansi.enabled', 'true')
spark.conf.set('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
spark.conf.set('spark.sql.csv.parser.columnPruning.enabled', 'false')
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'static')

HEADERS = {
    'bookings': ['booking_id', 'passenger_id', 'flight_id', 'airport_id', 'amount', 'booking_date'],
    'passengers': ['passenger_id', 'name', 'gender', 'nationality'],
    'airports': ['airport_id', 'airport_name', 'city', 'country'],
}
SCHEMAS = {name: StructType([StructField(col,
    DecimalType(18, 2) if col == 'amount' else DateType() if col == 'booking_date' else StringType(),
    True) for col in cols]) for name, cols in HEADERS.items()}

def normalize(name):
    return re.sub(r'[^a-z0-9]+', '_', name.strip().lower()).strip('_')

def checked_path(key, prefix):
    raw = cfg['paths'][key].rstrip('/')
    p = PurePosixPath(raw)
    require(not p.is_absolute() and '..' not in p.parts and '\\' not in raw,
            'Unsafe path: ' + raw)
    require(raw.startswith(prefix + '/') and str(p) == raw, 'Unexpected path: ' + raw)
    return raw

output_path = checked_path('silver', 'Files/silver')
input_paths = {n: checked_path('bronze_' + n, cfg['paths']['bronze'].rstrip('/')) for n in HEADERS}
require(output_path not in input_paths.values(), 'Output overlaps input')
audit = {'input_rows': {}, 'input_sha256': {}, 'null_counts': {}, 'duplicate_excess': {},
         'unmatched_bookings': {}, 'output_path': output_path}

def preflight(name, relative_path):
    # Strict full-file CSV validation is deliberate for these small lab inputs.
    # Refuse files above 10 MiB rather than silently collecting arbitrary data.
    local = Path('/lakehouse/default') / relative_path
    require(local.is_file() and local.stat().st_size <= 10 * 1024 * 1024,
            name + ': missing file or exceeds reviewed 10 MiB lab limit')
    original = local.read_bytes()
    with local.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, [])
        normalized = [normalize(c) for c in header]
        require(len(normalized) == len(set(normalized)), name + ': normalized column collision')
        require(normalized == HEADERS[name], name + ': missing, extra, reordered, or unexpected columns')
        count = 0
        for row in reader:
            count += 1
            require(len(row) == len(header), name + ': malformed record width')
            if name == 'bookings':
                amount, date = row[4], row[5]
                if amount:
                    require(bool(re.fullmatch(r'-?\d{1,16}(\.\d{1,2})?', amount)),
                            'amount cannot be represented losslessly as decimal(18,2)')
                    require(decimal.Decimal(amount).is_finite(), 'Non-finite amount')
                if date:
                    require(bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}', date)), 'Date must be yyyy-MM-dd')
                    datetime.date.fromisoformat(date)
    require(count > 0, name + ': empty input')
    audit['input_sha256'][name] = hashlib.sha256(original).hexdigest()
    return count

frames = {}
for name, path in input_paths.items():
    expected_count = preflight(name, path)
    # Header has already been checked exactly after normalization; ordinal schema
    # assignment is safe, including when raw headers differ only in case/spaces.
    df = (spark.read.schema(SCHEMAS[name]).option('header', True)
          .option('enforceSchema', True).option('mode', 'FAILFAST')
          .option('multiLine', True).option('quote', '"').option('escape', '"')
          .option('dateFormat', 'yyyy-MM-dd').option('encoding', 'UTF-8').csv(path))
    df = df.toDF(*[normalize(c) for c in df.columns]).cache()
    require(df.columns == HEADERS[name], name + ': Spark column mismatch')
    count = df.count()
    require(count == expected_count, name + ': CSV/Spark row count mismatch')
    require(df.schema == SCHEMAS[name], name + ': Spark type mismatch')
    # Force all column parsing, not merely the count optimization.
    nulls = df.agg(*[F.sum(F.when(F.col(c).isNull() |
        (F.trim(F.col(c).cast('string')) == ''), 1).otherwise(0)).alias(c)
        for c in df.columns]).first().asDict()
    audit['null_counts'][name] = nulls
    audit['input_rows'][name] = count
    frames[name] = df

def valid_key(df, name, key, unique):
    require(audit['null_counts'][name][key] == 0, f'{name}.{key}: null/blank key')
    require(df.filter(F.col(key) != F.trim(F.col(key))).limit(1).count() == 0,
            f'{name}.{key}: whitespace in key; refusing implicit key repair')
    duplicates = df.count() - df.select(key).distinct().count()
    audit['duplicate_excess'][name + '.' + key] = duplicates
    if unique:
        require(duplicates == 0, f'{name}.{key}: duplicate primary key')

b, p, a = frames['bookings'], frames['passengers'], frames['airports']
valid_key(b, 'bookings', 'booking_id', True)
for key, dim, name in [('passenger_id', p, 'passengers'), ('airport_id', a, 'airports')]:
    valid_key(b, 'bookings', key, False)  # Repeated foreign keys are legitimate.
    valid_key(dim, name, key, True)
    unmatched = b.join(dim.select(key), key, 'left_anti').count()
    audit['unmatched_bookings'][key] = unmatched
    require(unmatched == 0, key + ': orphan bookings; unsafe inner join')
print(json.dumps(audit, sort_keys=True))

# Rename dimension attributes first, then use key-list joins which emit keys once.
passenger = p.select('passenger_id', F.col('name').alias('passenger_name'),
                     F.col('gender').alias('passenger_gender'),
                     F.col('nationality').alias('passenger_nationality'))
airport = a.select('airport_id', 'airport_name', 'city', F.col('country').alias('airport_country'))
bp = b.join(passenger, ['passenger_id'], 'inner')
require(bp.count() == audit['input_rows']['bookings'], 'Passenger join changed booking cardinality')
silver = bp.join(airport, ['airport_id'], 'inner').select(
    'booking_id', 'passenger_id', 'flight_id', 'airport_id', 'amount', 'booking_date',
    'passenger_name', 'passenger_gender', 'passenger_nationality',
    'airport_name', 'city', 'airport_country').cache()
require(len(silver.columns) == len(set(silver.columns)), 'Ambiguous final column names')
silver_count = silver.count()
require(silver_count == audit['input_rows']['bookings'], 'Airport join changed booking cardinality')
require(silver.select('booking_id').distinct().count() == silver_count, 'Duplicated Silver bookings')
require(silver.filter(F.col('city').isNull() | (F.trim('city') == '')).limit(1).count() == 0,
        'Airport city missing; Gold grouping would be unsafe')
audit['silver_rows'] = silver_count
audit['silver_schema'] = silver.schema.jsonValue()

# Validate again immediately before the only persistent write.
verify_context()
for name, path in input_paths.items():
    require(hashlib.sha256((Path('/lakehouse/default') / path).read_bytes()).hexdigest()
            == audit['input_sha256'][name], 'Bronze changed during processing')
silver.write.mode('overwrite').format('parquet').save(output_path)
written = spark.read.parquet(output_path)
require([(f.name, f.dataType) for f in written.schema] ==
        [(f.name, f.dataType) for f in silver.schema], 'Parquet schema readback mismatch')
require(written.count() == silver_count, 'Parquet row count readback mismatch')
require(silver.exceptAll(written).limit(1).count() == 0 and
        written.exceptAll(silver).limit(1).count() == 0, 'Parquet content readback mismatch')
audit['status'] = 'Succeeded'
print(json.dumps(audit, sort_keys=True))
for df in frames.values():
    df.unpersist()
silver.unpersist()
# Uncaught errors fail the Notebook activity; no success exit on validation failure.
