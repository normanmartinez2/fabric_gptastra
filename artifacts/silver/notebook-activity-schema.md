### TridentNotebook Activity Properties

Properties for activities with `type: "TridentNotebook"`.

| Name                  | Type            | Required        | Description       |
|-----------------------|-----------------|-----------------|-------------------|
| typeProperties        | [TridentNotebookActivityTypeProperties](#tridentnotebook-activity-type-properties) | true   | The properties for Trident notebook activity |
| externalReferences    | [ExternalReferences](#external-references)| false | Reference to the connection.|

#### TridentNotebook Activity Type Properties

| Name                  | Type            | Required        | Description       |
|-----------------------|-----------------|-----------------|-------------------|
| notebookId            | String          | true            | Notebook ID |
| workspaceId           | String          | true            | Workspace ID |
| parameters            | Object          | false           | Parameters to be used for each run of this job. If the notebook takes a parameter that is not specified, the default value from the notebook will be used |
| sessionTag            | String          | false           | Spark session tag |


