BigQuery Connector Extension
----------
This extension enables ingesting data into Druid from [Google Big Query](https://cloud.google.com/bigquery).

### Connecting to BigQuery
Connection to BigQuery is implemented utilizing [Simba JDBC driver](https://www.simba.com/drivers/bigquery-odbc-jdbc/).  
Authentication is performed using Service Account (JSON). For ingestion to work properly Service Account should have the
following permissions:
```
BigQuery Data Viewer
BigQuery Job User
BigQuery Read Session User
```

Service Account (JSON) file is selected in UI and its contents are passed in ingestion spec:
`ioConfig > firehose > database > connectorConfig > serviceAccount`

#### Installing JDBC driver
BigQuery JDBC driver and all accompanying libraries should be copied to `druid/extensions/bigquery-connector/` folder.  
One of the following approaches can be used:
- JDBC driver can be downloaded from official link [here](https://cloud.google.com/bigquery/providers/simba-drivers/).
Worth mentioning that there are some dependency version conflicts. A workaround is to replace
`google-api-services-bigquery-v2-rev426-1.25.0.jar` with [google-api-services-bigquery-v2-rev434-1.22.0.jar](https://mvnrepository.com/artifact/com.google.apis/google-api-services-bigquery/v2-rev434-1.22.0).  
- there's a JDBC driver bundle with fixed dependency version conflicts for Druid which can be downloaded from [here](http://pkg.facetdata.com/jdbc/simba-bigquery-jdbc-1.2.2.1004.tar.gz)


### Parallel ingestion
BigQuery connector implementation is mainly based on existing `SqlFirehoseFactory`. One major difference between
`BigQueryFirehoseFactory` and `SqlFirehoseFactory` is that the former also supports parallel
([index_parallel](https://druid.apache.org/docs/latest/ingestion/native-batch.html#parallel-task)) batch indexing.
Ingestion is split into multiple tasks where each task ingests data only for a certain (configurable) period e.g. day.
To achieve that, each task is assigned a generated SQL filter that filters underlying BigQuery dataset according to
timestamp column which is selected in Druid console and passed as part of ingestion spec:
e.g.: `WHERE timestamp_column >= 2019-12-11 AND timestamp_column < 2019-12-12`. For initial version - the whole ingestion
date range needs to be specified explicitly in ingestion spec.

### Initializing BigQuery ingestion
To use BigQuery connector extension, make sure you [include](../../docs/development/extensions.md#loading-extensions) the extension in your config file:
```
druid.extensions.loadList=[<other extensions> ..., "bigquery-connector"]
```
The quickest way to initialize `BigQueryFirehoseFactory` is using "Load Data > Google BigQuery" section in Druid web console.
All required initialization parameters have corresponding UI selectors and ingestion spec is generated according to selected
values.

**Sample ioConfig for BigQuery ingestion**  
```
"ioConfig": {
    "type": "index_parallel",
    "firehose": {
      "type": "bigquery",
      "splitGranularity": "DAY",                   // Granularity value used for task sharding
      "database": {
        "connectorConfig": {
          "queryTimeout": 3600,                    // query timeout value in seconds
          "serviceAccount": {...}                  // contents of Service Account (JSON)
        },
        "type": "bigquery"
      },
      "tables": [
        "myDataset.myTable"                        // BigQuery dataset and table to ingest the data from (optional)
      ],
      "sqls": [                                    // array of SQL scripts to be used in data ingestion
        "SELECT * FROM myDataset.myTable"          // each script from this array would later be sharded
      ],                                           // according to ingestionDateRange
      "timestampColumn": "timestamp_column",       // name of the timestamp column
      "ingestionDateRange": {                      // ingestion date range is specified here
        "dateFrom": "2020-01-01T00:00:00.000Z",    // dates are formated in ISO 8601
        "dateTo": "2020-02-01T00:00:00.000Z"
      }
    }
  }
```