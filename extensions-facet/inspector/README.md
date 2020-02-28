Overview
--------
This extension is used for enforcing and updating cluster resource usage for tenants when they send queries to the 
cluster. This extension is supposed to run at all Router nodes. 

This extension injects a `ServletFilter` in the filter chain of Router server. Whenever a query is received this filter
will check the resource quotas left for specified granularities (by default minutely and hourly) for that tenant and 
decide to allow or deny the query. If query is allowed then after query processing is done the consumed resources
information is updated for specified granularities (buckets).

Details
-------
Whenever a query is received on `/druid/v2/` path, the extension looks for a Header containing tenant id information, 
if this is not present the query will fail with exception. 

Current resource usage information for resources for different granularities is fetched using the `StatsManager` and  
quota is checked for them. Resources and Granularities to use in these checks will be based on cap information of resources 
for different granularities present in the resource_caps table for the tenant. If caps information is not for the given tenant 
then caps for default tenant will be used and an alert will be sent for first time this happens.

See InspectorConfig#DEFAULT_RESOURCES and InspectorConfig#DEFAULT_GRANULARITIES for defaults. 

After the query is processed a Header containing information about resources usage for this query is extracted, if not 
present then query will fail with an exception.

For getting and updating stats about resource usage `StatsManager` interface is provided. Currently `DatabaseStatsManager`
is the only implementation which is completely stateless and on each query goes to database to fetch/update tenant stats.
`DatabaseStatsManager` uses connection pool to query/update DB to prevent overhead associated with creating connection 
to DB in the query path.

`ResourceInspectorFilter` is the class from where all the logic is called. It uses `Inspector` (which uses `StatsManager`)
for resource usage checking and updating. `Inspector` uses a thread pool to perform async update of resource usages to
prevent holding back the response. 

Configs
------
|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`facet.quotasInspector.statsManager.type`|StatsManager to use. Options - "sqldb"|none|yes|
|`facet.quotasInspector.statsManager.sqldb.connectionURL`|Use with sqldb StatsManager. Connection URL to DB|none|yes|
|`facet.quotasInspector.statsManager.sqldb.user`|Use with sqldb StatsManager. user for DB|none|yes|
|`facet.quotasInspector.statsManager.sqldb.password`|Use with sqldb StatsManager. `PasswordProvider` to get DB password|none|yes|
|`facet.quotasInspector.statsManager.database.connectionTimeout`|Use with sqldb StatsManager. Connection timeout in millis for waiting to get a connection from the pool|250|no|
|`facet.quotasInspector.statsManager.database.idleTimeout`|Use with sqldb StatsManager. Maximum amount of time in millis that a connection is allowed to sit idle in the pool|60000|no|
|`facet.quotasInspector.statsManager.database.maxLifetime`|Use with sqldb StatsManager. Maximum lifetime of a connection in the pool|1800000|no|
|`facet.quotasInspector.statsManager.database.minimumIdle`|Use with sqldb StatsManager. Minimum number of idle connections that HikariCP tries to maintain in the pool|same as maximumPoolSize|no|
|`facet.quotasInspector.statsManager.database.maximumPoolSize`|Use with sqldb StatsManager. Maximum size that the pool is allowed to reach, including both idle and in-use connections|10|no|
|`facet.quotasInspector.config.capsSyncPeriodSeconds`|Sync period for tenant caps in seconds|60|no|

`SqlDbStatsManager` uses HikariCP library for connection pooling. More detailed information about configs can be 
found [here](https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby).

Database Tables
-----------
Certain conventions are being set in regards to the table and key/column names in the code.

|Table Name|Description|Column{Type}|
|----------|-----------|------------|
|resource_caps|Table for storing usage caps for resources per tenant, per granularity|tenant_id{Integer}<br>granularity{String}</br><br>[**_`resource`_**_usage{long}, ...]</br>|
|resource_usage_**_`granularity`_**|Table for storing current resource usage stats in buckets represented by _`period_start`_ column and **_granularity_** type|tenant_id{Integer}<br>period_start{DateTime}</br><br>[**_`resource`_**_usage{long}, ...]</br>|

where <br>
**_`resource`_** can be _`cpu`_, _`memory`_, ... <br>
**_`granularity`_** can be _`minutely`_, _`hourly`_ ...

HOW-TOs
-----
1. Add/modify caps for new tenants - POST a json map `{GranularityType -> {Resource -> Long}}` to the endpoint 
`/druid-ext/inspector/v1/stats/caps/{tenantId}` for adding/updating caps for tenant having `tenantId`. For example, adding
minutely cpu cap of 1.97 for tenant 4 can be done like this -
```bash
curl -X 'POST' -H 'Content-Type:application/json' -d '{"MINUTE":{"CPU":"10000"}}' http://<router-host:port>/druid-ext/inspector/v1/stats/caps/4
```
2. Add new resource - This is a manual process and there are several steps -
  - Alter resource_caps and resource_usage tables to add columns for this resource types.
  - Add new Resource Enum in the code, deploy new code.

Deployment
----------
Before enabling this extension at Router nodes, one has to make sure that Historicals, Middle Managers, Brokers and clients
are sending expected information in request-response headers otherwise queries will fail.  

TODO
----
1. Implement another `ServletFilterHolder` for SQL query path.
