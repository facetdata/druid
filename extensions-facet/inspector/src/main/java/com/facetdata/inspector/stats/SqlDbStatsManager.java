package com.facetdata.inspector.stats;

import com.facetdata.inspector.Resource;
import com.facetdata.inspector.config.SqlDbConfig;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.RetryTransactionException;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Timestamp;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Stateless implementation of {@link StatsManager} which on each query goes to database to fetch/update tenant stats
 */
public class SqlDbStatsManager implements StatsManager
{
  private static final EmittingLogger log = new EmittingLogger(SqlDbStatsManager.class);

  public static final String RESOURCE_USAGE_TABLE_BASE = "resource_usage";
  public static final String RESOURCE_CAP_TABLE = "resource_caps";
  public static final String TENANT_ID_COLUMN_LABEL = "tenant_id";
  public static final String GRANULARITY_COLUMN_LABEL = "granularity";

  private final Pattern capKeyPattern = Pattern.compile(".+_cap");
  private final AuditManager auditManager;
  private final DBI dbi;
  private final int maxRetries;

  public SqlDbStatsManager(@JacksonInject SqlDbConfig sqlDBConfig, @JacksonInject AuditManager auditManager)
  {
    this.auditManager = auditManager;
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(sqlDBConfig.getConnectionURL());
    config.setUsername(sqlDBConfig.getUsername());
    config.setPassword(sqlDBConfig.getPasswordProvider().getPassword());
    config.setDriverClassName(sqlDBConfig.getDriverClassName());
    // from - https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    config.addDataSourceProperty("useServerPrepStmts", "true");

    if (sqlDBConfig.getConnectionTimeout() != null) {
      config.addDataSourceProperty("connectionTimeout", sqlDBConfig.getConnectionTimeout());
    }
    if (sqlDBConfig.getIdleTimeout() != null) {
      config.addDataSourceProperty("idleTimeout", sqlDBConfig.getIdleTimeout());
    }
    if (sqlDBConfig.getMaxLifetime() != null) {
      config.addDataSourceProperty("maxLifetime", sqlDBConfig.getMaxLifetime());
    }
    if (sqlDBConfig.getMinimumIdle() != null) {
      config.addDataSourceProperty("minimumIdle", sqlDBConfig.getMinimumIdle());
    }
    if (sqlDBConfig.getMaximumPoolSize() != null) {
      config.addDataSourceProperty("maximumPoolSize", sqlDBConfig.getMaximumPoolSize());
    }
    this.dbi = new DBI(new HikariDataSource(config));
    this.maxRetries = sqlDBConfig.getTransactionRetries();
    log.info("SqlDbStatsManager initialized with configs [%s]", sqlDBConfig);
  }

  @Override
  public void init(EnumSet<GranularityType> defaultGranularities, EnumSet<Resource> defaultResources) throws Exception
  {
    createCapTableIfNotPresent(defaultResources);
    createResourceUsageTablesIfNotPresent(defaultGranularities, defaultResources);
    // add default caps
    insertDefaultCapValuesIfNotPresent(defaultGranularities, defaultResources);
  }

  @VisibleForTesting
  public DBI getDbi()
  {
    return dbi;
  }

  private void createCapTableIfNotPresent(EnumSet<Resource> defaultResources) throws Exception
  {
    String resourcesColumns = defaultResources.stream().map(
        resource -> StringUtils.format(" `%s` bigint NOT NULL,\n", resource.getCapKey())
    ).collect(Collectors.joining());
    String createTableSql = StringUtils.format("CREATE TABLE IF NOT EXISTS `%s` (\n"
                                               + "  `tenant_id` int(11) NOT NULL,\n"
                                               + "  `granularity` varchar(255) NOT NULL,\n"
                                               + "%s"
                                               + "  PRIMARY KEY (`tenant_id`,`granularity`)\n"
                                               + ")", RESOURCE_CAP_TABLE, resourcesColumns);
    log.debug("Executing sql : [%s]", createTableSql);
    runWithRetry(
        (Task<Void>) () -> {
          dbi.useHandle(
              handle -> {
                if (tableNotExists(handle, RESOURCE_CAP_TABLE)) {
                  createTable(handle, createTableSql);
                }
              }
          );
          return null;
        },
        "Unable to create cap table",
        maxRetries
    );
  }

  private void createResourceUsageTablesIfNotPresent(
      EnumSet<GranularityType> defaultGranularities,
      EnumSet<Resource> defaultResources
  ) throws Exception
  {
    for (GranularityType granularityType : defaultGranularities) {
      String tableName = StringUtils.format(
          "%s_%sly",
          RESOURCE_USAGE_TABLE_BASE,
          StringUtils.toLowerCase(granularityType.name())
      );
      String baseExceptionMsg = StringUtils.format(
          "Unable to create [%s] table",
          tableName
      );
      String resourcesColumns = defaultResources.stream().map(
          resource -> StringUtils.format(" `%s` bigint NOT NULL,\n", resource.getUsageKey())
      ).collect(Collectors.joining());
      String createTableSql = StringUtils.format("CREATE TABLE IF NOT EXISTS `%s` (\n"
                                                 + "  `tenant_id` int(11) NOT NULL,\n"
                                                 + "  `period_start` datetime NOT NULL,\n"
                                                 + "%s"
                                                 + "  PRIMARY KEY (`tenant_id`,`period_start`)\n"
                                                 + ")", tableName, resourcesColumns);
      log.debug("Executing sql : [%s]", createTableSql);
      runWithRetry(
          (Task<Void>) () -> {
            dbi.useHandle(
                handle -> {
                  if (tableNotExists(handle, tableName)) {
                    createTable(handle, createTableSql);
                  }
                }
            );
            return null;
          },
          baseExceptionMsg,
          maxRetries
      );
    }
  }

  private void insertDefaultCapValuesIfNotPresent(
      EnumSet<GranularityType> defaultGranularities,
      EnumSet<Resource> defaultResources
  ) throws Exception
  {
    for (GranularityType granularityType : defaultGranularities) {
      String baseExceptionMsg = StringUtils.format("Unable to insert cap values in table [%s]", RESOURCE_CAP_TABLE);
      String resourcesColumns = getResourceCapsColumnInsertSqlString(defaultResources);
      EnumMap<Resource, Long> resourceDefaultValues = new EnumMap<>(Resource.class);
      defaultResources.forEach(resource -> resourceDefaultValues.put(
          resource,
          resource.getDefaultCapPerGranularity(granularityType)
      ));
      String resourcesColumnsDefaultValues = getResourceValuesInsertSqlString(resourceDefaultValues);
      String sqlInsert = StringUtils.format(
          "INSERT IGNORE INTO %s "
          + "(tenant_id, granularity, %s) "
          + "values (%d, '%s', %s)",
          RESOURCE_CAP_TABLE,
          resourcesColumns,
          TENANT_ID_FOR_DEFAULT_CAPS,
          granularityType.name(),
          resourcesColumnsDefaultValues
      );
      log.debug("Executing sql: [%s]", sqlInsert);
      runWithRetry(
          (Task<Void>) () -> {
            dbi.useHandle(
                handle -> {
                  int updatedRows = handle.createStatement(sqlInsert).execute();
                  if (updatedRows == 0) {
                    log.warn("Table already have default values for bucket [%s]", granularityType.name());
                  }
                }
            );
            return null;
          },
          baseExceptionMsg,
          maxRetries
      );
    }
  }

  /**
   * Can be called from multiple threads.
   */
  @Override
  public EnumMap<Resource, Long> getStatsForTenant(
      int tenantId,
      EnumSet<Resource> resources,
      long bucketStartMillis,
      GranularityType granularity
  ) throws Exception
  {
    String resourceSqlString = getResourceUsageColumnInsertSqlString(resources);
    String tableName = StringUtils.format("%s_%sly", RESOURCE_USAGE_TABLE_BASE, granularity);
    Timestamp periodStartTS = new Timestamp(bucketStartMillis);
    String sqlString = StringUtils.format(
        "SELECT tenant_id, period_start, %s FROM %s WHERE tenant_id = ? and period_start = ?",
        resourceSqlString,
        tableName
    );
    log.debug("Executing sql: [%s] with parameters [%s], [%s]", sqlString, tenantId, periodStartTS);
    String baseExceptionMsg = StringUtils.format(
        "Unable to get stats for tenant:[%s] for period:[%s]",
        tenantId,
        periodStartTS
    );
    return runWithRetry(
        () -> {
          final EnumMap<Resource, Long> stats = new EnumMap<>(Resource.class);

          dbi.useHandle(handle -> {
            handle.createQuery(sqlString)
                  .bind(0, tenantId)
                  .bind(1, periodStartTS)
                  .map((ResultSetMapper<Void>) (index, resultSet, ctx) -> {
                    for (Resource resource : resources) {
                      stats.put(resource, resultSet.getLong(resource.getUsageKey()));
                    }
                    return null;
                  }).first(); // .first to trigger the query execution
          });
          if (stats.isEmpty()) {
            log.debug("No rows found for this period [%s] and or tenant [%d]", periodStartTS, tenantId);
            for (Resource resource : resources) {
              stats.put(resource, 0L);
            }
          }
          return stats;
        },
        baseExceptionMsg,
        maxRetries
    );
  }

  /**
   * Can be called from multiple threads.
   */
  @Override
  public void updateStatsForTenant(
      int tenantId,
      EnumMap<Resource, Long> delta,
      long bucketStartMillis,
      GranularityType granularity
  ) throws Exception
  {
    Timestamp periodStartTS = new Timestamp(bucketStartMillis);
    String baseExceptionMsg = StringUtils.format(
        "Unable to update stats for tenant: [%d] for period: [%s] and granularity: [%s]",
        tenantId,
        periodStartTS,
        granularity
    );
    final String resourceSqlString = getResourceUsageColumnInsertSqlString(delta.keySet());
    final String insertValuesString = getResourceValuesInsertSqlString(delta);
    final String updateValuesString = getResourceValuesOnDuplicateInsertSqlString(delta);

    final String tableName = StringUtils.format("%s_%sly", RESOURCE_USAGE_TABLE_BASE, granularity);
    final String insertOnDuplicateUpdateSql = StringUtils.format(
        "INSERT INTO %s (tenant_id, period_start, %s) VALUES (%d, '%s', %s) ON DUPLICATE KEY UPDATE %s",
        tableName,
        resourceSqlString,
        tenantId,
        periodStartTS,
        insertValuesString,
        updateValuesString
    );
    runWithRetry(
        (Task<Void>) () -> {
          dbi.useTransaction((conn, status) -> {
            log.debug("Executing sql: [%s]", insertOnDuplicateUpdateSql);
            int insertedRows = conn.createStatement(insertOnDuplicateUpdateSql).execute();
            if (insertedRows == 0) {
              // actually this should not happen as if some problem occurs at db side then exception should be thrown
              throw new RetryTransactionException("INSERT into DB shouldn't result in 0 inserted rows, lets retry");
            }
          });
          return null;
        },
        baseExceptionMsg,
        maxRetries
    );
  }

  /**
   * Can be called from multiple threads.
   */
  @Override
  public EnumMap<GranularityType, EnumMap<Resource, Long>> getCapsForTenant(int tenantId) throws Exception
  {
    String sqlString = StringUtils.format(
        "SELECT * FROM %s WHERE tenant_id = ?",
        RESOURCE_CAP_TABLE
    );
    log.debug("Executing sql: [%s] with parameters [%s]", sqlString, tenantId);
    String baseExceptionMsg = StringUtils.format("Unable to get caps for tenant: [%d]", tenantId);

    return runWithRetry(
        () -> {
          EnumSet<Resource> resources = EnumSet.noneOf(Resource.class);
          EnumMap<GranularityType, EnumMap<Resource, Long>> caps = new EnumMap<>(GranularityType.class);

          dbi.useHandle(
              handle -> handle
                  .createQuery(sqlString)
                  .bind(0, tenantId)
                  .map((ResultSetMapper<Void>) (index, resultSet, ctx) -> {
                    if (resources.isEmpty()) {
                      resources.addAll(getResourcesFromResultSet(resultSet));
                    }
                    EnumMap<Resource, Long> resourceCaps = new EnumMap<>(Resource.class);
                    for (Resource resource : resources) {
                      resourceCaps.put(resource, resultSet.getLong(resource.getCapKey()));
                    }
                    caps.put(
                        GranularityType.valueOf(StringUtils.toUpperCase(resultSet.getString(GRANULARITY_COLUMN_LABEL))),
                        resourceCaps
                    );
                    return null;
                  }).list() // .list to trigger query execution
          );
          return caps;
        },
        baseExceptionMsg,
        maxRetries
    );
  }

  /**
   * Can be called from multiple threads. Currently being called from a single place.
   */
  @Override
  public Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> getCapsForAllTenants() throws Exception
  {
    String sqlString = StringUtils.format(
        "SELECT * FROM %s",
        RESOURCE_CAP_TABLE
    );
    String baseExceptionMsg = "Unable to get caps for all tenants";

    return runWithRetry(
        () -> {
          EnumSet<Resource> resources = EnumSet.noneOf(Resource.class);
          Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> caps = new HashMap<>();

          dbi.useHandle(
              handle -> {
                handle
                    .createQuery(sqlString)
                    .map((ResultSetMapper<Void>) (index, resultSet, ctx) -> {
                      if (resources.isEmpty()) {
                        resources.addAll(getResourcesFromResultSet(resultSet));
                      }
                      EnumMap<Resource, Long> resourceCaps = new EnumMap<>(Resource.class);
                      for (Resource resource : resources) {
                        resourceCaps.put(resource, resultSet.getLong(resource.getCapKey()));
                      }
                      int tenantId = resultSet.getInt(TENANT_ID_COLUMN_LABEL);
                      GranularityType granularity = GranularityType.valueOf(StringUtils.toUpperCase(resultSet.getString(
                          GRANULARITY_COLUMN_LABEL)));
                      caps.computeIfAbsent(tenantId, tId -> new EnumMap<>(GranularityType.class))
                          .put(granularity, resourceCaps);
                      return null;
                    }).list(); // .list to trigger query execution
              }
          );
          return caps;
        },
        baseExceptionMsg,
        maxRetries
    );
  }

  /**
   * If resource caps exist for some resources then they will be overwritten. If new granularities are present in the
   * {@param caps} map then they will be inserted. Before adding caps for new resources, resource_caps table need to be
   * modified to have columns for those resources. This method can be called by multiple users trying to add/update caps
   * for a particular tenant, its just that last one will win.
   */
  @Override
  public void addOrUpdateCapsForTenant(
      int tenantId,
      EnumMap<GranularityType, EnumMap<Resource, Long>> caps,
      AuditInfo auditInfo
  ) throws Exception
  {
    String baseExceptionMsg = StringUtils.format("Unable to add/update caps for for tenant: [%s]", tenantId);
    try {
      dbi.useTransaction((conn, status) -> {
        final DateTime auditTime = DateTimes.nowUtc();
        auditManager.doAudit(
            AuditEntry.builder()
                      .key(Integer.toString(tenantId))
                      .type("caps")
                      .auditInfo(auditInfo)
                      .payload(caps.toString())
                      .auditTime(auditTime)
                      .build(),
            conn
        );

        for (GranularityType granularity : caps.keySet()) {
          String insertOnDuplicateUpdateSql = StringUtils.format(
              "INSERT INTO %s (tenant_id, granularity, %s) VALUES (%d, '%s', %s) ON DUPLICATE KEY UPDATE %s",
              RESOURCE_CAP_TABLE,
              getResourceCapsColumnInsertSqlString(caps.get(granularity).keySet()),
              tenantId,
              granularity.name(),
              getResourceValuesInsertSqlString(caps.get(granularity)),
              getResourceCapValuesUpdateSqlString(caps.get(granularity))
          );
          log.debug("Executing sql: [%s]", insertOnDuplicateUpdateSql);

          int insertedRows = conn.createStatement(insertOnDuplicateUpdateSql).execute();
          if (insertedRows == 0) {
            // actually this should not happen as if some problem occurs at db side then exception should be thrown
            throw new SQLException("INSERT into DB shouldn't result in 0 inserted rows");
          }
        }
      });
    }
    catch (Exception e) {
      // Unlike methods which read the DB state like getCapsForAllTenants(), we don't want to retry
      // addOrUpdateCapsForTenant() and thus not calling handleException().
      log.makeAlert(e, baseExceptionMsg).emit();
      throw e;
    }
  }

  private boolean tableNotExists(Handle handle, String tableName)
  {
    return handle.createQuery(StringUtils.format(
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_name like '%s'",
        tableName
    )).list().isEmpty();
  }

  private void createTable(Handle handle, String createSql)
  {
    handle.createStatement(createSql).execute();
  }

  private String getResourceCapsColumnInsertSqlString(Set<Resource> resources)
  {
    return getResourceColumnInsertSqlString(resources, false);
  }

  private String getResourceUsageColumnInsertSqlString(Set<Resource> resources)
  {
    return getResourceColumnInsertSqlString(resources, true);
  }

  private String getResourceColumnInsertSqlString(Set<Resource> resources, boolean usage)
  {
    return resources.stream()
                    .map(resource -> usage ? resource.getUsageKey() : resource.getCapKey())
                    .collect(Collectors.joining(","));
  }

  private String getResourceValuesInsertSqlString(EnumMap<Resource, Long> deltaValues)
  {
    return deltaValues.values().stream().map(String::valueOf).collect(Collectors.joining(","));
  }

  private String getResourceValuesOnDuplicateInsertSqlString(EnumMap<Resource, Long> deltaValues)
  {
    return deltaValues.entrySet()
                      .stream()
                      .map(resourceUsage -> resourceUsage.getKey().getUsageKey()
                                            + " = "
                                            + resourceUsage.getKey().getUsageKey()
                                            + " + "
                                            + resourceUsage.getValue())
                      .collect(Collectors.joining(","));
  }

  private String getResourceCapValuesUpdateSqlString(EnumMap<Resource, Long> deltaValues)
  {
    return deltaValues.entrySet()
                      .stream()
                      .map(resourceUsage -> resourceUsage.getKey().getCapKey()
                                            + " = "
                                            + resourceUsage.getValue())
                      .collect(Collectors.joining(","));
  }

  private EnumSet<Resource> getResourcesFromResultSet(ResultSet resultSet) throws SQLException
  {
    EnumSet<Resource> resources = EnumSet.noneOf(Resource.class);
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int numColumns = resultSetMetaData.getColumnCount();
    for (int i = 1; i <= numColumns; i++) {
      String name = resultSetMetaData.getColumnLabel(i);
      if (capKeyPattern.matcher(name).matches()) {
        resources.add(Resource.valueOf(StringUtils.toUpperCase(name.substring(0, name.length() - "_cap".length()))));
      }
    }
    return resources;
  }

  private interface Task<T>
  {
    T perform() throws Exception;
  }

  private static <T> T runWithRetry(Task<T> task, String baseExceptionMsg, int maxRetries) throws Exception
  {
    int nTry = 1;
    while (true) {
      try {
        return task.perform();
      }
      catch (Exception e) {
        handleException(e, baseExceptionMsg, nTry, maxRetries);
      }
      nTry++;
    }
  }

  private static void handleException(Exception e, String baseExceptionMsg, int nTry, int maxRetries)
      throws Exception
  {
    if (isTransientException(e) && nTry <= maxRetries) {
      // retry again
      int waitMillis = 10 + ThreadLocalRandom.current().nextInt(90);
      log.warn(e, baseExceptionMsg + ", retrying [%d] of [%d] in [%d] millis", nTry, maxRetries, waitMillis);
      Thread.sleep(waitMillis);
    } else {
      log.makeAlert(e, baseExceptionMsg).emit();
      throw e;
    }
  }

  // copied from SQLMetadataConnector and MySQLConnector class in Druid code base
  private static boolean isTransientException(Throwable e)
  {
    return e != null && (e instanceof RetryTransactionException
                         || e instanceof SQLTransientException
                         || e instanceof SQLRecoverableException
                         || e instanceof UnableToObtainConnectionException
                         || e instanceof UnableToExecuteStatementException
                         || (e instanceof SQLException
                             && ((SQLException) e).getErrorCode() == 1317 /* ER_QUERY_INTERRUPTED */)
                         || (e instanceof SQLException && isTransientException(e.getCause()))
                         || (e instanceof DBIException && isTransientException(e.getCause())));
  }
}
