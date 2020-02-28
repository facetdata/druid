package com.facetdata.inspector.stats;

import com.facetdata.inspector.Resource;
import com.facetdata.inspector.config.SqlDbConfig;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.StringMapper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlDbStatsManagerTest
{
  private SqlDbStatsManager sqlDBStatsManager;
  private EnumSet<GranularityType> defaultGranularities;
  private EnumSet<Resource> defaultResources;

  @Before
  public void setUp() throws Exception
  {
    ServiceEmitter emitter = new ServiceEmitter("test", "test", new NoopEmitter());
    EmittingLogger.registerEmitter(emitter);
    String url = "jdbc:h2:mem:testdb;MODE=MYSQL;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    sqlDBStatsManager = new SqlDbStatsManager(
        new SqlDbConfig(
            url,
            "",
            () -> "",
            "org.h2.Driver",
            null,
            null,
            null,
            null,
            null,
            null
        ),
        new AuditManager()
        {
          @Override
          public void doAudit(AuditEntry auditEntry)
          {

          }

          @Override
          public void doAudit(AuditEntry auditEntry, Handle handler)
          {

          }

          @Override
          public List<AuditEntry> fetchAuditHistory(String key, String type, Interval interval)
          {
            return null;
          }

          @Override
          public List<AuditEntry> fetchAuditHistory(String type, Interval interval)
          {
            return null;
          }

          @Override
          public List<AuditEntry> fetchAuditHistory(String key, String type, int limit)
          {
            return null;
          }

          @Override
          public List<AuditEntry> fetchAuditHistory(String type, int limit)
          {
            return null;
          }
        }
    );
    defaultGranularities = EnumSet.of(GranularityType.MINUTE);
    defaultResources = EnumSet.of(Resource.CPU);
    sqlDBStatsManager.init(
        defaultGranularities,
        defaultResources
    );
  }

  @After
  public void tearDown()
  {
    sqlDBStatsManager.getDbi().useHandle(handle -> handle.createStatement("DROP ALL OBJECTS").execute());
  }

  @Test
  public void testInit()
  {
    // tables should be created and default values should be present in the table
    sqlDBStatsManager.getDbi().useHandle(
        handle -> {
          List<String> tables = handle.createQuery(
              "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_name like 'resource_%'")
                                      .map(StringMapper.FIRST)
                                      .list();

          Assert.assertEquals(1 + defaultGranularities.size(), tables.size());
          List<String> expectedTables = new ArrayList<>();
          expectedTables.add(SqlDbStatsManager.RESOURCE_CAP_TABLE);
          defaultGranularities.forEach(granularityType -> expectedTables.add(
              StringUtils.format(
                  "%s_%sly",
                  SqlDbStatsManager.RESOURCE_USAGE_TABLE_BASE,
                  StringUtils.toLowerCase(granularityType.name())
              )));
          Assert.assertEquals(expectedTables, tables);

          // List of rows from select statement, Map contains column labels as key and its value for a row
          List<Map<String, Object>> caps = handle.createQuery(
              StringUtils.format("SELECT * FROM %s", SqlDbStatsManager.RESOURCE_CAP_TABLE))
                                                 .list();
          Assert.assertEquals(defaultGranularities.size(), caps.size());
          caps.forEach(
              columnLabelAndValue -> {
                Assert.assertEquals(
                    StatsManager.TENANT_ID_FOR_DEFAULT_CAPS,
                    columnLabelAndValue.get(SqlDbStatsManager.TENANT_ID_COLUMN_LABEL)
                );
                defaultResources.forEach(
                    resource ->
                        Assert.assertEquals(
                            resource.getDefaultCapPerGranularity(
                                GranularityType.valueOf(columnLabelAndValue.get(SqlDbStatsManager.GRANULARITY_COLUMN_LABEL)
                                                                           .toString())
                            ),
                            columnLabelAndValue.get(resource.getCapKey())
                        )
                );
              }
          );
        }
    );
  }

  @Test
  public void testGetStatsForTenant() throws Exception
  {
    int tenantId = 1;
    long bucketStartMillis = GranularityType.MINUTE.getDefaultGranularity().bucketStart(DateTimes.nowUtc()).getMillis();
    long cpuUsage = 17;
    sqlDBStatsManager.getDbi().useHandle(
        handle -> handle.createStatement(
            StringUtils.format(
                "INSERT INTO %s (tenant_id, period_start, cpu_usage) VALUES (%d, '%s', %s)",
                StringUtils.format("%s_%sly", SqlDbStatsManager.RESOURCE_USAGE_TABLE_BASE, GranularityType.MINUTE),
                tenantId,
                new Timestamp(bucketStartMillis),
                cpuUsage
            )).execute()
    );
    EnumMap<Resource, Long> stats = new EnumMap<>(Resource.class);
    stats.put(Resource.CPU, 17L);
    Assert.assertEquals(
        stats,
        sqlDBStatsManager.getStatsForTenant(tenantId, defaultResources, bucketStartMillis, GranularityType.MINUTE)
    );

    int nonExistingTenant = 100;
    stats.clear();
    stats.put(Resource.CPU, 0L);
    Assert.assertEquals(
        stats,
        sqlDBStatsManager.getStatsForTenant(
            nonExistingTenant,
            defaultResources,
            bucketStartMillis,
            GranularityType.MINUTE
        )
    );

    Assert.assertEquals(
        stats,
        sqlDBStatsManager.getStatsForTenant(
            StatsManager.TENANT_ID_FOR_DEFAULT_CAPS,
            defaultResources,
            bucketStartMillis,
            GranularityType.MINUTE
        )
    );
  }

  @Test
  public void testUpdateStatsForTenant() throws Exception
  {
    int tenantId = 1;
    long bucketStartMillis = GranularityType.MINUTE.getDefaultGranularity().bucketStart(DateTimes.nowUtc()).getMillis();
    long cpuUsage = 1;
    EnumMap<Resource, Long> delta = new EnumMap<>(Resource.class);
    delta.put(Resource.CPU, cpuUsage);
    sqlDBStatsManager.updateStatsForTenant(tenantId, delta, bucketStartMillis, GranularityType.MINUTE);
    Assert.assertEquals(
        delta,
        sqlDBStatsManager.getStatsForTenant(
            tenantId,
            EnumSet.copyOf(delta.keySet()),
            bucketStartMillis,
            GranularityType.MINUTE
        )
    );
    sqlDBStatsManager.updateStatsForTenant(tenantId, delta, bucketStartMillis, GranularityType.MINUTE);
    delta.put(Resource.CPU, cpuUsage + cpuUsage);
    Assert.assertEquals(
        delta,
        sqlDBStatsManager.getStatsForTenant(
            tenantId,
            EnumSet.copyOf(delta.keySet()),
            bucketStartMillis,
            GranularityType.MINUTE
        )
    );
  }

  @Test
  public void testGetCapsForTenantAndAddOrUpdateCapsForTenant() throws Exception
  {
    EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps = new EnumMap<>(GranularityType.class);
    tenantCaps.put(GranularityType.MINUTE, new EnumMap<>(Resource.class));
    tenantCaps.get(GranularityType.MINUTE).put(Resource.CPU, Resource.CPU.getDefaultCapPerGranularity(GranularityType.MINUTE));
    Assert.assertEquals(tenantCaps, sqlDBStatsManager.getCapsForTenant(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS));
    int tenantId = 1;
    Assert.assertEquals(
        new EnumMap<GranularityType, EnumMap<Resource, Long>>(GranularityType.class),
        sqlDBStatsManager.getCapsForTenant(tenantId)
    );
    sqlDBStatsManager.addOrUpdateCapsForTenant(tenantId, tenantCaps, new AuditInfo("", "", ""));
    Assert.assertEquals(tenantCaps, sqlDBStatsManager.getCapsForTenant(tenantId));
  }

  @Test
  public void testGetCapsForAllTenantsAndAddOrUpdateCapsForTenant() throws Exception
  {
    EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps = new EnumMap<>(GranularityType.class);
    tenantCaps.put(GranularityType.MINUTE, new EnumMap<>(Resource.class));
    tenantCaps.get(GranularityType.MINUTE).put(Resource.CPU, Resource.CPU.getDefaultCapPerGranularity(GranularityType.MINUTE));
    Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> caps = new HashMap<>();
    caps.put(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS, tenantCaps);
    Assert.assertEquals(caps, sqlDBStatsManager.getCapsForAllTenants());
    int tenantId = 1;
    sqlDBStatsManager.addOrUpdateCapsForTenant(tenantId, tenantCaps, new AuditInfo("", "", ""));
    caps.put(tenantId, tenantCaps);
    Assert.assertEquals(caps, sqlDBStatsManager.getCapsForAllTenants());
  }
}
