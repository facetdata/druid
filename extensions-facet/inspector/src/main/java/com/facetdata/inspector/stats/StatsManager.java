package com.facetdata.inspector.stats;

import com.facetdata.inspector.Resource;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.java.util.common.granularity.GranularityType;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

/**
 * This class is responsible for retrieving/updating usage stats for tenants.
 * A database implementation is provided which is totally stateless and on each query which goes to database
 * to fetch/update the stats. A cached implementation can be provided for performance reasons trading-off accuracy.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "sqldb", value = SqlDbStatsManager.class)
})
public interface StatsManager
{
  /**
   * Tenant id used for storing default caps on resources in DB. These default caps are used when no cap information is
   * found for a tenant in the DB.
   */
  int TENANT_ID_FOR_DEFAULT_CAPS = -1;

  /**
   * Perform any initialization logic here.
   */
  void init(EnumSet<GranularityType> defaultGranularities, EnumSet<Resource> defaultResources) throws Exception;

  /**
   * Get usage stats for tenant for a set of resources starting from time represented by {@param bucketStartMillis}
   * for a period represented by {@param granularity}.
   * For example, one can ask for current minute usage of CPU resource for a tenant with id 1 by calling this method like -
   * <pre>{@code
   *   getStatsForTenant(
   *               1,
   *               EnumSet.of(Resource.CPU),
   *               GranularityType.MINUTE.getDefaultGranularity().bucketStart(DateTimes.nowUtc()).getMillis(),
   *               GranularityType.MINUTE
   *               )
   * }</pre>
   *
   * @param tenantId          Id of the tenant
   * @param resources         Set of {@link Resource} for which usage stats is required
   * @param bucketStartMillis start millis of the time period for which usage stats is needed
   * @param granularity       {@link GranularityType} representing period for getting usage stats
   *
   * @return map of resource to usage value
   */
  EnumMap<Resource, Long> getStatsForTenant(
      int tenantId,
      EnumSet<Resource> resources,
      long bucketStartMillis,
      GranularityType granularity
  ) throws Exception;

  /**
   * Update usage stats for tenant for a set of resources for the time period represented by {@param bucketStartMillis}
   * and {@param granularity}.
   * For example, one can ask to update current hour usage of CPU resource for a tenant with id 1 by calling this method like -
   * <pre>{@code
   *   updateStatsForTenant(
   *               1,
   *               EnumMap of {Resource.CPU -> 20},
   *               GranularityType.HOUR.getDefaultGranularity().bucketStart(DateTimes.nowUtc()).getMillis(),
   *               GranularityType.HOUR
   *               )
   * }</pre>
   *
   * @param tenantId          Id of the tenant
   * @param delta             map of {@link Resource} to usage values to update
   * @param bucketStartMillis start millis of the time period for which usage stats needs to be updated
   * @param granularity       {@link GranularityType} representing period for updating usage stats
   */
  void updateStatsForTenant(
      int tenantId,
      EnumMap<Resource, Long> delta,
      long bucketStartMillis,
      GranularityType granularity
  ) throws Exception;

  /**
   * Used to retrieve caps per granularity period on {@link Resource} usage for a tenant.
   * For example, if a tenant has a cap of 100 for CPU per minute and 1000 per hour then return value would look like
   * <pre>
   * {
   *  GranularityType.MINUTE -> {Resource.CPU -> 100},
   *  GranularityType.HOUR -> {Resource.CPU -> 1000}
   * }
   * </pre>
   *
   * @param tenantId Id of the tenant
   *
   * @return map representing caps on resources per granularity period
   */
  EnumMap<GranularityType, EnumMap<Resource, Long>> getCapsForTenant(int tenantId) throws Exception;

  /**
   * Used to retrieve caps per granularity period on {@link Resource} usage for all tenants.
   * For example, if there are two tenants with id "1" and "2" having a cap of 100 for CPU per minute and 1000 per hour
   * then return value would look like -
   * <pre>
   * {1 ->
   *  {
   *    GranularityType.MINUTE -> {Resource.CPU -> 100},
   *    GranularityType.HOUR -> {Resource.CPU -> 1000}
   *  },
   * 2 ->
   *  {
   *    GranularityType.MINUTE -> {Resource.CPU -> 100},
   *    GranularityType.HOUR -> {Resource.CPU -> 1000}
   *  }
   * }
   * </pre>
   *
   * @return map representing caps on resources per granularity period for all tenants
   */
  Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> getCapsForAllTenants() throws Exception;

  /**
   * Used to update/add resource cap information about a tenant.
   *
   * @param tenantId  tenant id for which add/update should be done\
   * @param caps      {Granularity -> {Resource -> Cap Value, ...}, ...}
   * @param auditInfo Information about the author making the update
   */
  void addOrUpdateCapsForTenant(
      int tenantId,
      EnumMap<GranularityType, EnumMap<Resource, Long>> caps,
      AuditInfo auditInfo
  ) throws Exception;

}
