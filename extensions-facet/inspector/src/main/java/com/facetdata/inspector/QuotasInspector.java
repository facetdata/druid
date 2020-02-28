package com.facetdata.inspector;

import com.facetdata.inspector.config.InspectorConfig;
import com.facetdata.inspector.http.ResourceInspectorFilter;
import com.facetdata.inspector.stats.StatsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * QuotasInspector is responsible for inspecting (checking and enforcing) and updating usage stats for each query received.
 * Methods of this class can be called from multiple threads by {@link ResourceInspectorFilter} so care should be taken
 * while accessing/modifying class level variables like {@link QuotasInspector#caps}
 */
public class QuotasInspector
{
  private static final EmittingLogger log = new EmittingLogger(QuotasInspector.class);

  /**
   * TenantId -> {Granularity -> {Resource -> cap_value}}
   * <p>
   * Using a map of resource to cap value here for each granularity because just in case in future if we want to monitor
   * more cluster resources like Memory or disk usage etc. it can be done easily. Also using granularity instead of
   * hardcoding minute and hour periods for the same reason.
   * <p>
   * For example, if there are two tenants with id "1" and "2" having a cap of 100 for CPU per minute and 1000 per hour
   * then the map will look like -
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
   */
  private final ConcurrentHashMap<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> caps;
  private final EnumMap<GranularityType, EnumMap<Resource, Long>> defaultCaps;
  private final ListeningScheduledExecutorService scheduledUpdateExecutorService;
  private final StatsManager statsManager;
  private final ServiceEmitter emitter;

  @Inject
  public QuotasInspector(InspectorConfig inspectorConfig) throws Exception
  {
    final long start = System.currentTimeMillis();
    this.statsManager = inspectorConfig.getStatsManager();
    this.emitter = inspectorConfig.getEmitter();
    this.scheduledUpdateExecutorService = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded(
        "Update-Caps-[%d]"));
    statsManager.init(InspectorConfig.DEFAULT_GRANULARITIES, InspectorConfig.DEFAULT_RESOURCES);
    // retrieve caps for all tenants beforehand, if new tenants are added or caps are updated then they will be fetched on fly
    this.caps = new ConcurrentHashMap<>(statsManager.getCapsForAllTenants());
    this.defaultCaps = Preconditions.checkNotNull(caps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS));
    // schedule period sync of caps data
    scheduledUpdateExecutorService.scheduleWithFixedDelay(() -> {
      try {
        final Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> newCaps = statsManager.getCapsForAllTenants();
        caps.putAll(newCaps);
      }
      catch (Throwable t) {
        log.makeAlert(t, "Unable to add/update caps for all tenants").emit();
      }
    }, 1, inspectorConfig.getCapsSyncPeriodSeconds(), TimeUnit.SECONDS);
    final long end = System.currentTimeMillis();
    log.info("QuotasInspector initialized in [%s] millis with configs [%s]", end - start, inspectorConfig);
  }

  /**
   * This method is responsible for checking and enforcing caps on a particular resource usage. Can be called from multiple
   * threads.
   * <p>
   * Current usage value of the resources for different granularity for the tenant will be fetched from {@link StatsManager}
   * and check will be performed if the tenant quota is exhausted or not. Resources and Granularities to use in these checks
   * will be based on cap information of resources for different granularities present in the resource_caps table. If caps
   * are not found for the given tenant then caps for default tenant will be used and an alert will be sent for first time this happens.
   * <p>
   * For example, to enforce minutely and hourly usage caps on CPU resources for tenant with id "1", the method call will be like -
   *
   * <pre> {@code
   *  inspect(
   *    1,
   *    0
   *  );
   * }</pre>
   * <p>
   * This will be called by multiple threads from {@link ResourceInspectorFilter#doFilter(ServletRequest, ServletResponse, FilterChain)}
   * method.
   *
   * @param tenantId    Id of the tenant trying to use resources of the cluster
   * @param startMillis will be used to get buckets to enforce usage checks
   *
   * @return {@link ConstraintCheckResult} indicating whether the tenant should be allowed to use these resource or not
   */
  public ConstraintCheckResult inspect(int tenantId, long startMillis)
      throws Exception
  {
    long inspectStartTime = System.currentTimeMillis();
    try {
      log.debug("Inspecting for tenant [%d]", tenantId);

      final EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps = caps.computeIfAbsent(tenantId, s -> {
        log.makeAlert("Caps for tenant [%d] not found in DB, using default caps", tenantId).emit();
        return defaultCaps;
      });

      final ConstraintCheckResult.Builder checkResultBuilder = new ConstraintCheckResult.Builder();

      for (GranularityType granularity : tenantCaps.keySet()) {
        final DateTime periodStart = granularity.getDefaultGranularity().bucketStart(DateTimes.utc(startMillis));

        final Map<Resource, Long> currentUsage = statsManager.getStatsForTenant(
            tenantId,
            EnumSet.copyOf(tenantCaps.get(granularity).keySet()),
            periodStart.getMillis(),
            granularity
        );
        currentUsage.forEach((resource, usage) -> {
          boolean quotaExceeded = resource.checkQuotaExceeded(
              tenantCaps.get(granularity).get(resource),
              usage,
              0
          );
          checkResultBuilder.addResult(quotaExceeded, tenantId, resource, granularity, periodStart);
        });
      }

      return checkResultBuilder.build();
    }
    finally {
      emitter.emit(ServiceMetricEvent.builder()
                                     .setDimension("tenant_id", Integer.toString(tenantId))
                                     .setDimension("period_start", DateTimes.utc(startMillis).toString())
                                     .build(
                                         "inspect/stats/time",
                                         System.currentTimeMillis() - inspectStartTime
                                     ));
    }
  }

  /**
   * This method is responsible for updating resource usages after query is run. Can be called from multiple threads.
   * <p>
   * For example, if a query consumed 13 units of CPU resource and started at 1563962805124 millis and ended
   * at 1563962805154 millis for tenant with id "1", the method call will be like -
   *
   * <pre> {@code
   *      update(
   *        1,
   *        EnumMap of {Resource.CPU -> 13},
   *        1563962805124,
   *        1563962805154
   *      );
   *     }</pre>
   * <p>
   * This will update the CPU usage value for buckets containing startMillis (1563962805124). Update will be done for
   * buckets whose cap information is present in the resource_caps table for this tenant and if not present then buckets
   * for default tenant will be used.
   * <p>
   * In future, usage information can be spread across buckets falling in range startMillis to endMillis proportionally.
   * <p>
   * Note that in this example CPU units consumed are not equal to (endMillis - startMillis) as these millis represents
   * system time and used for enforcing caps whereas the CPU units should come from some Druid metric representing the
   * actual CPU time consumed.
   *
   * @param tenantId    Id of the tenant
   * @param deltaValues deltaValues for resource usage to be updated
   * @param startMillis start time (on system clock) of the query, used for updating usage values in buckets
   * @param endMillis   end time (on system clock) of the query
   */
  public void update(
      int tenantId,
      EnumMap<Resource, Long> deltaValues,
      long startMillis,
      long endMillis
  )
  {
    log.debug(
        "Updating resource usage [%s] for tenant [%s] for period [%s]-[%s]",
        deltaValues,
        tenantId,
        startMillis,
        endMillis
    );
    // for now updating the resource usage for bucket containing endMillis i.e. when the query finished
    // I think usage information should be spread across buckets falling in range startMillis to endMillis proportionally
    for (GranularityType granularity : caps.get(tenantId).keySet()) {
      final long updateStartTime = System.currentTimeMillis();
      try {
        statsManager.updateStatsForTenant(
            tenantId,
            deltaValues,
            granularity.getDefaultGranularity().bucketStart(DateTimes.utc(endMillis)).getMillis(),
            granularity
        );
      }
      catch (Exception e) {
        log.makeAlert(
            e,
            "Failed to updated resource usage [%s] in bucket [%s] for tenant [%d] for period [%s]-[%s]",
            deltaValues,
            granularity,
            tenantId,
            startMillis,
            endMillis
        ).emit();
        emitter.emit(ServiceMetricEvent.builder()
                                       .setDimension("tenant_id", Integer.toString(tenantId))
                                       .setDimension("period_start", DateTimes.utc(endMillis).toString())
                                       .setDimension("granularity", granularity.name())
                                       .build(
                                           "update/failed/count",
                                           1
                                       ));
      }
      finally {
        emitter.emit(ServiceMetricEvent.builder()
                                       .setDimension("tenant_id", Integer.toString(tenantId))
                                       .setDimension("period_start", DateTimes.utc(endMillis).toString())
                                       .setDimension("granularity", granularity.name())
                                       .build(
                                           "update/stats/time",
                                           System.currentTimeMillis() - updateStartTime
                                       ));

      }
    }
  }

  /**
   * This is generally called when we want to update {@link QuotasInspector#caps} data structure with the latest cap data
   * If this is called multiple times for a tenant then the last call to caps#compute will determine the final cap values
   * for that tenant, although the probability is very rare because as of now this method is called only after cap info
   * is added/updated for a tenant using {@link StatsManager#addOrUpdateCapsForTenant(int, EnumMap, AuditInfo)} endpoint.
   * Using the same executor service used for period sync of caps data so that {@link QuotasInspector#caps} is updated
   * sequentially.
   */
  public void syncCapsForTenant(int tenantId)
  {
    Futures.addCallback(
        scheduledUpdateExecutorService.submit(() -> {
          try {
            final EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps = statsManager.getCapsForTenant(tenantId);
            if (tenantCaps.isEmpty()) {
              throw new ISE("Cannot find caps for tenant [%d]", tenantId);
            }
            caps.put(tenantId, tenantCaps);
          }
          catch (Exception e) {
            // will be handled in onFailure
            Throwables.propagate(e);
          }
          return null;
        }),
        new FutureCallback<Void>()
        {
          @Override
          public void onSuccess(@Nullable Void result)
          {
            log.info("Successfully synced caps for tenant [%d]", tenantId);
          }

          @Override
          public void onFailure(Throwable t)
          {
            log.makeAlert(t, "Unable to sync caps for tenant [%d]", tenantId).emit();
          }
        }
    );
  }

  public ServiceEmitter getEmitter()
  {
    return this.emitter;
  }

  @VisibleForTesting
  public ConcurrentHashMap<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> getCaps()
  {
    return caps;
  }
}
