package com.facetdata.inspector;

import com.facetdata.inspector.config.InspectorConfig;
import com.facetdata.inspector.stats.StatsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.Assert;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class QuotasInspectorTest extends TestCase
{
  StatsManager statsManager;
  InspectorConfig inspectorConfig;

  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    statsManager = EasyMock.createStrictMock(StatsManager.class);
    ServiceEmitter emitter = new ServiceEmitter("test", "test", new NoopEmitter());
    EmittingLogger.registerEmitter(emitter);
    inspectorConfig = new InspectorConfig(
        statsManager,
        emitter,
        null
    );
  }

  @Override
  public void tearDown() throws Exception
  {

  }

  private Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> initializeDefaultCapsAndAddMoreCapsForTenants(
      Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> moreCapsForTenants
  )
      throws Exception
  {
    Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> allCaps = new HashMap<>();
    allCaps.put(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS, new EnumMap<>(GranularityType.class));
    InspectorConfig.DEFAULT_GRANULARITIES.forEach(
        granularityType -> {
          allCaps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS).put(granularityType, new EnumMap<>(Resource.class));
          InspectorConfig.DEFAULT_RESOURCES.forEach(
              resource -> allCaps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS)
                                 .get(granularityType)
                                 .put(
                                     resource,
                                     resource.getDefaultCapPerGranularity(granularityType)
                                 ));
        });
    allCaps.putAll(moreCapsForTenants);
    statsManager.init(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall();
    EasyMock.expect(statsManager.getCapsForAllTenants()).andReturn(allCaps).once();
    return allCaps;
  }

  public void testInspectForNonExistingTenantWithDefaultCaps() throws Exception
  {
    Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> initializedCaps = initializeDefaultCapsAndAddMoreCapsForTenants(
        ImmutableMap.of());
    final int tenantId = 1;

    Capture<Integer> capturedTenantId = Capture.newInstance(CaptureType.ALL);
    Capture<EnumSet<Resource>> capturedResources = Capture.newInstance(CaptureType.ALL);
    Capture<GranularityType> capturedGranularities = Capture.newInstance(CaptureType.ALL);

    EnumMap<Resource, Long> currentStats = new EnumMap<>(Resource.class);
    InspectorConfig.DEFAULT_RESOURCES.forEach(resource -> currentStats.put(resource, 0L));

    EasyMock.expect(statsManager.getStatsForTenant(
        EasyMock.captureInt(capturedTenantId),
        EasyMock.capture(capturedResources),
        EasyMock.anyLong(),
        EasyMock.capture(capturedGranularities)
    ))
            .andReturn(currentStats)
            .times(InspectorConfig.DEFAULT_GRANULARITIES.size());

    EasyMock.replay(statsManager);

    QuotasInspector quotasInspector = new QuotasInspector(inspectorConfig);

    ConstraintCheckResult result = quotasInspector.inspect(
        tenantId,
        System.currentTimeMillis()
    );

    EasyMock.verify(statsManager);
    Assert.assertEquals(capturedTenantId.getValues(), ImmutableList.of(tenantId, tenantId));
    Assert.assertEquals(
        capturedResources.getValues(),
        ImmutableList.of(InspectorConfig.DEFAULT_RESOURCES, InspectorConfig.DEFAULT_RESOURCES)
    );
    Assert.assertEquals(
        capturedGranularities.getValues(),
        ImmutableList.copyOf(InspectorConfig.DEFAULT_GRANULARITIES)
    );
    Assert.assertFalse(result.isQuotaExceeded());
    Assert.assertEquals(2, quotasInspector.getCaps().size());
    Assert.assertEquals(
        initializedCaps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS),
        quotasInspector.getCaps().get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS)
    );
    Assert.assertEquals(
        initializedCaps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS),
        quotasInspector.getCaps().get(tenantId)
    );
  }

  public void testInspectExistingTenantPass() throws Exception
  {
    final int tenantId = 1;
    EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps = new EnumMap<>(GranularityType.class);
    for (Resource resource : Resource.values()) {
      tenantCaps.computeIfAbsent(GranularityType.HOUR, k -> new EnumMap<>(Resource.class))
                .put(resource, 100L);
      tenantCaps.computeIfAbsent(GranularityType.MINUTE, k -> new EnumMap<>(Resource.class))
                .put(resource, 10L);
    }
    Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> initializedCaps = initializeDefaultCapsAndAddMoreCapsForTenants(
        ImmutableMap.of(tenantId, tenantCaps)
    );

    Capture<Integer> capturedTenantId = Capture.newInstance(CaptureType.ALL);
    Capture<EnumSet<Resource>> capturedResources = Capture.newInstance(CaptureType.ALL);
    Capture<GranularityType> capturedGranularities = Capture.newInstance(CaptureType.ALL);

    EnumMap<Resource, Long> currentStats = new EnumMap<>(Resource.class);
    InspectorConfig.DEFAULT_RESOURCES.forEach(resource -> currentStats.put(resource, 1L));

    EasyMock.expect(statsManager.getStatsForTenant(
        EasyMock.captureInt(capturedTenantId),
        EasyMock.capture(capturedResources),
        EasyMock.anyLong(),
        EasyMock.capture(capturedGranularities)
    ))
            .andReturn(currentStats)
            .times(tenantCaps.size());

    EasyMock.replay(statsManager);

    QuotasInspector quotasInspector = new QuotasInspector(inspectorConfig);

    ConstraintCheckResult result = quotasInspector.inspect(
        tenantId,
        System.currentTimeMillis()
    );

    EasyMock.verify(statsManager);
    Assert.assertEquals(ImmutableList.of(tenantId, tenantId), capturedTenantId.getValues());
    Assert.assertEquals(
        ImmutableList.of(InspectorConfig.DEFAULT_RESOURCES, InspectorConfig.DEFAULT_RESOURCES),
        capturedResources.getValues()
    );
    Assert.assertEquals(
        capturedGranularities.getValues(),
        ImmutableList.copyOf(tenantCaps.keySet())
    );
    Assert.assertFalse(result.isQuotaExceeded());

    Assert.assertEquals(2, quotasInspector.getCaps().size());
    Assert.assertEquals(
        initializedCaps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS),
        quotasInspector.getCaps().get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS)
    );
    Assert.assertEquals(
        tenantCaps,
        quotasInspector.getCaps().get(tenantId)
    );
  }

  public void testInspectExistingTenantFail() throws Exception
  {
    final int tenantId = 1;
    EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps = new EnumMap<>(GranularityType.class);
    for (Resource resource : Resource.values()) {
      tenantCaps.computeIfAbsent(GranularityType.HOUR, k -> new EnumMap<>(Resource.class))
                .put(resource, 100L);
      tenantCaps.computeIfAbsent(GranularityType.MINUTE, k -> new EnumMap<>(Resource.class))
                .put(resource, 10L);
    }
    Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> initializedCaps = initializeDefaultCapsAndAddMoreCapsForTenants(
        ImmutableMap.of(tenantId, tenantCaps)
    );

    Capture<Integer> capturedTenantId = Capture.newInstance(CaptureType.ALL);
    Capture<EnumSet<Resource>> capturedResources = Capture.newInstance(CaptureType.ALL);
    Capture<GranularityType> capturedGranularities = Capture.newInstance(CaptureType.ALL);

    EnumMap<Resource, Long> currentStats = new EnumMap<>(Resource.class);
    InspectorConfig.DEFAULT_RESOURCES.forEach(resource -> currentStats.put(resource, Long.MAX_VALUE));

    EasyMock.expect(statsManager.getStatsForTenant(
        EasyMock.captureInt(capturedTenantId),
        EasyMock.capture(capturedResources),
        EasyMock.anyLong(),
        EasyMock.capture(capturedGranularities)
    ))
            .andReturn(currentStats)
            .times(InspectorConfig.DEFAULT_GRANULARITIES.size());

    EasyMock.replay(statsManager);

    QuotasInspector quotasInspector = new QuotasInspector(inspectorConfig);

    ConstraintCheckResult result = quotasInspector.inspect(
        tenantId,
        System.currentTimeMillis()
    );

    EasyMock.verify(statsManager);
    Assert.assertEquals(ImmutableList.of(tenantId, tenantId), capturedTenantId.getValues());
    Assert.assertEquals(
        capturedResources.getValues(),
        ImmutableList.of(tenantCaps.get(GranularityType.MINUTE).keySet(), tenantCaps.get(GranularityType.HOUR).keySet())
    );
    Assert.assertEquals(
        capturedGranularities.getValues(),
        ImmutableList.copyOf(tenantCaps.keySet())
    );
    Assert.assertTrue(result.isQuotaExceeded());

    Assert.assertEquals(2, quotasInspector.getCaps().size());
    Assert.assertEquals(
        initializedCaps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS),
        quotasInspector.getCaps().get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS)
    );
    Assert.assertEquals(
        tenantCaps,
        quotasInspector.getCaps().get(tenantId)
    );
  }

  public void testUpdate() throws Exception
  {
    int tenantId = 1;
    EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps = new EnumMap<>(GranularityType.class);
    tenantCaps.compute(GranularityType.HOUR, (k, v) -> new EnumMap<>(Resource.class))
              .compute(Resource.CPU, (k, v) -> 100L);
    tenantCaps.compute(GranularityType.MINUTE, (k, v) -> new EnumMap<>(Resource.class))
              .compute(Resource.CPU, (k, v) -> 10L);
    Map<Integer, EnumMap<GranularityType, EnumMap<Resource, Long>>> initializedCaps = initializeDefaultCapsAndAddMoreCapsForTenants(
        ImmutableMap.of(tenantId, tenantCaps)
    );

    Capture<Integer> capturedTenantId = Capture.newInstance(CaptureType.ALL);
    Capture<EnumMap<Resource, Long>> capturedResources = Capture.newInstance(CaptureType.ALL);
    Capture<GranularityType> capturedGranularities = Capture.newInstance(CaptureType.ALL);

    statsManager.updateStatsForTenant(
        EasyMock.captureInt(capturedTenantId),
        EasyMock.capture(capturedResources),
        EasyMock.anyLong(),
        EasyMock.capture(capturedGranularities)
    );

    EasyMock.expectLastCall().times(tenantCaps.size());

    EasyMock.replay(statsManager);

    QuotasInspector quotasInspector = new QuotasInspector(inspectorConfig);
    EnumMap<Resource, Long> usage = new EnumMap<>(Resource.class);
    usage.put(Resource.CPU, 1L);
    quotasInspector.update(
        tenantId,
        usage,
        System.currentTimeMillis(),
        System.currentTimeMillis() + 100
    );

    EasyMock.verify(statsManager);
    Assert.assertEquals(ImmutableList.of(tenantId, tenantId), capturedTenantId.getValues());
    Assert.assertEquals(
        capturedResources.getValues(),
        ImmutableList.of(usage, usage)
    );
    Assert.assertEquals(
        capturedGranularities.getValues(),
        ImmutableList.copyOf(tenantCaps.keySet())
    );

    Assert.assertEquals(2, quotasInspector.getCaps().size());
    Assert.assertEquals(
        initializedCaps.get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS),
        quotasInspector.getCaps().get(StatsManager.TENANT_ID_FOR_DEFAULT_CAPS)
    );
    Assert.assertEquals(
        tenantCaps,
        quotasInspector.getCaps().get(tenantId)
    );
  }
}
