package com.facetdata.inspector.config;

import com.facetdata.inspector.Resource;
import com.facetdata.inspector.stats.StatsManager;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.EnumSet;

public class InspectorConfig
{
  public static final EnumSet<GranularityType> DEFAULT_GRANULARITIES = EnumSet.of(
      GranularityType.MINUTE,
      GranularityType.HOUR
  );
  public static final EnumSet<Resource> DEFAULT_RESOURCES = EnumSet.allOf(Resource.class);
  private static final int DEFAULT_SYNC_PERIOD_SECONDS = 60;
  private final StatsManager statsManager;
  private final ServiceEmitter emitter;
  @JsonProperty
  private final Integer capsSyncPeriodSeconds;

  @JsonCreator
  public InspectorConfig(
      @JacksonInject StatsManager statsManager,
      @JacksonInject ServiceEmitter emitter,
      @JsonProperty("capsSyncPeriodSeconds") Integer capsSyncPeriodSeconds
  )
  {
    this.statsManager = statsManager;
    this.emitter = emitter;
    this.capsSyncPeriodSeconds = capsSyncPeriodSeconds != null ? capsSyncPeriodSeconds : DEFAULT_SYNC_PERIOD_SECONDS;
  }

  public StatsManager getStatsManager()
  {
    return statsManager;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public Integer getCapsSyncPeriodSeconds()
  {
    return capsSyncPeriodSeconds;
  }

  @Override
  public String toString()
  {
    return "InspectorConfig{" +
           "statsManager=" + statsManager +
           ", emitter=" + emitter +
           ", capsSyncPeriodSeconds=" + capsSyncPeriodSeconds +
           '}';
  }
}
