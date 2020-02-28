package com.facetdata.inspector;

import org.apache.druid.java.util.common.granularity.GranularityType;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class UnitConstraintResult
{
  private final boolean quotaNotExceeded;
  private final int tenantId;
  private final Resource resource;
  private final GranularityType granularityType;
  private final DateTime periodStart;

  public UnitConstraintResult(
      boolean quotaNotExceeded,
      int tenantId,
      Resource resource,
      GranularityType granularityType,
      DateTime periodStart
  )
  {
    this.quotaNotExceeded = quotaNotExceeded;
    this.tenantId = tenantId;
    this.resource = resource;
    this.granularityType = granularityType;
    this.periodStart = periodStart;
  }

  public boolean isQuotaNotExceeded()
  {
    return quotaNotExceeded;
  }

  public int getTenantId()
  {
    return tenantId;
  }

  public Resource getResource()
  {
    return resource;
  }

  public GranularityType getGranularityType()
  {
    return granularityType;
  }

  public DateTime getPeriodStart()
  {
    return periodStart;
  }

  public Map<String, String> getDimensionMap()
  {
    Map<String, String> dimensionMap = new HashMap<>();
    dimensionMap.put("tenant_id", Integer.toString(getTenantId()));
    dimensionMap.put("resource", getResource().name());
    dimensionMap.put("granularity", getGranularityType().name());
    dimensionMap.put("period_start", getPeriodStart().toString());
    return dimensionMap;
  }
}
