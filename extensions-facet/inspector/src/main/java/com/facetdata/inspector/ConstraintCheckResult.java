package com.facetdata.inspector;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents result of the check performed against resource quotas for different granularity buckets.
 * The result should be created using the {@link Builder} provided incrementally for each resource and granularity pair,
 * see {@link QuotasInspector#inspect(int, long)} for usage example.
 */
public class ConstraintCheckResult
{
  // Individual constraint results
  private final List<UnitConstraintResult> exhaustedResources;
  private final List<UnitConstraintResult> remainingResources;
  private final boolean quotaExceeded;
  private final String message;

  private ConstraintCheckResult(
      boolean quotaExceeded,
      String message,
      List<UnitConstraintResult> exhaustedResources,
      List<UnitConstraintResult> remainingResources
  )
  {
    this.quotaExceeded = quotaExceeded;
    this.message = message;
    this.exhaustedResources = exhaustedResources;
    this.remainingResources = remainingResources;
  }

  public boolean isQuotaExceeded()
  {
    return quotaExceeded;
  }

  public String getMessage()
  {
    return message;
  }

  public List<UnitConstraintResult> getExhaustedResources()
  {
    return exhaustedResources;
  }

  public List<UnitConstraintResult> getRemainingResources()
  {
    return remainingResources;
  }

  public static class Builder
  {
    final StringBuilder message = new StringBuilder();
    private final List<UnitConstraintResult> exhaustedResources = new ArrayList<>();
    private final List<UnitConstraintResult> remainingResources = new ArrayList<>();

    public Builder addResult(
        boolean quotaExceeded,
        int tenantId,
        Resource resource,
        GranularityType granularityType,
        DateTime periodStart
    )
    {
      List<UnitConstraintResult> listToUpdate;
      if (quotaExceeded) {
        listToUpdate = exhaustedResources;
        addMessage(StringUtils.format("%sLY usage quota exceeded for [%s] resource\n", granularityType, resource));
      } else {
        listToUpdate = remainingResources;
      }
      listToUpdate.add(
          new UnitConstraintResult(
              quotaExceeded,
              tenantId,
              resource,
              granularityType,
              periodStart
          ));
      return this;
    }

    public Builder addMessage(String msg)
    {
      message.append(msg);
      return this;
    }

    public ConstraintCheckResult build()
    {
      /*
       * If there are some exhausted resources or if addResult is not called at all then quotaExceeded is true.
       *
       * Truth Table
       * Exhausted Resource is Empty | Remaining Resource is Empty | Quota Exceeded
       * ----------------------------|-----------------------------|---------------
       *          True               |            True             |      True
       *          False              |            False            |      True
       *          False              |            True             |      True
       *          True               |            False            |      False
       * */
      return new ConstraintCheckResult(
          !exhaustedResources.isEmpty() || remainingResources.isEmpty(),
          message.toString(),
          exhaustedResources,
          remainingResources
      );
    }
  }

  @Override
  public String toString()
  {
    return "ConstraintCheckResult{" +
           "exhaustedResources=" + exhaustedResources +
           ", remainingResources=" + remainingResources +
           ", quotaExceeded=" + quotaExceeded +
           ", message='" + message + '\'' +
           '}';
  }
}
