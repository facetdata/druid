package com.facetdata.inspector;

import junit.framework.TestCase;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.junit.Assert;


public class ConstraintCheckResultTest extends TestCase
{
  public void testBuilderWithNoResources()
  {
    ConstraintCheckResult.Builder builder = new ConstraintCheckResult.Builder();
    ConstraintCheckResult result = builder.build();
    Assert.assertTrue(result.isQuotaExceeded());
  }

  public void testBuilderWithExhaustedResources()
  {
    ConstraintCheckResult.Builder builder = new ConstraintCheckResult.Builder();
    builder.addResult(true, 1, Resource.CPU, GranularityType.MINUTE, DateTimes.nowUtc());
    ConstraintCheckResult result = builder.build();
    Assert.assertTrue(result.isQuotaExceeded());
  }

  public void testBuilderWithRemainingResources()
  {
    ConstraintCheckResult.Builder builder = new ConstraintCheckResult.Builder();
    builder.addResult(false, 1, Resource.CPU, GranularityType.MINUTE, DateTimes.nowUtc());
    ConstraintCheckResult result = builder.build();
    Assert.assertFalse(result.isQuotaExceeded());
  }


  public void testBuilderWithExhaustedAndRemainingResources()
  {
    ConstraintCheckResult.Builder builder = new ConstraintCheckResult.Builder();
    builder.addResult(false, 1, Resource.CPU, GranularityType.MINUTE, DateTimes.nowUtc());
    builder.addResult(true, 2, Resource.CPU, GranularityType.MINUTE, DateTimes.nowUtc());
    ConstraintCheckResult result = builder.build();
    Assert.assertTrue(result.isQuotaExceeded());
  }
}
