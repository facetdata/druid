package com.facetdata.inspector;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.GranularityType;

/**
 * Enum for representing cluster resources to be monitored
 */
public enum Resource
{
  CPU {
    /** Pass zero as estimate if not known. */
    @Override
    public boolean checkQuotaExceeded(
        long cap,
        long currentUsage,
        long estimate
    )
    {
      return cap - currentUsage - estimate < 0;
    }

    /** The unit of values returned from this and {@link #getDefaultCapPerGranularity(GranularityType)} method
     * is CPU-second. */
    @Override
    public Number getDefaultCapPerSecond()
    {
      return 0.2;
    }

    @Override
    public long getDefaultCapPerGranularity(GranularityType granularityType)
    {
      if (granularityType == GranularityType.SECOND) {
        // cannot return 0.2 so returning the lowest possbile integral value higher that 0.2
        return 1;
      } else {
        return (long) ((double) getDefaultCapPerSecond() * granularityType.getDefaultGranularity()
                                                                          .bucket(DateTimes.nowUtc())
                                                                          .toDuration()
                                                                          .getStandardSeconds());
      }
    }
  },
  QUERY_COUNT {
    @Override
    public boolean checkQuotaExceeded(long cap, long currentUsage, long estimate)
    {
      return cap - currentUsage - estimate < 0;
    }

    /** This method and {@link #getDefaultCapPerGranularity(GranularityType)} return values which are numbers of queries. */
    @Override
    public Number getDefaultCapPerSecond()
    {
      return 100L; // arbitrary chosen value as have no data for basis
    }

    @Override
    public long getDefaultCapPerGranularity(GranularityType granularityType)
    {
      return (long) getDefaultCapPerSecond() * granularityType.getDefaultGranularity()
                                                              .bucket(DateTimes.nowUtc())
                                                              .toDuration()
                                                              .getStandardSeconds();
    }
  };

  /**
   * Key in the usage table, for example it will be the column name if usage info is being stored
   * in a database table
   */
  public String getUsageKey()
  {
    return StringUtils.format("%s_usage", StringUtils.toLowerCase(this.name()));
  }

  /**
   * Key in the cap table, for example it will be the column name if caps info is being stored
   * in a database table
   */
  public String getCapKey()
  {
    return StringUtils.format("%s_cap", StringUtils.toLowerCase(this.name()));
  }

  /**
   * Used for calculating default caps for different granularities while inserting caps for default tenant
   * upon initialization. Its a Number because some resources might need to define per second caps in floating point.
   */
  public abstract Number getDefaultCapPerSecond();

  /**
   * Return the caps per granularity, the class itself is reponsible to do that so it can appropriately cast the
   * number returned from {@link Resource#getDefaultCapPerSecond()} and calculate per granularity cap.
   */
  public abstract long getDefaultCapPerGranularity(GranularityType granularityType);

  public abstract boolean checkQuotaExceeded(
      long cap,
      long currentUsage,
      long estimate
  );
}
