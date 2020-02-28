package com.facetdata.connector.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

public class IngestionDateRange
{

  @JsonProperty("dateFrom")
  private DateTime dateFrom;

  @JsonProperty("dateTo")
  private DateTime dateTo;

  @JsonCreator
  public IngestionDateRange(
      @JsonProperty("dateFrom") DateTime dateFrom,
      @JsonProperty("dateTo") DateTime dateTo
  )
  {
    Preconditions.checkNotNull(dateFrom, "dateFrom must not be null");
    Preconditions.checkNotNull(dateTo, "dateTo must not be null");
    if (dateFrom.isAfter(dateTo)) {
      throw new IllegalArgumentException("Error instantiating IngestionDateRange: dateFrom is after dateTo");
    }
    this.dateFrom = dateFrom;
    this.dateTo = dateTo;
  }

  public List<Interval> getIngestionIntervals(Granularity granularity)
  {
    List<Interval> ingestionIntervals = new ArrayList<>();
    if (dateFrom == null || dateTo == null) {
      return ingestionIntervals;
    }

    for (Interval interval : granularity.getIterable(new Interval(dateFrom, dateTo))) {
      if (interval.getStart().isBefore(dateFrom)) {
        interval = interval.withStart(dateFrom);
      }
      if (interval.getEnd().isAfter(dateTo)) {
        interval = interval.withEnd(dateTo);
      }
      ingestionIntervals.add(interval);
    }

    return ingestionIntervals;
  }

  public DateTime getDateFrom()
  {
    return dateFrom;
  }

  public DateTime getDateTo()
  {
    return dateTo;
  }
}
