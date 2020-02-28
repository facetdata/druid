package com.facetdata.connector.bigquery;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.firehose.SqlFirehoseFactory;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class BigQueryFirehoseFactory extends SqlFirehoseFactory
    implements FiniteFirehoseFactory<InputRowParser<Map<String, Object>>, String>
{
  private static final Logger log = new Logger(BigQueryFirehoseFactory.class);
  private static final String BQ_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";

  @JsonProperty("timestampColumn")
  private final String timestampColumn;

  @JsonProperty("ingestionDateRange")
  private final IngestionDateRange ingestionDateRange;

  @JsonProperty("splitGranularity")
  private final Granularity splitGranularity;

  private List<InputSplit<String>> splits;

  @JsonCreator
  public BigQueryFirehoseFactory(
      @JsonProperty("sqls") List<String> sqls,
      @JsonProperty("maxCacheCapacityBytes") Long maxCacheCapacityBytes,
      @JsonProperty("maxFetchCapacityBytes") Long maxFetchCapacityBytes,
      @JsonProperty("prefetchTriggerBytes") Long prefetchTriggerBytes,
      @JsonProperty("fetchTimeout") Long fetchTimeout,
      @JsonProperty("foldCase") boolean foldCase,
      @JsonProperty("database") BigQueryDatabaseConnector sqlFirehoseDatabaseConnector,
      @JsonProperty("timestampColumn") String timestampColumn,
      @JsonProperty("ingestionDateRange") IngestionDateRange ingestionDateRange,
      @JsonProperty("splitGranularity") Granularity splitGranularity,
      @JacksonInject @Smile ObjectMapper objectMapper
  )
  {
    super(
        sqls,
        maxCacheCapacityBytes,
        // TODO: prefetch is disabled until a race condition in SqlFetcher is resolved
        0L,
        prefetchTriggerBytes,
        adjustPrefetchTimeout(fetchTimeout, sqlFirehoseDatabaseConnector),
        foldCase,
        sqlFirehoseDatabaseConnector,
        objectMapper
    );

    Preconditions.checkNotNull(timestampColumn, "timestampColumn must not be null");
    Preconditions.checkNotNull(splitGranularity, "splitGranularity must not be null");
    this.timestampColumn = timestampColumn;
    this.ingestionDateRange = ingestionDateRange;
    this.splitGranularity = splitGranularity;
  }

  private static Long adjustPrefetchTimeout(Long fetchTimeout, BigQueryDatabaseConnector sqlFirehoseDatabaseConnector)
  {
    // BigQuery query timeout not specified
    if (sqlFirehoseDatabaseConnector == null ||
        sqlFirehoseDatabaseConnector.getConnectorConfig() == null ||
        sqlFirehoseDatabaseConnector.getConnectorConfig().getQueryTimeout() == null
    ) {
      return fetchTimeout;
    }

    // adjust prefecth timeout so that it is in sync with configured BigQuery query timeout
    Long queryTimeout = TimeUnit.SECONDS.toMillis(sqlFirehoseDatabaseConnector.getConnectorConfig().getQueryTimeout());
    if (fetchTimeout == null || fetchTimeout < queryTimeout) {
      log.info("Adjusting fetchTimeout to [%s] according to BigQuery query timeout", queryTimeout);
      return queryTimeout;
    }
    return fetchTimeout;
  }

  @Override
  public Stream<InputSplit<String>> getSplits()
  {
    initializeSplitsIfNeeded();
    return this.splits.stream();
  }

  @Override
  public int getNumSplits()
  {
    initializeSplitsIfNeeded();
    return this.splits.size();
  }

  @Override
  public FiniteFirehoseFactory<InputRowParser<Map<String, Object>>, String> withSplit(InputSplit<String> split)
  {
    return new BigQueryFirehoseFactory(
        Collections.singletonList(split.get()),
        this.getMaxCacheCapacityBytes(),
        this.getMaxFetchCapacityBytes(),
        this.getPrefetchTriggerBytes(),
        this.getFetchTimeout(),
        this.foldCase,
        (BigQueryDatabaseConnector) this.sqlFirehoseDatabaseConnector,
        this.timestampColumn,
        this.ingestionDateRange,
        this.splitGranularity,
        this.objectMapper
    );
  }

  private void initializeSplitsIfNeeded()
  {
    // make sure prerequisites for generating splits are set and splits are not yet generated
    if (this.splits != null) {
      return;
    }
    // TODO: resolve/query ingestionDateRange from BigQuery in case it's not set
    Preconditions.checkNotNull(this.timestampColumn, "timestampColumn must not be null");
    Preconditions.checkNotNull(this.ingestionDateRange, "ingestionDateRange must not be null");

    // generate splits where each one would only process data for a certain period according to splitGranularity value
    List<InputSplit<String>> splits = new ArrayList<>();
    DateTimeFormatter dateFormatter = DateTimeFormat.forPattern(BQ_DATETIME_FORMAT);
    List<Interval> ingestionIntervals = ingestionDateRange.getIngestionIntervals(splitGranularity);
    for (String sql : this.sqls) {
      for (Interval interval : ingestionIntervals) {
        String filter = " WHERE " + this.timestampColumn + " >= '" + dateFormatter.print(interval.getStart())
                        + "' AND " + this.timestampColumn + " < '" + dateFormatter.print(interval.getEnd()) + "'";
        String splitSql = "SELECT * FROM (" + sql + ") " + filter;
        splits.add(new InputSplit<>(splitSql));
      }
    }

    this.splits = splits;
  }

}
