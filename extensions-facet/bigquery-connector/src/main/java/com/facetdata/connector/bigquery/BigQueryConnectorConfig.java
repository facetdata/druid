package com.facetdata.connector.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class BigQueryConnectorConfig
{

  @JsonProperty
  private GoogleServiceAccount serviceAccount;

  /**
   * The length of time, in seconds, that the driver waits for a query to retrieve the results of an executed job.
   * See <a href="https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/bq/options/timeout.htm">
   * Simba JDBC Driver Timeout
   * </a>
   */
  @JsonProperty
  private Long queryTimeout;

  @JsonCreator
  public BigQueryConnectorConfig(
      @JsonProperty("serviceAccount") GoogleServiceAccount serviceAccount,
      @JsonProperty("queryTimeout") Long queryTimeout
  )
  {
    Preconditions.checkNotNull(serviceAccount, "serviceAccount must not be null");
    Preconditions.checkNotNull(queryTimeout, "queryTimeout must not be null");
    this.serviceAccount = serviceAccount;
    this.queryTimeout = queryTimeout;
  }

  public GoogleServiceAccount getServiceAccount()
  {
    return serviceAccount;
  }

  public Long getQueryTimeout()
  {
    return queryTimeout;
  }
}
