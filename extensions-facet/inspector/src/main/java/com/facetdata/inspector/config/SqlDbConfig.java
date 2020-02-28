package com.facetdata.inspector.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;

public class SqlDbConfig
{
  private static final int DEFAULT_RETRIES = 3;
  private static final int DEFAULT_CONNECTION_TIMEOUT = 250;
  private static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  @JsonProperty
  private final String connectionURL;
  @JsonProperty
  private final String username;
  @JsonProperty("password")
  private final PasswordProvider passwordProvider;
  @JsonProperty
  private final String driverClassName;
  @JsonProperty
  private final Integer connectionTimeout;
  @JsonProperty
  private final Integer idleTimeout;
  @JsonProperty
  private final Integer maxLifetime;
  @JsonProperty
  private final Integer minimumIdle;
  @JsonProperty
  private final Integer maximumPoolSize;
  @JsonProperty
  private final Integer transactionRetries;

  @JsonCreator
  public SqlDbConfig(
      @JsonProperty("connectionURL") String connectionURL,
      @JsonProperty("username") String username,
      @JsonProperty("password") PasswordProvider passwordProvider,
      @JsonProperty("driverClassName") String driverClassName,
      @JsonProperty("connectionTimeout") Integer connectionTimeout,
      @JsonProperty("idleTimeout") Integer idleTimeout,
      @JsonProperty("maxLifetime") Integer maxLifetime,
      @JsonProperty("minimumIdle") Integer minimumIdle,
      @JsonProperty("maximumPoolSize") Integer maximumPoolSize,
      @JsonProperty("transactionRetries") Integer transactionRetries
  )
  {
    this.connectionURL = connectionURL;
    this.username = username;
    this.passwordProvider = passwordProvider;
    this.driverClassName = driverClassName != null ? driverClassName : MYSQL_DRIVER_CLASS_NAME;
    this.connectionTimeout = connectionTimeout != null ? connectionTimeout : DEFAULT_CONNECTION_TIMEOUT;
    this.idleTimeout = idleTimeout;
    this.maxLifetime = maxLifetime;
    this.minimumIdle = minimumIdle;
    this.maximumPoolSize = maximumPoolSize;
    this.transactionRetries = transactionRetries != null ? transactionRetries : DEFAULT_RETRIES;
  }

  public String getConnectionURL()
  {
    return connectionURL;
  }

  public String getUsername()
  {
    return username;
  }

  public PasswordProvider getPasswordProvider()
  {
    return passwordProvider;
  }

  public String getDriverClassName()
  {
    return driverClassName;
  }

  public Integer getConnectionTimeout()
  {
    return connectionTimeout;
  }

  public Integer getIdleTimeout()
  {
    return idleTimeout;
  }

  public Integer getMaxLifetime()
  {
    return maxLifetime;
  }

  public Integer getMinimumIdle()
  {
    return minimumIdle;
  }

  public Integer getMaximumPoolSize()
  {
    return maximumPoolSize;
  }

  public Integer getTransactionRetries()
  {
    return transactionRetries;
  }

  @Override
  public String toString()
  {
    return "SqlDbConfig{" +
           "connectionURL='" + connectionURL + '\'' +
           ", username='" + username + '\'' +
           ", passwordProvider=" + passwordProvider +
           ", driverClassName=" + driverClassName +
           ", connectionTimeout=" + connectionTimeout +
           ", idleTimeout=" + idleTimeout +
           ", maxLifetime=" + maxLifetime +
           ", minimumIdle=" + minimumIdle +
           ", maximumPoolSize=" + maximumPoolSize +
           ", transactionRetries=" + transactionRetries +
           '}';
  }
}
