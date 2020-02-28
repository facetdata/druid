package com.facetdata.connector.bigquery;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.skife.jdbi.v2.DBI;

import java.io.File;
import java.io.IOException;


@JsonTypeName("bigquery")
public class BigQueryDatabaseConnector extends SQLFirehoseDatabaseConnector
{
  private static final String BIGQUERY_JDBC_DRIVER_CLASS_NAME = "com.simba.googlebigquery.jdbc42.Driver";
  private static final String API_URL = "https://www.googleapis.com/bigquery/v2:443";

  private final String jdbcUrl;
  private final BigQueryConnectorConfig connectorConfig;
  private final DBI dbi;

  public BigQueryDatabaseConnector(
      @JsonProperty("connectorConfig") BigQueryConnectorConfig connectorConfig
  ) throws IOException
  {
    this.connectorConfig = connectorConfig;
    this.jdbcUrl = this.constructUrl(connectorConfig);
    final BasicDataSource datasource = getDatasource();
    this.dbi = new DBI(datasource);
  }

  @JsonProperty
  public BigQueryConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  private String constructUrl(
      BigQueryConnectorConfig connectorConfig
  ) throws IOException
  {
    GoogleServiceAccount serviceAccount = connectorConfig.getServiceAccount();
    File serviceAccountFile = createTempServiceAccountFile(serviceAccount);

    return StringUtils.format(
        "jdbc:bigquery://%s;" +
        "ProjectId=%s;OAuthType=0;" +
        "OAuthServiceAcctEmail=%s;" +
        "OAuthPvtKeyPath=%s;" +
        "Timeout=%s;",
        API_URL,
        serviceAccount.getProjectId(),
        serviceAccount.getClientEmail(),
        serviceAccountFile.getAbsolutePath(),
        connectorConfig.getQueryTimeout()
    );
  }

  private BasicDataSource getDatasource()
  {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(this.jdbcUrl);
    dataSource.setTestOnBorrow(true);
    dataSource.setValidationQuery(getValidationQuery());
    dataSource.setDriverClassLoader(getClass().getClassLoader());
    dataSource.setDriverClassName(BIGQUERY_JDBC_DRIVER_CLASS_NAME);
    return dataSource;
  }

  /**
   * Serializes {@link GoogleServiceAccount} to a temporary JSON file which BigQuery JDBC driver uses for authentication
   *
   * @param serviceAccount {@link GoogleServiceAccount} to be serialized
   * @return {@link File} containing service account JSON data
   * @throws IOException if temporary file cannot be created
   */
  private File createTempServiceAccountFile(GoogleServiceAccount serviceAccount) throws IOException
  {
    try {
      File serviceAccountFile = File.createTempFile("bigquery", ".json");
      ObjectWriter writer = new ObjectMapper().writer();
      writer.writeValue(new File(serviceAccountFile.getAbsolutePath()), serviceAccount);
      serviceAccountFile.deleteOnExit();
      return serviceAccountFile;
    }
    catch (IOException e) {
      throw new IOException("Failed to create BigQuery service account file", e);
    }
  }
}
