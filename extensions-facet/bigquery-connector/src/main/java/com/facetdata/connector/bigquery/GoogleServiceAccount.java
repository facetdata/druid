package com.facetdata.connector.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GoogleServiceAccount
{
  @JsonProperty("project_id")
  private String projectId;

  @JsonProperty("token_uri")
  private String tokenUri;

  @JsonProperty("private_key")
  private String privateKey;

  @JsonProperty("auth_provider_x509_cert_url")
  private String authProviderX509CertUrl;

  @JsonProperty("client_x509_cert_url")
  private String clientX509CertUrl;

  @JsonProperty("auth_uri")
  private String authUri;

  @JsonProperty("type")
  private String type;

  @JsonProperty("private_key_id")
  private String privateKeyId;

  @JsonProperty("client_id")
  private String clientId;

  @JsonProperty("client_email")
  private String clientEmail;

  @JsonCreator
  public GoogleServiceAccount(
      @JsonProperty("project_id") String projectId,
      @JsonProperty("token_uri") String tokenUri,
      @JsonProperty("private_key") String privateKey,
      @JsonProperty("auth_provider_x509_cert_url") String authProviderX509CertUrl,
      @JsonProperty("client_x509_cert_url") String clientX509CertUrl,
      @JsonProperty("auth_uri") String authUri,
      @JsonProperty("type") String type,
      @JsonProperty("private_key_id") String privateKeyId,
      @JsonProperty("client_id") String clientId,
      @JsonProperty("client_email") String clientEmail
  )
  {
    this.projectId = projectId;
    this.tokenUri = tokenUri;
    this.privateKey = privateKey;
    this.authProviderX509CertUrl = authProviderX509CertUrl;
    this.clientX509CertUrl = clientX509CertUrl;
    this.authUri = authUri;
    this.type = type;
    this.privateKeyId = privateKeyId;
    this.clientId = clientId;
    this.clientEmail = clientEmail;
  }

  public String getProjectId()
  {
    return projectId;
  }

  public String getTokenUri()
  {
    return tokenUri;
  }

  public String getPrivateKey()
  {
    return privateKey;
  }

  public String getAuthProviderX509CertUrl()
  {
    return authProviderX509CertUrl;
  }

  public String getClientX509CertUrl()
  {
    return clientX509CertUrl;
  }

  public String getAuthUri()
  {
    return authUri;
  }

  public String getType()
  {
    return type;
  }

  public String getPrivateKeyId()
  {
    return privateKeyId;
  }

  public String getClientId()
  {
    return clientId;
  }

  public String getClientEmail()
  {
    return clientEmail;
  }
}
