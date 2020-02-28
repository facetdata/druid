package com.facetdata.auth0;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("auth0")
public class Auth0Authenticator implements Authenticator
{
  private static final Logger LOG = new Logger(Auth0Authenticator.class);

  private final String name;
  private final String authorizerName;
  private final String clientId;
  private final PasswordProvider clientSecret;
  private final JWTUtils jwtUtils;

  @JsonCreator
  public Auth0Authenticator(
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("issuer") String issuer,
      @JsonProperty("audience") String audience,
      @JsonProperty("clientId") String clientId,
      @JsonProperty("clientSecret") PasswordProvider clientSecret
  )
  {
    this.name = name;
    this.authorizerName = authorizerName;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.jwtUtils = new JWTUtils(issuer, audience);
  }

  @Override
  public Filter getFilter()
  {
    return new Auth0AuthenticationFilter(jwtUtils);
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return ImmutableMap.of("authenticatorName", name, "authorizerName", authorizerName);
  }

  @Override
  public String getPath()
  {
    return null;
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  @Nullable
  @Override
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Nullable
  @Override
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    if (clientId == null || clientSecret == null) {
      LOG.error("clientId and clientSecret needs to be set for username/pwd based Jdbc authentication");
      return null;
    }

    final String user = (String) context.get("user");
    final String pwd = (String) context.get("password");

    if (user == null || pwd == null) {
      LOG.error("No username/pwd found to perform Jdbc authentication");
      return null;
    }

    try {
      final String token = jwtUtils.getAccessToken(user, pwd, clientId, clientSecret.getPassword());
      Preconditions.checkNotNull(token);
      final DecodedJWT decodedJWT = jwtUtils.verify(token);
      return Auth0AuthenticationFilter.authenticate(decodedJWT, authorizerName, name);
    }
    catch (Exception e) {
      LOG.error(e, "Unable to authenticate JDBC context using Auth0 authenticator");
    }
    return null;
  }
}
