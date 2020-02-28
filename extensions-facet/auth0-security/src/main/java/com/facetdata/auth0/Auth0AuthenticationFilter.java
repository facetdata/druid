package com.facetdata.auth0;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This filter is used to validate the JWT token recieved from the front end.
 * If validate then extracts the permissions claim and propagates it to the authorizer for authorization checks.
 */
public class Auth0AuthenticationFilter implements Filter
{
  private static final Logger LOG = new Logger(Auth0AuthenticationFilter.class);

  private JWTUtils jwtUtils;
  private String authenticatorName;
  private String authorizerName;

  public Auth0AuthenticationFilter(JWTUtils jwtUtils)
  {
    this.jwtUtils = jwtUtils;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException
  {
    authenticatorName = filterConfig.getInitParameter("authenticatorName");
    authorizerName = filterConfig.getInitParameter("authorizerName");
    Preconditions.checkNotNull(authorizerName, "authorizerName cannot be null");
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException
  {
    final HttpServletRequest req = (HttpServletRequest) request;
    final HttpServletResponse res = (HttpServletResponse) response;

    final String authHeader = req.getHeader("Authorization");
    final String bearerPrefix = "Bearer ";
    if (authHeader == null || !authHeader.startsWith(bearerPrefix)) {
      // invalid OAuth based token
      LOG.debug("No valid bearer token found, moving on to the next authenticator if any");
      chain.doFilter(request, response);
      return;
    }

    final String token = authHeader.substring(bearerPrefix.length());
    try {
      DecodedJWT decodedJWT = jwtUtils.verify(token);
      final AuthenticationResult authenticationResult = authenticate(decodedJWT, authorizerName, authenticatorName);
      req.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
    }
    catch (Exception e) {
      LOG.error(e, "Unable to authenticate using Auth0 authenticator");
      res.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }
    chain.doFilter(request, response);
  }

  @Override
  public void destroy()
  {

  }

  public static AuthenticationResult authenticate(
      DecodedJWT decodedJWT,
      String authorizerName,
      String authenticatorName
  )
  {
    final String user = decodedJWT.getSubject();
    List<String> permissions = decodedJWT.getClaim("permissions").asList(String.class);
    if (permissions == null) {
      LOG.warn("No permissions found in the access token for user [%s]", user);
      permissions = new ArrayList<>();
    }
    final Map<String, Object> context = ImmutableMap.of("permissions", permissions);
    // sets the permission in context, authorizer will extract this and use for authorization checks
    return new AuthenticationResult(
        user,
        authorizerName,
        authenticatorName,
        context
    );
  }
}
