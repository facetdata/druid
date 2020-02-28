package com.facetdata.auth0;

import com.auth0.jwk.GuavaCachedJwkProvider;
import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Map;

public class Auth0AuthenticationFilterTest
{
  // Mocks
  private JwkProvider mockUrlJwkProvider;
  private HttpServletRequest mockRequest;
  private HttpServletResponse mockResponse;
  private FilterChain mockFilterChain;
  private Jwk mockJwk;

  private Auth0AuthenticationFilter authenticationFilter;
  private JwkProvider guavaCachedJwkProvider;

  private final String keyId = "k1";
  private final String issuer = "testIssuer";
  private final String audience = "testAudience";
  private final String[] permissions = new String[]{"read:datasource:*"};
  private final String subject = "testSubject";
  private final String authenticatorName = "auth0";
  private final String authorizerName = "auth0";


  @Before
  public void setUp() throws Exception
  {
    mockRequest = EasyMock.createStrictMock(HttpServletRequest.class);
    mockResponse = EasyMock.createStrictMock(HttpServletResponse.class);
    mockFilterChain = EasyMock.createStrictMock(FilterChain.class);
    mockJwk = EasyMock.createStrictMock(Jwk.class);
    mockUrlJwkProvider = EasyMock.createStrictMock(UrlJwkProvider.class);
    guavaCachedJwkProvider = new GuavaCachedJwkProvider(mockUrlJwkProvider);

    final JWTUtils jwtUtils = new JWTUtils(issuer, audience);
    Field jwkProviderField = jwtUtils.getClass().getDeclaredField("cachedJwkProvider");
    jwkProviderField.setAccessible(true);
    // replace actual cachedJwkProvider with one created locally which wraps mockUrlJwkProvider
    jwkProviderField.set(jwtUtils, guavaCachedJwkProvider);

    authenticationFilter = new Auth0AuthenticationFilter(jwtUtils);
    final Map<String, String> map = ImmutableMap.of(
        "authenticatorName",
        authenticatorName,
        "authorizerName",
        authorizerName
    );
    final FilterConfig filterConfig = new FilterConfig()
    {
      @Override
      public String getFilterName()
      {
        return null;
      }

      @Override
      public ServletContext getServletContext()
      {
        return null;
      }

      @Override
      public String getInitParameter(String name)
      {
        return map.get(name);
      }

      @Override
      public Enumeration<String> getInitParameterNames()
      {
        return null;
      }
    };
    authenticationFilter.init(filterConfig);
  }

  @Test
  public void testDoFilterNoAuthHeader() throws ServletException, IOException
  {
    EasyMock.expect(mockRequest.getHeader("Authorization")).andReturn(null).once();
    mockFilterChain.doFilter(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
    authenticationFilter.doFilter(mockRequest, mockResponse, mockFilterChain);
    EasyMock.verify(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
  }

  @Test
  public void testDoFilterInvalidAuthHeader() throws ServletException, IOException
  {
    EasyMock.expect(mockRequest.getHeader("Authorization")).andReturn("nothing").once();
    mockFilterChain.doFilter(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
    authenticationFilter.doFilter(mockRequest, mockResponse, mockFilterChain);
    EasyMock.verify(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
  }

  @Test
  public void testDoFilterPass() throws NoSuchAlgorithmException, ServletException, IOException, JwkException
  {
    KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();
    String token = JWT.create()
                      .withSubject(subject)
                      .withKeyId(keyId)
                      .withAudience(audience)
                      .withIssuer(issuer)
                      .withArrayClaim("permissions", permissions)
                      .sign(Algorithm.RSA256(null, (RSAPrivateKey) privateKey));

    EasyMock.expect(mockRequest.getHeader("Authorization")).andReturn("Bearer " + token).once();
    EasyMock.expect(mockUrlJwkProvider.get(EasyMock.anyString())).andReturn(mockJwk).once();
    EasyMock.expect(mockJwk.getPublicKey()).andReturn(publicKey).once();

    Capture<String> capturedAttribute = Capture.newInstance();
    Capture<AuthenticationResult> capturedResult = Capture.newInstance();
    mockRequest.setAttribute(EasyMock.capture(capturedAttribute), EasyMock.capture(capturedResult));
    EasyMock.expectLastCall().once();

    mockFilterChain.doFilter(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
    authenticationFilter.doFilter(mockRequest, mockResponse, mockFilterChain);
    EasyMock.verify(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);

    Assert.assertEquals(AuthConfig.DRUID_AUTHENTICATION_RESULT, capturedAttribute.getValue());

    final Map<String, Object> context = ImmutableMap.of("permissions", Lists.newArrayList(permissions));
    final AuthenticationResult expectedResult = new AuthenticationResult(
        subject,
        authorizerName,
        authenticatorName,
        context
    );

    Assert.assertEquals(expectedResult, capturedResult.getValue());
  }

  @Test
  public void testDoFilterFailInvalidIssuer() throws NoSuchAlgorithmException, ServletException, IOException, JwkException
  {
    KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();
    String token = JWT.create()
                      .withSubject(subject)
                      .withKeyId(keyId)
                      .withAudience(audience)
                      .withIssuer("random")
                      .withArrayClaim("permissions", permissions)
                      .sign(Algorithm.RSA256(null, (RSAPrivateKey) privateKey));

    EasyMock.expect(mockRequest.getHeader("Authorization")).andReturn("Bearer " + token).once();
    EasyMock.expect(mockUrlJwkProvider.get(EasyMock.anyString())).andReturn(mockJwk).once();
    EasyMock.expect(mockJwk.getPublicKey()).andReturn(publicKey).once();
    Capture<Integer> capturedResponseCode = Capture.newInstance();
    mockResponse.sendError(EasyMock.captureInt(capturedResponseCode));
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
    authenticationFilter.doFilter(mockRequest, mockResponse, mockFilterChain);
    EasyMock.verify(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);

    Assert.assertEquals((Integer) HttpServletResponse.SC_UNAUTHORIZED, capturedResponseCode.getValue());
  }

  @Test
  public void testDoFilterFailInvalidAudience() throws NoSuchAlgorithmException, ServletException, IOException, JwkException
  {
    KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();
    String token = JWT.create()
                      .withSubject(subject)
                      .withKeyId(keyId)
                      .withAudience("random")
                      .withIssuer(issuer)
                      .withArrayClaim("permissions", permissions)
                      .sign(Algorithm.RSA256(null, (RSAPrivateKey) privateKey));

    EasyMock.expect(mockRequest.getHeader("Authorization")).andReturn("Bearer " + token).once();
    EasyMock.expect(mockUrlJwkProvider.get(EasyMock.anyString())).andReturn(mockJwk).once();
    EasyMock.expect(mockJwk.getPublicKey()).andReturn(publicKey).once();
    Capture<Integer> capturedResponseCode = Capture.newInstance();
    mockResponse.sendError(EasyMock.captureInt(capturedResponseCode));
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
    authenticationFilter.doFilter(mockRequest, mockResponse, mockFilterChain);
    EasyMock.verify(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);

    Assert.assertEquals((Integer) HttpServletResponse.SC_UNAUTHORIZED, capturedResponseCode.getValue());
  }

  @Test
  public void testDoFilterPassNoPermissions() throws NoSuchAlgorithmException, ServletException, IOException, JwkException
  {
    KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();
    String token = JWT.create()
                      .withSubject(subject)
                      .withKeyId(keyId)
                      .withAudience(audience)
                      .withIssuer(issuer)
                      .sign(Algorithm.RSA256(null, (RSAPrivateKey) privateKey));

    EasyMock.expect(mockRequest.getHeader("Authorization")).andReturn("Bearer " + token).once();
    EasyMock.expect(mockUrlJwkProvider.get(EasyMock.anyString())).andReturn(mockJwk).once();
    EasyMock.expect(mockJwk.getPublicKey()).andReturn(publicKey).once();

    Capture<String> capturedAttribute = Capture.newInstance();
    Capture<AuthenticationResult> capturedResult = Capture.newInstance();
    mockRequest.setAttribute(EasyMock.capture(capturedAttribute), EasyMock.capture(capturedResult));
    EasyMock.expectLastCall().once();

    mockFilterChain.doFilter(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);
    authenticationFilter.doFilter(mockRequest, mockResponse, mockFilterChain);
    EasyMock.verify(mockRequest, mockResponse, mockUrlJwkProvider, mockJwk, mockFilterChain);

    Assert.assertEquals(AuthConfig.DRUID_AUTHENTICATION_RESULT, capturedAttribute.getValue());

    final Map<String, Object> context = ImmutableMap.of("permissions", new ArrayList<String>());
    final AuthenticationResult expectedResult = new AuthenticationResult(
        subject,
        authorizerName,
        authenticatorName,
        context
    );

    Assert.assertEquals(expectedResult, capturedResult.getValue());
  }
}
