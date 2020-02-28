package com.facetdata.auth0;

import com.auth0.jwk.JwkException;
import com.auth0.jwt.impl.NullClaim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.server.security.AuthenticationResult;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;

public class Auth0AuthenticatorTest
{
  private Auth0Authenticator auth0Authenticator;
  private final String name = "authn";
  private final String authorizerName = "authz";
  private final String issuer = "iss";
  private final String audience = "aud";
  private final String clientId = "clientId";
  private final String clientSecret = "clientPwd";

  private final JWTUtils mockJwtUtils = EasyMock.createStrictMock(JWTUtils.class);
  private final DecodedJWT mockDecodedJWT = EasyMock.createStrictMock(DecodedJWT.class);

  @Before
  public void setUp() throws Exception
  {
    auth0Authenticator = new Auth0Authenticator(
        name,
        authorizerName,
        issuer,
        audience,
        clientId,
        new DefaultPasswordProvider(clientSecret)
    );
    Field jwtUtilsField = auth0Authenticator.getClass().getDeclaredField("jwtUtils");
    jwtUtilsField.setAccessible(true);
    jwtUtilsField.set(auth0Authenticator, mockJwtUtils);
  }

  @Test
  public void authenticateJDBCContext() throws JwkException
  {
    final String user = "testUser";
    final String pwd = "pwd";
    final String token = "token";

    Capture<String> capturedUser = EasyMock.newCapture();
    Capture<String> capturedPassword = EasyMock.newCapture();
    Capture<String> capturedClientId = EasyMock.newCapture();
    Capture<String> capturedClientSecret = EasyMock.newCapture();
    Capture<String> capturedToken = EasyMock.newCapture();

    EasyMock.expect(
        mockJwtUtils.getAccessToken(
            EasyMock.capture(capturedUser),
            EasyMock.capture(capturedPassword),
            EasyMock.capture(capturedClientId),
            EasyMock.capture(capturedClientSecret)
        )).andReturn(token).once();

    EasyMock.expect(mockJwtUtils.verify(EasyMock.capture(capturedToken))).andReturn(mockDecodedJWT).once();

    EasyMock.expect(mockDecodedJWT.getSubject()).andReturn(user).once();
    EasyMock.expect(mockDecodedJWT.getClaim(EasyMock.anyString())).andReturn(new NullClaim()).once();

    EasyMock.replay(mockJwtUtils, mockDecodedJWT);

    AuthenticationResult authenticationResult = auth0Authenticator.authenticateJDBCContext(ImmutableMap.of(
        "user",
        user,
        "password",
        pwd
    ));
    Assert.assertEquals(authorizerName, authenticationResult.getAuthorizerName());
    Assert.assertEquals(name, authenticationResult.getAuthenticatedBy());
    Assert.assertEquals(user, authenticationResult.getIdentity());
    Assert.assertEquals(new ArrayList<String>(), authenticationResult.getContext().get("permissions"));

    Assert.assertEquals(user, capturedUser.getValue());
    Assert.assertEquals(pwd, capturedPassword.getValue());
    Assert.assertEquals(clientId, capturedClientId.getValue());
    Assert.assertEquals(clientSecret, capturedClientSecret.getValue());
    Assert.assertEquals(token, capturedToken.getValue());

    EasyMock.verify(mockJwtUtils, mockDecodedJWT);
  }

  @Test
  public void authenticateJDBCContextNoUser()
  {
    final String pwd = "pwd";
    EasyMock.replay(mockJwtUtils, mockDecodedJWT);
    AuthenticationResult authenticationResult = auth0Authenticator.authenticateJDBCContext(ImmutableMap.of(
        "password",
        pwd
    ));
    Assert.assertNull(authenticationResult);
    EasyMock.verify(mockJwtUtils, mockDecodedJWT);
  }

  @Test
  public void authenticateJDBCContextNoPwd()
  {
    final String user = "testUser";
    EasyMock.replay(mockJwtUtils, mockDecodedJWT);
    AuthenticationResult authenticationResult = auth0Authenticator.authenticateJDBCContext(ImmutableMap.of(
        "user",
        user
    ));
    Assert.assertNull(authenticationResult);
    EasyMock.verify(mockJwtUtils, mockDecodedJWT);
  }

  @Test
  public void authenticateJDBCContextNoUserPwd()
  {
    EasyMock.replay(mockJwtUtils, mockDecodedJWT);
    AuthenticationResult authenticationResult = auth0Authenticator.authenticateJDBCContext(ImmutableMap.of());
    Assert.assertNull(authenticationResult);
    EasyMock.verify(mockJwtUtils, mockDecodedJWT);
  }

  @Test
  public void authenticateJDBCContextNoClientId()
  {
    final String user = "testUser";
    final String pwd = "pwd";
    auth0Authenticator = new Auth0Authenticator("", "", "test.com", "", null, new DefaultPasswordProvider(""));
    EasyMock.replay(mockJwtUtils, mockDecodedJWT);
    AuthenticationResult authenticationResult = auth0Authenticator.authenticateJDBCContext(ImmutableMap.of(
        "user",
        user,
        "password",
        pwd
    ));
    Assert.assertNull(authenticationResult);
    EasyMock.verify(mockJwtUtils, mockDecodedJWT);
  }

  @Test
  public void authenticateJDBCContextNoClientSecret()
  {
    final String user = "testUser";
    final String pwd = "pwd";
    auth0Authenticator = new Auth0Authenticator("", "", "test.com", "", "id", null);
    EasyMock.replay(mockJwtUtils, mockDecodedJWT);
    AuthenticationResult authenticationResult = auth0Authenticator.authenticateJDBCContext(ImmutableMap.of(
        "user",
        user,
        "password",
        pwd
    ));
    Assert.assertNull(authenticationResult);
    EasyMock.verify(mockJwtUtils, mockDecodedJWT);
  }

  @Test
  public void authenticateJDBCContextNoClientIdSecret()
  {
    final String user = "testUser";
    final String pwd = "pwd";
    auth0Authenticator = new Auth0Authenticator("", "", "test.com", "", null, null);
    EasyMock.replay(mockJwtUtils, mockDecodedJWT);
    AuthenticationResult authenticationResult = auth0Authenticator.authenticateJDBCContext(ImmutableMap.of(
        "user",
        user,
        "password",
        pwd
    ));
    Assert.assertNull(authenticationResult);
    EasyMock.verify(mockJwtUtils, mockDecodedJWT);
  }
}
