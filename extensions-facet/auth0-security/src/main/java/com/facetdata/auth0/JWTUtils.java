package com.facetdata.auth0;

import com.auth0.client.auth.AuthAPI;
import com.auth0.exception.Auth0Exception;
import com.auth0.json.auth.TokenHolder;
import com.auth0.jwk.GuavaCachedJwkProvider;
import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.net.AuthRequest;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.BasicAuthUtils;

import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class JWTUtils
{
  private static final Logger LOG = new Logger(JWTUtils.class);
  private static final byte[] SALT = BasicAuthUtils.generateSalt();

  private final long eagerExpirationMillis = 5_000; // expire tokens 5 seconds before their actual expiration time
  // cache of ([User, hash(Pwd)] -> Access Token)
  private final Map<TokenCacheKey, String> tokenCache;
  private final JwkProvider cachedJwkProvider;
  private final String issuer;
  private final String audience;

  public JWTUtils(String issuer, String audience)
  {
    Preconditions.checkNotNull(issuer, "issuer cannot be null, needed for token verification");
    Preconditions.checkNotNull(audience, "audience cannot be null, needed for token verification");
    this.issuer = issuer;
    this.audience = audience;
    // By default it stores 5 keys for 10 hours but these values can be changed
    this.cachedJwkProvider = new GuavaCachedJwkProvider(new UrlJwkProvider(issuer));
    this.tokenCache = new ConcurrentHashMap<>();
  }

  public DecodedJWT verify(String token) throws JwkException
  {
    final String keyId = JWT.decode(token).getKeyId();
    final Jwk jwk = cachedJwkProvider.get(keyId);
    final Algorithm signingAlgo = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
    final JWTVerifier verifier = JWT.require(signingAlgo).withIssuer(issuer).withAudience(audience).build();
    return verifier.verify(token);
  }

  /**
   * This method can be called from mulitple threads simultaneously.
   */
  public String getAccessToken(String user, String pwd, String clientId, String clientSecret)
  {
    final TokenCacheKey tokenCacheKey = new TokenCacheKey(user, pwd);

    String token = tokenCache.get(tokenCacheKey);
    if (token == null) {
      return tokenCache.computeIfAbsent(tokenCacheKey, key -> {
        String newToken;
        try {
          newToken = requestToken(key.user, pwd, clientId, clientSecret);
        }
        catch (Auth0Exception e) {
          LOG.error(e, "Unable to fetch access token for user [%s]", key.user);
          return null;
        }
        return newToken;
      });
    } else if (JWT.decode(token).getExpiresAt().before(new Date(System.currentTimeMillis() - eagerExpirationMillis))) {
      return tokenCache.compute(tokenCacheKey, (key, previousToken) -> {
        // check if no other thread has already refreshed the token
        if (previousToken == null || JWT.decode(previousToken)
                                        .getExpiresAt()
                                        .before(new Date(System.currentTimeMillis() - eagerExpirationMillis))) {
          String newToken;
          try {
            newToken = requestToken(key.user, pwd, clientId, clientSecret);
          }
          catch (Auth0Exception e) {
            LOG.error(e, "Unable to refresh access token for user [%s]", key.user);
            return null;
          }
          return newToken;
        }
        return previousToken;
      });
    } else {
      return token;
    }
  }

  public String requestToken(String user, String pwd, String clientId, String clientSecret) throws Auth0Exception
  {
    AuthAPI auth = new AuthAPI(issuer, clientId, clientSecret);
    AuthRequest request = auth.login(user, pwd)
                              .setAudience(audience);
    TokenHolder holder = request.execute();
    return holder.getAccessToken();
  }

  static class TokenCacheKey
  {
    private final String user;
    private final byte[] hashedPwd;

    TokenCacheKey(String user, String pwd)
    {
      Preconditions.checkNotNull(user);
      Preconditions.checkNotNull(pwd);
      this.user = user;
      this.hashedPwd = BasicAuthUtils.hashPassword(pwd.toCharArray(), JWTUtils.SALT, BasicAuthUtils.DEFAULT_KEY_ITERATIONS);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TokenCacheKey that = (TokenCacheKey) o;
      return user.equals(that.user) &&
             Arrays.equals(hashedPwd, that.hashedPwd);
    }

    @Override
    public int hashCode()
    {
      int result = Objects.hash(user);
      result = 31 * result + Arrays.hashCode(hashedPwd);
      return result;
    }
  }
}
