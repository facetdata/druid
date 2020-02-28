package com.facetdata.auth0;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TokenCacheKeyTest
{
  @Test
  public void testKeyCache()
  {
    final String user1 = "user1";
    final String pwd = "pwd";
    final String value = "value";
    final JWTUtils.TokenCacheKey tokenCacheKey = new JWTUtils.TokenCacheKey(user1, pwd);
    final Map<JWTUtils.TokenCacheKey, String> tokenCacheKeyMap = ImmutableMap.of(tokenCacheKey, value);
    // run it multiple times to make sure results are consistent
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(value, tokenCacheKeyMap.get(new JWTUtils.TokenCacheKey(user1, pwd)));
      final JWTUtils.TokenCacheKey tokenCacheKeyWrongPwd = new JWTUtils.TokenCacheKey(user1, "pwd2");
      Assert.assertNull(tokenCacheKeyMap.get(tokenCacheKeyWrongPwd));
      final JWTUtils.TokenCacheKey tokenCacheKeyDiffUser = new JWTUtils.TokenCacheKey("user2", "pwd1");
      Assert.assertNull(tokenCacheKeyMap.get(tokenCacheKeyDiffUser));
    }
  }
}
