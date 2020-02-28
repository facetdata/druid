package com.facetdata.auth0;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class Auth0PermissionPatternTest
{
  // These tests the pattern of valid and invalid permission in auth0
  @Parameterized.Parameters(name = "{index}: Permission:{0} ExpectedMatch:{1}")
  public static Iterable<Object[]> data()
  {
    return Arrays.asList(
        new Object[][]{
            {"read:datasource:*", true},
            {"write:datasource:*", true},
            {"read:config:*", true},
            {"write:config:*", true},
            {"read:state:*", true},
            {"write:state:*", true},
            {"READ:DATASOURCE:*", true},
            {"WRITE:DATASOURCE:*", true},
            {"READ:CONFIG:*", true},
            {"WRITE:CONFIG:*", true},
            {"READ:STATE:*", true},
            {"WRITE:STATE:*", true},
            {"READ:datasource:ds1", true},
            // random state and configs names are allowed
            {"read:config:random", true},
            {"read:state:random", true},
            // mix-match for upper/lower case allowed
            {"read:Datasource:*", true},
            {"rEad:dataSOUrce:*", true},
            // no restriction on datasource names
            {"write:datasource:datasource", true},
            {"write:datasource:druid", true},
            {"write:datasource:asdf*fda-32", true},
            {"write:datasource:@#$%@#$", true},
            // stars are allowed
            {"*:*:*", true},
            {"*:config:*", true},
            {"read:*:*", true},
            {"read:*:stuff", true},
            // other wildcards and regex not allowed
            {".+:*:*", false},
            // mix-match of characters and star are not allowed for action and resource type
            {"read:co*g:*", false},
            // actions other than read/write and resource other than datasource, config and state not allowed
            {"allow:datasource:*", false},
            {"read:policy:*", false},
            // pattern should have all three non-empty parts separated by ":"
            {"", false},
            {"*", false},
            {"*:", false},
            {"*:*", false},
            {"*:*:", false},
            {"write", false},
            {"write::*", false},
            {":config:*", false},
            {"read:config", false},
            {"read:config:", false},
            {"read config datasource", false}
        });
  }

  private final String permission;
  private final boolean expectedMatch;

  public Auth0PermissionPatternTest(String permission, boolean expectedMatch)
  {
    this.permission = permission;
    this.expectedMatch = expectedMatch;
  }

  @Test
  public void testValidatePermission()
  {
    Assert.assertEquals(expectedMatch, Auth0Authorizer.validatePermission(permission).matches());
  }
}
