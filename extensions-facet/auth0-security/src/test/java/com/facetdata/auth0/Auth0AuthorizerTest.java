package com.facetdata.auth0;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@RunWith(Parameterized.class)
public class Auth0AuthorizerTest
{
  private final AuthenticationResult authenticationResult;
  private final Resource resource;
  private final Action action;
  private final Access expectedAccess;

  private final Auth0Authorizer auth0Authorizer = new Auth0Authorizer();


  @Parameterized.Parameters(name = "{index}: Context:{0} Resource:{1} Action:{2} ExpectedAccess:{3}")
  public static Iterable<Object[]> data()
  {
    return Arrays.asList(
        new Object[][]{
            {
                // invalid permission patterns will be ignored
                ImmutableMap.of("permissions", Lists.newArrayList("invalid", "read:datasource:*")),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.READ,
                new Access(true)
            },
            {
                ImmutableMap.of("permissions", Lists.newArrayList("read:datasource:ds1", "invalid")),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.READ,
                new Access(true)
            },
            {
                ImmutableMap.of("permissions", Lists.newArrayList("*:datasource:*", "invalid")),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(true)
            },
            {
                ImmutableMap.of("permissions", Lists.newArrayList("random", "*:CONFIG:*", "invalid")),
                new Resource("con1", ResourceType.CONFIG),
                Action.WRITE,
                new Access(true)
            },
            {
                ImmutableMap.of("permissions", Collections.singletonList("*:*:S1")),
                new Resource("S1", ResourceType.STATE),
                Action.WRITE,
                new Access(true)
            },
            // Resource name checks are case sensistive
            {
                ImmutableMap.of("permissions", Collections.singletonList("*:DATASOURCE:DS1")),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(false)
            },
            {
                ImmutableMap.of("permissions", Collections.singletonList("read:datasource:*")),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(false)
            },
            {
                ImmutableMap.of("permissions", Collections.singletonList("*:datasource:ds2")),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(false)
            },
            {
                ImmutableMap.of("permissions", Lists.newArrayList("invalid", "random")),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(false)
            },
            {
                ImmutableMap.of("permissions", new ArrayList<>()),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(false)
            },
            {
                ImmutableMap.of(),
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(false)
            },
            {
                null,
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.WRITE,
                new Access(false)
            }
        });
  }

  public Auth0AuthorizerTest(
      Map<String, Object> context,
      Resource resource,
      Action action,
      Access expectedAccess
  )
  {
    this.authenticationResult = new AuthenticationResult("", "", "", context);
    this.resource = resource;
    this.action = action;
    this.expectedAccess = expectedAccess;
    ServiceEmitter emitter = new ServiceEmitter("test", "test", new NoopEmitter());
    EmittingLogger.registerEmitter(emitter);
  }

  @Test
  public void authorize()
  {
    Assert.assertEquals(
        expectedAccess.isAllowed(),
        auth0Authorizer.authorize(authenticationResult, resource, action).isAllowed()
    );
  }
}
