package com.facetdata.auth0;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class Auth0SecurityModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("com.facetdata.auth0.Auth0SecurityModule")
            .registerSubtypes(Auth0Authenticator.class)
            .registerSubtypes(Auth0Authorizer.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
