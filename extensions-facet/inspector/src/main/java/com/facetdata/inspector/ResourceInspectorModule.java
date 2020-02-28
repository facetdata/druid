package com.facetdata.inspector;

import com.facetdata.inspector.config.InspectorConfig;
import com.facetdata.inspector.config.SqlDbConfig;
import com.facetdata.inspector.http.ResourceInspectorServletFilterHolder;
import com.facetdata.inspector.http.StatsResource;
import com.facetdata.inspector.stats.StatsManager;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.initialization.jetty.ServletFilterHolder;

import java.util.List;

public class ResourceInspectorModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "facet.inspector.config", InspectorConfig.class);
    JsonConfigProvider.bind(binder, "facet.inspector.statsManager", StatsManager.class);
    JsonConfigProvider.bind(binder, "facet.inspector.statsManager.sqldb", SqlDbConfig.class);

    binder.bind(QuotasInspector.class).in(LazySingleton.class);
    Jerseys.addResource(binder, StatsResource.class);

    // TODO - add one more filterholder to cover SQL path
    Multibinder.newSetBinder(binder, ServletFilterHolder.class)
               .addBinding().to(ResourceInspectorServletFilterHolder.class);
  }
}
