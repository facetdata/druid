package com.facetdata.connector.bigquery;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class BigQueryConnectorModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule()
            .registerSubtypes(
                new NamedType(BigQueryDatabaseConnector.class, "bigquery"),
                new NamedType(BigQueryFirehoseFactory.class, "bigquery")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
