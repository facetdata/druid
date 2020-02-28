package com.facetdata.inspector.http;

import com.facetdata.inspector.QuotasInspector;
import com.google.inject.Inject;
import org.apache.druid.server.initialization.jetty.ServletFilterHolder;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

public class ResourceInspectorServletFilterHolder implements ServletFilterHolder
{
  private QuotasInspector quotasInspector;

  @Inject
  public ResourceInspectorServletFilterHolder(QuotasInspector quotasInspector) throws Exception
  {
    this.quotasInspector = quotasInspector;
  }

  @Override
  public Filter getFilter()
  {
    return new ResourceInspectorFilter(quotasInspector);
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return null;
  }

  @Override
  public String getPath()
  {
    return "/druid/v2";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }
}
