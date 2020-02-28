package com.facetdata.inspector.http;

import com.facetdata.inspector.ConstraintCheckResult;
import com.facetdata.inspector.QuotasInspector;
import com.facetdata.inspector.Resource;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumMap;

public class ResourceInspectorFilter implements Filter
{
  private static final EmittingLogger log = new EmittingLogger(ResourceInspectorFilter.class);
  private final QuotasInspector quotasInspector;

  public ResourceInspectorFilter(QuotasInspector quotasInspector)
  {
    this.quotasInspector = quotasInspector;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException
  {

  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException
  {
    // extract information required to enforce caps
    final int tenantId = HTTPMetadataExtractor.extractTenantId((HttpServletRequest) request);

    final long startTimeMillis = System.currentTimeMillis();

    ConstraintCheckResult result = null;
    try {
      result = quotasInspector.inspect(tenantId, startTimeMillis);
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }

    if (result.isQuotaExceeded()) {
      quotasInspector.getEmitter().emit(getEventBuilderForQuotaExceeded(result));
      log.debug("Tenant [%d]: %s", tenantId, result.getMessage());
      // 430 - unreserved status code
      ((HttpServletResponse) response).sendError(430, result.getMessage());
      return;
    }

    // let the query go through since it passed the enforcement checks
    chain.doFilter(request, response);

    final long endTimeMillis = System.currentTimeMillis();
    final EnumMap<Resource, Long> deltaValues = HTTPMetadataExtractor.extractResourceUsageInformation((HttpServletResponse) response);
    // increase query count by 1
    deltaValues.put(Resource.QUERY_COUNT, 1L);

    // update usage values
    quotasInspector.update(
        tenantId,
        deltaValues,
        startTimeMillis,
        endTimeMillis
    );
  }

  @Override
  public void destroy()
  {

  }

  private ServiceEventBuilder<ServiceMetricEvent> getEventBuilderForQuotaExceeded(ConstraintCheckResult result)
  {
    ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    result.getExhaustedResources()
          .forEach(unitConstraintResult -> unitConstraintResult.getDimensionMap().forEach(builder::setDimension));
    return builder.build("quota/exceeded/count", 1);
  }
}
