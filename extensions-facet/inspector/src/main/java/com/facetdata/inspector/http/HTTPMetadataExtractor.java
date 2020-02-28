package com.facetdata.inspector.http;

import com.facetdata.inspector.Resource;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.EnumMap;
import java.util.stream.Stream;

/**
 * Used to extract relevant information from request/response headers like resource usage, tenantId etc.
 */
public class HTTPMetadataExtractor
{
  private static final EmittingLogger log = new EmittingLogger(HTTPMetadataExtractor.class);
  private static final String UPDATE_HEADER_KEY = "UPDATE_RESOURCES";
  private static final String TENANT_HEADER_KEY = "TENANT_ID";

  /**
   * Used to extract resource usage values from response headers after query is run.
   * <p>
   * Header format -
   * "UPDATE_RESOURCES": "Resource1:USAGE_NUMBER1, Resource2:USAGE_NUMBER2, ...."
   *
   * @return returns a map of {@link Resource} to usage value
   */
  public static EnumMap<Resource, Long> extractResourceUsageInformation(HttpServletResponse response)
  {
    String headerValue = response.getHeader(UPDATE_HEADER_KEY);
    log.debug("Got headerValue [%s]", headerValue);
    final EnumMap<Resource, Long> resourcesAndValuesMap = new EnumMap<>(Resource.class);
    if (headerValue == null || headerValue.length() == 0) {
      log.makeAlert("No resource usage header found !!").emit();
      return resourcesAndValuesMap;
    }

    String[] resourcesAndValues = headerValue.split(",");
    Stream.of(resourcesAndValues).forEach(s -> {
      String[] resourceAndValue = s.split(":");
      Preconditions.checkState(resourceAndValue.length > 1);
      Resource resource = Resource.valueOf(StringUtils.toUpperCase(resourceAndValue[0].trim()));
      long val = Long.parseLong(resourceAndValue[1].trim());
      if (val == 0) {
        log.warn("Found [%s] usage to be zero in the header", resource);
      }
      resourcesAndValuesMap.put(resource, val);
    });

    log.debug("Extracted resource information [%s]", resourcesAndValuesMap);
    return resourcesAndValuesMap;
  }

  public static int extractTenantId(HttpServletRequest request)
  {
    final String tenantId = request.getHeader(TENANT_HEADER_KEY);
    log.debug("Got header [%s]", tenantId);
    if (!Strings.isNullOrEmpty(tenantId)) {
      return Integer.parseInt(tenantId);
    } else {
      throw new ISE("No tenant id found in the request headers");
    }
  }
}
