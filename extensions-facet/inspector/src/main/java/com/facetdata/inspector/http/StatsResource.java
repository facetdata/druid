package com.facetdata.inspector.http;

import com.facetdata.inspector.QuotasInspector;
import com.facetdata.inspector.Resource;
import com.facetdata.inspector.stats.StatsManager;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.http.security.ConfigResourceFilter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.EnumMap;

@Path("/druid-ext/inspector/v1/stats")
public class StatsResource
{
  private static final EmittingLogger log = new EmittingLogger(StatsResource.class);
  private final StatsManager statsManager;
  private final QuotasInspector quotasInspector;

  @Inject
  public StatsResource(StatsManager statsManager, QuotasInspector quotasInspector)
  {
    this.statsManager = statsManager;
    this.quotasInspector = quotasInspector;
  }

  @POST
  @Path("/caps/{tenantId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response addOrUpdateCaps(
      @PathParam("tenantId") final int tenantId,
      final EnumMap<GranularityType, EnumMap<Resource, Long>> tenantCaps,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    try {
      log.debug("Adding/Updating caps [%s] for tenant [%d]", tenantCaps, tenantId);
      statsManager.addOrUpdateCapsForTenant(tenantId, tenantCaps, new AuditInfo(author, comment, req.getRemoteAddr()));
      quotasInspector.syncCapsForTenant(tenantId);
      return Response.ok().build();
    }
    catch (Exception e) {
      log.error(e, "Add/update caps for tenant [%d] failed", tenantId);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                     .entity(ImmutableMap.of("error", e.toString()))
                     .build();
    }
  }
}
