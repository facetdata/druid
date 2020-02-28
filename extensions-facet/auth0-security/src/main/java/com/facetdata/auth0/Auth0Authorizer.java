package com.facetdata.auth0;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("auth0")
public class Auth0Authorizer implements Authorizer
{
  private static final EmittingLogger log = new EmittingLogger(Auth0Authorizer.class);
  // permissions are of form ACTION:RESEOURCE_TYPE:RESOURCE_NAME
  private static final Pattern permissionPattern = Pattern.compile(
      "^(read|write|\\*):(datasource|config|state|\\*):(.+|\\*)$",
      Pattern.CASE_INSENSITIVE
  );

  @Override
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    log.debug(
        "Authorizing request of [%s] on [%s] for [%s] with context [%s]",
        action.name(),
        resource.getName(),
        authenticationResult.getIdentity(),
        authenticationResult.getContext()
    );
    final String id = authenticationResult.getIdentity();
    final List<String> permissions = authenticationResult.getContext() != null ? (List<String>) authenticationResult.getContext().get("permissions") : null;
    if (permissions == null || permissions.size() == 0) {
      String alertMsg = StringUtils.format("No permissions found to perform authorization of user [%s]", id);
      log.makeAlert(alertMsg).emit();
      return new Access(false, alertMsg);
    }

    boolean access = false;
    for (String permission : permissions) {
      Matcher matcher = validatePermission(permission);
      if (!matcher.matches()) {
        log.makeAlert("Invalid permission pattern found: [%s], skipping it!", permission).emit();
        continue;
      }
      String actionStr = matcher.group(1);
      String resourceType = matcher.group(2);
      String resourceName = matcher.group(3);
      boolean match = ("*".equals(actionStr) || actionStr.equalsIgnoreCase(action.name())) &&
                      ("*".equals(resourceType) || resourceType.equalsIgnoreCase(resource.getType().name())) &&
                      // resource name match is case sensitive
                      ("*".equals(resourceName) || resourceName.equals(resource.getName()));
      if (match) {
        access = true;
        break;
      }
    }
    return new Access(access);
  }

  static Matcher validatePermission(String permission)
  {
    return permissionPattern.matcher(permission);
  }
}
