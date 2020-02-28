Auth0 Security Extension
----------
This extension uses auth0 issued tokens to perform authentication and authorization checks. Authenticator expects request
to contain `Authorization` header with value `Bearer <token>`, `<token>` is JWT access token containing `permissions` claims.
This token is verified to match `issuer` and `audience` from the runtime configs, `permissions` are extracted from the token
which is used by the Authorizer to perform authorization checks. Please see `Auth0PermissionPatternTest` for valid permission
patterns and `Auth0AuthorizerTest` for authorization behaviour. 

This extension does not handle auth between druid nodes for internal druid communication so it can be used in combination with
druid-basic-security extension for escalator. Following configs should be used -
 
```
druid.extensions.loadList=[<other extensions> ..., "auth0-security", "druid-basic-security"]

# Authenticator
druid.auth.authenticatorChain=["auth0", "basic"]

druid.auth.authenticator.auth0.type=auth0
druid.auth.authenticator.auth0.issuer=<auth0 application Domain>
druid.auth.authenticator.auth0.audience=<auth0 API Identifier>
druid.auth.authenticator.auth0.authorizerName=auth0

druid.auth.authenticator.basic.type=basic
druid.auth.authenticator.basic.initialInternalClientPassword=<some_pwd>
druid.auth.authenticator.basic.authorizerName=basic

# Authorizer
druid.auth.authorizers=["auth0", "basic"]
druid.auth.authorizer.auth0.type=auth0
druid.auth.authorizer.basic.type=basic

# Escalator
druid.escalator.type=basic
druid.escalator.internalClientUsername=druid_system
druid.escalator.internalClientPassword=<some_pwd>
druid.escalator.authorizerName=basic
```

Support for username/pwd based JDBC authentication
-------
This extension supports username/pwd based authentication for JDBC connections. Following configuration needs to be done
in auth0 to allow this -

1. Create a Machine to Machine (M2M) application, which is a proxy for actual application that will be sending username/pwd 
of the user for authentication.

2. Set the Token Endpoint Authentication Method to POST in the application settings.

3. In the Advanced Settings allow Password grant type.

4. In the Tenant Settings - Username-Password-Authentication needs to set for the Default Directory. This indicates that 
for authenticating users via pwd, internal auth0 database should be used which contains users information. This is the 
same string as present in the Database section of Connections settings.

5. A new user can be created for this application (e.g. looker, superset etc.) under the Users & Roles settings.

6. Apart from the issuer and audience configs of the API, clientId and client secret of the application needs to be securely 
passed to the Druid process through runtime properties. Username and Password will be sent in the JDBC context. Using all 
these information Druid can request access token on behalf of user and can perform authorization checks.

Following configuration needs to be set in druid runtime props to support JDBC authentication -
```
druid.auth.authenticator.auth0.clientId=<clientId of the M2M application>
druid.auth.authenticator.auth0.clientSecret=<clientSecret of the M2M application, this field is a [password provider][1]>
```

[1]: https://druid.apache.org/docs/latest/operations/password-provider.html

### Encrypting username password sent over wire 
For JDBC auth over TLS, clients using avatica jdbc driver to query Apache Druid have to specify https url in the connection 
string along with trust store location and password as specified here - https://calcite.apache.org/avatica/docs/client_reference.html

So the the connect url will be of form - `String.format("jdbc:avatica:remote:url=%s;truststore=%s;truststore_password=%s", url, trustStore, trustStorePwd)`

where - 

1. `url` - `https://<router_host_port>/druid/v2/sql/avatica/`

2. `trustStore` - if standard CA is used to sign certs then it can be default java trust store which is at `${JAVA_HOME}/jre/lib/security/cacerts`

3. `trustStorePwd` - trust store password, default pwd for java trust store is `changeit`
