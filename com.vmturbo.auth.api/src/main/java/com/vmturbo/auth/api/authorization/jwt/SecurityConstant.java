package com.vmturbo.auth.api.authorization.jwt;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import io.grpc.Context;
import io.grpc.Metadata;

/**
 * Define security related constants.
 */
public class SecurityConstant {
    /**
     * key for user id
     */
    public static final Context.Key<String> USER_ID_CTX_KEY = Context.key("userId");

    /**
     * key for user's IP address
     */
    public static final Context.Key<String> USER_IP_ADDRESS_KEY = Context.key("userIpAddress");

    /**
     * key for user's UUID
     */
    public static final Context.Key<String> USER_UUID_KEY = Context.key("userUuid");

    /**
     * key for user's roles
     */
    public static final Context.Key<List<String>> USER_ROLES_KEY = Context.key("userRoles");

    /**
     * key for user's scope groups
     */
    public static final Context.Key<List<Long>> USER_SCOPE_GROUPS_KEY = Context.key("userScopeGroups");

    /**
     * key for the JWT in the grpc Context
     */
    public static final Context.Key<String> CONTEXT_JWT_KEY = Context.key("jwt");


    /**
     * key for JWT token in call metadata
     */
    public static final Metadata.Key<String> JWT_METADATA_KEY = Metadata.Key.of("jwt", ASCII_STRING_MARSHALLER);

    /**
     * HTTP header attribute to identify component
     */
    public static final String COMPONENT_ATTRIBUTE = "x-turbo-component";

    /**
     * All roles in the JWT begin with this prefix.
     */
    public static final String ROLE_STRING = "ROLE_";

    /**
     * The role for the ADMINISTRATOR.
     */
    public static final String ADMINISTRATOR = PredefinedRole.ADMINISTRATOR.name();

    /**
     * The role for observer.
     */
    public static final String OBSERVER = PredefinedRole.OBSERVER.name();

    /**
     * The role for operational observer.
     */
    public static final String OPERATIONAL_OBSERVER = PredefinedRole.OPERATIONAL_OBSERVER.name();

    /**
     * The role for automator user.
     */
    public static final String AUTOMATOR = PredefinedRole.AUTOMATOR.name();

    /**
     * The role for deployer.
     */
    public static final String DEPLOYER = PredefinedRole.DEPLOYER.name();

    /**
     * The role for advisor.
     */
    public static final String ADVISOR = PredefinedRole.ADVISOR.name();

    /**
     * The role for shared_observer.
     */
    public static final String SHARED_OBSERVER = PredefinedRole.SHARED_OBSERVER.name();

    /**
     * The role for shared_advisor.
     */
    public static final String SHARED_ADVISOR = PredefinedRole.SHARED_ADVISOR.name();

    /**
     * The role for site_admin.
     */
    public static final String SITE_ADMIN = PredefinedRole.SITE_ADMIN.name();

    /**
     * The role for report_editor.
     */
    public static final String REPORT_EDITOR = PredefinedRole.REPORT_EDITOR.name();

    /**
     * The legacy customer type.
     */
    public static final String DEDICATED_CUSTOMER = "DedicatedCustomer";

    /**
     * The HTTP turbo JWT token header.
     */
    public static final String X_TURBO_TOKEN = "x-turbo-token";

    /**
     * The HTTP turbo user name header.
     */
    public static final String X_TURBO_USER = "x-turbo-user";

    /**
     * The HTTP turbo role header.
     */
    public static final String X_TURBO_ROLE = "x-turbo-roles";

    /**
     * Default credential.
     */
    public static final String CREDENTIALS = "***";

    /**
     * The header name for authentication token.
     */
    public static final String AUTH_HEADER_NAME = "x-auth-token";

    /**
     * The header name for identifying oauth2 provider.
     */
    public static final String OAUTH2_HEADER_NAME = "x-oauth2";

    /**
     * Hydra oauth2 provider.
     */
    public static final String HYDRA = "hydra";

    /**
     * Http scheme.
     */
    public static final String HTTP = "http";

    /**
     * Hydra admin.
     */
    public static final String HYDRA_ADMIN = "hydra-admin";

    /**
     * Hydra admin port.
     */
    public static final String HYDRA_ADMIN_PORT = "4445";

    /**
     * Hydra introspect path.
     */
    public static final String HYDRA_INTROSPECT_PATH = "/oauth2/introspect";

    /**
     * Hydra clients path.
     */
    public static final String HYDRA_CLIENTS_PATH = "/clients";

    /**
     * Client id.
     */
    public static final String CLIENT_ID = "client_id";

    /**
     * Client secret.
     */
    public static final String CLIENT_SECRET = "client_secret";

    /**
     * Client name.
     */
    public static final String CLIENT_NAME = "client_name";

    /**
     * Client credentials.
     */
    public static final String CLIENT_CREDENTIALS = "client_credentials";

    /**
     * Client secret post.
     */
    public static final String CLIENT_SECRET_POST = "client_secret_post";

    /**
     * Client grants.
     */
    public static final String CLIENT_GRANTS = "grant_types";

    /**
     * Client scope.
     */
    public static final String CLIENT_SCOPE = "scope";

    /**
     * Client auth method.
     */
    public static final String CLIENT_AUTH_METHOD = "token_endpoint_auth_method";

    /**
     * Token.
     */
    public static final String TOKEN = "token";

    /**
     * Tokens.
     */
    public static final String TOKENS = "tokens";

    /**
     * Default ip.
     */
    public static final String DEFAULT_IP = "UNKNOWN";

    /**
     * Active.
     */
    public static final String ACTIVE = "active";

    /**
     * Constant for security target name for audit log entries.
     */
    public static final String AUDIT_LOG_SECURITY = "Security";

    /**
     * Constant for client network.
     */
    public static final String CLIENT_NETWORK = "client-network";

    /**
     * Constant for client network token metadata.
     */
    public static final String TOKEN_NAME = "name";
    public static final String CREATED = "created";
    public static final String CLAIMS_MADE = "claimsMade";
    public static final String CLAIMS_REMAINING = "claimsRemaining";
    public static final String CLAIMS_EXPIRATION = "claimExpiration";

    /**
     * Constant for client network port.
     */
    public static final String CLIENT_NETWORK_PORT = "8080";

    /**
     * Constant for client network path.
     */
    public static final String CLIENT_NETWORK_PATH = "/api/v1/";

    /**
     * These are predefined external groups for all the roles in XL.
     */
    public static final Set<SecurityGroupDTO> PREDEFINED_SECURITY_GROUPS_SET =
            Sets.newHashSet(new SecurityGroupDTO(ADMINISTRATOR.toLowerCase(), DEDICATED_CUSTOMER, ADMINISTRATOR),
                    new SecurityGroupDTO(AUTOMATOR.toLowerCase(), DEDICATED_CUSTOMER, AUTOMATOR),
                    new SecurityGroupDTO(DEPLOYER.toLowerCase(), DEDICATED_CUSTOMER, DEPLOYER),
                    new SecurityGroupDTO(ADVISOR.toLowerCase(), DEDICATED_CUSTOMER, ADVISOR),
                    new SecurityGroupDTO(OBSERVER.toLowerCase(), DEDICATED_CUSTOMER, OBSERVER),
                    new SecurityGroupDTO(OPERATIONAL_OBSERVER.toLowerCase(), DEDICATED_CUSTOMER, OPERATIONAL_OBSERVER),
                    new SecurityGroupDTO(SITE_ADMIN.toLowerCase(), DEDICATED_CUSTOMER, SITE_ADMIN));

    /**
     * These are predefined roles in XL.
     */
    public static final Set<String> PREDEFINED_ROLE_SET = Stream.of(PredefinedRole.values())
            .map(Enum::name)
            .collect(Collectors.toSet());

    /**
     * Enum for predefine roles.
     */
    public enum PredefinedRole {
        /**
         * ADMINISTRATOR role.
         */
        ADMINISTRATOR,
        /**
         * SITE_ADMIN role.
         */
        SITE_ADMIN,
        /**
         * AUTOMATOR role.
         */
        AUTOMATOR,
        /**
         * DEPLOYER role.
         */
        DEPLOYER,
        /**
         * ADVISOR role.
         */
        ADVISOR,
        /**
         * OBSERVER role.
         */
        OBSERVER,
        /**
         * OPERATIONAL_OBSERVER role.
         */
        OPERATIONAL_OBSERVER,
        /**
         * SHARED_ADVISOR role.
         */
        SHARED_ADVISOR,
        /**
         * SHARED_OBSERVER role.
         */
        SHARED_OBSERVER,
        /**
         * REPORT role.
         */
        REPORT_EDITOR,
    }

    /**
     * Role to privilege map which is used for calculating privileges (permissions). The
     * bigger number on value, the more privilege the role has. For example, ADMINISTRATOR role have
     * max privileges with 100 as value; SHARED_OBSERVER has min privileges with 40 as value.
     */
    public static final Map<String, Integer> PRIVILEGE_MAP =
            ImmutableMap.<String, Integer>builder()
                    .put(ADMINISTRATOR, 100)
                    .put(SITE_ADMIN, 90)
                    .put(AUTOMATOR, 80)
                    .put(ADVISOR, 70)
                    .put(SHARED_ADVISOR, 60)
                    .put(DEPLOYER, 50)
                    .put(OBSERVER, 40)
                    .put(SHARED_OBSERVER, 30)
                    .put(OPERATIONAL_OBSERVER, 20)
                    .put(REPORT_EDITOR, 10)
                    .build();

    /**
     * The Consul key
     */
    @VisibleForTesting
    public static final String CONSUL_KEY = "dbcreds";

    /**
     * The Consul root DB username key.
     */
    public static final String CONSUL_ROOT_DB_USER_KEY = "rootdbUsername";

    /**
     * The Postgres DB root username key.
     */
    public static final String POSTGRES_ROOT_USER_KEY = "postgresRootUsername";

    /**
     * The Consul root DB password key.
     */
    public static final String CONSUL_ROOT_DB_PASS_KEY = "rootdbcreds";

    /**
     * The arango root DB password key.
     */
    public static final String ARANGO_ROOT_PW_KEY = "arangocreds";

    /**
     * The influx root DB password key.
     */
    public static final String INFLUX_ROOT_PW_KEY = "influxcreds";

}
