package com.vmturbo.auth.api.authorization.jwt;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import java.util.List;
import java.util.Set;

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
    public static final String ADMINISTRATOR = "administrator";

    /**
     * The role for observer.
     */
    public static final String OBSERVER = "OBSERVER";

    /**
     * The role for automator user.
     */
    public static final String AUTOMATOR = "AUTOMATOR";

    /**
     * The role for deployer.
     */
    public static final String DEPLOYER = "DEPLOYER";

    /**
     * The role for advisor.
     */
    public static final String ADVISOR = "ADVISOR";

    /**
     * The role for site_admin.
     */
    public static final String SITE_ADMIN = "SITE_ADMIN";

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
     * These are predefined external groups for all the roles in XL.
     */
    public static final Set<SecurityGroupDTO> PREDEFINED_SECURITY_GROUPS_SET =
            Sets.newHashSet(new SecurityGroupDTO(ADMINISTRATOR.toLowerCase(), DEDICATED_CUSTOMER, ADMINISTRATOR),
                    new SecurityGroupDTO(AUTOMATOR.toLowerCase(), DEDICATED_CUSTOMER, AUTOMATOR),
                    new SecurityGroupDTO(DEPLOYER.toLowerCase(), DEDICATED_CUSTOMER, DEPLOYER),
                    new SecurityGroupDTO(ADVISOR.toLowerCase(), DEDICATED_CUSTOMER, ADVISOR),
                    new SecurityGroupDTO(OBSERVER.toLowerCase(), DEDICATED_CUSTOMER, OBSERVER),
                    new SecurityGroupDTO(SITE_ADMIN.toLowerCase(), DEDICATED_CUSTOMER, SITE_ADMIN));
}
