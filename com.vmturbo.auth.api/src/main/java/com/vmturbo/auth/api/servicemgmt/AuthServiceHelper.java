package com.vmturbo.auth.api.servicemgmt;

import java.util.EnumMap;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * AuthServiceHelper helper class to define enums and mappings for auth services and probe security.
 */
public final class AuthServiceHelper {

    private AuthServiceHelper() {}

    /**
     * The location.
     */
    public enum LOCATION {
        /**
         * Local Service.
         */
        LOCAL,
        /**
         * External Service.
         */
        EXTERNAL
    }

    /**
     * The service type.
     */
    public enum TYPE {
        /**
         * Service is probe.
         */
        PROBE
    }

    /**
     * The service role.
     */
    public enum ROLE {
        /**
         * Role is probe admin.
         */
        PROBE_ADMIN
    }

    /**
     * The capability type.
     */
    public enum CAPABILITY {
        /**
         * Probe session connect capability.
         */
        PROBE_SESSION_CONNECT
    }

    private static final EnumMap<ROLE, Set<CAPABILITY>> ROLE_CAPABILITY_MAPPING = new EnumMap<>(ROLE.class);

    static {
        ROLE_CAPABILITY_MAPPING.put(ROLE.PROBE_ADMIN, Sets.newHashSet(CAPABILITY.PROBE_SESSION_CONNECT));
    }
}
