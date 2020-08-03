package com.vmturbo.api.component.external.api;

import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import com.vmturbo.api.component.security.HeaderMapper;
import com.vmturbo.api.component.security.IntersightHeaderMapper;

/**
 * Factory to create vendor specific header. Currently only support Cisco Intersight.
 */
public class HeaderMapperFactory {

    // map vendor roles to XL external groups.
    private static final String ADMINISTRATOR = "administrator";
    private static final String OBSERVER = "observer";
    private static final String ADVISOR = "advisor";
    private static final String AUTOMATOR = "automator";
    private static final String DEPLOYER = "deployer";

    private static final Map<ExternalVendor, MapperGenerator<String, HeaderMapper>>
            EXTERNAL_VENDOR_TO_INTERNAL_USER_MAPPER =
            ImmutableMap.of(ExternalVendor.CISCO_INTERSIGHT,
                    (externalAdminRoles, externalObserverRoles, externalUser, externalRole, jwtTokenPublicKeyTag, jwtToken)
                            -> getCiscoIntersightHeaderMapper(
                            externalAdminRoles, externalObserverRoles, externalUser, externalRole,
                            jwtTokenPublicKeyTag, jwtToken));

    private HeaderMapperFactory() {}

    /**
     * Factory method to get {@link HeaderMapper}. Default to Cisco InterSight.
     *
     * @param externalVendor external vendor to integrate with.
     * @param externalAdminRoles external vendor admin roles.
     * @param externalObserverRoles external vendor observer roles
     * @param externalUser external user name tag from header to get user name.
     * @param externalRole external user role tag from header to get uer role.
     * @param jwtTokenPublicKeyTag the vender's JWT token public key property name.
     * @param jwtTokenTag the vender's JWT token property name.
     * @return {@link HeaderMapper} for the specific vendor.  Default to Cisco InterSight.
     */
    public static HeaderMapper getHeaderMapper(@Nonnull Optional<ExternalVendor> externalVendor,
            @Nonnull String externalAdminRoles, @Nonnull String externalObserverRoles,
            @Nonnull String externalUser, @Nonnull String externalRole,
            @Nonnull String jwtTokenPublicKeyTag, @Nonnull String jwtTokenTag) {
        return externalVendor.map(vendor -> EXTERNAL_VENDOR_TO_INTERNAL_USER_MAPPER.get(vendor)
                .generate(externalAdminRoles, externalObserverRoles, externalUser, externalRole,
                        jwtTokenPublicKeyTag, jwtTokenTag))
                .orElse(getCiscoIntersightHeaderMapper(externalAdminRoles, externalObserverRoles,
                        externalUser, externalRole, jwtTokenPublicKeyTag, jwtTokenTag));
    }

    private static HeaderMapper getCiscoIntersightHeaderMapper(@Nonnull String externalAdminRoles,
            @Nonnull String externalObserverRoles, @Nonnull String externalUser,
            @Nonnull String externalRole, @Nonnull String jwtTokenPublicKeyTag,
            @Nonnull String jwtToken) {
        final Builder builder = ImmutableMap.<String, String>builder();
        final StringTokenizer adminTokenizer = new StringTokenizer(externalAdminRoles, ",");
        while (adminTokenizer.hasMoreTokens()) {
            builder.put(adminTokenizer.nextToken(), ADMINISTRATOR);
        }
        final StringTokenizer readOnlyTokenizer = new StringTokenizer(externalObserverRoles, ",");
        while (readOnlyTokenizer.hasMoreTokens()) {
            builder.put(readOnlyTokenizer.nextToken(), OBSERVER);
        }
        return new IntersightHeaderMapper(builder.build(), externalUser, externalRole,
                jwtTokenPublicKeyTag, jwtToken);
    }

    /**
     * Supported external vendors.
     */
    enum ExternalVendor {
        CISCO_INTERSIGHT
    }

    /**
     * Function interface to build vendor specific {@link HeaderMapper}.
     *
     * @param <R> input type
     * @param <T> oupput type
     */
    @FunctionalInterface
    private interface MapperGenerator<R, T> {
        T generate(R externalAdminRoles, R externalObserverRoles, R externalUser, R externalRole,
                R jwtTokenPublicKeyTag, R jwtToken);
    }
}