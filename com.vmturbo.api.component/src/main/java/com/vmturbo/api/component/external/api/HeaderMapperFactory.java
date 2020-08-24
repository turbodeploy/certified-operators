package com.vmturbo.api.component.external.api;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_ROLE_SET;

import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.apache.commons.collections4.map.CaseInsensitiveMap;

import com.vmturbo.api.component.security.HeaderMapper;
import com.vmturbo.api.component.security.IntersightHeaderMapper;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PredefinedRole;

/**
 * Factory to create vendor specific header. Currently only support Cisco Intersight.
 */
public class HeaderMapperFactory {

    private static final Map<ExternalVendor, MapperGenerator<String, HeaderMapper>>
            EXTERNAL_VENDOR_TO_INTERNAL_USER_MAPPER =
            ImmutableMap.of(ExternalVendor.CISCO_INTERSIGHT,
                    (roleMap, externalUser, externalRole, jwtTokenPublicKeyTag, jwtToken) -> getCiscoIntersightHeaderMapper(
                            roleMap, externalUser, externalRole, jwtTokenPublicKeyTag, jwtToken));

    private HeaderMapperFactory() {}

    /**
     * Factory method to get {@link HeaderMapper}. Default to Cisco InterSight.
     *
     * @param externalVendor       external vendor to integrate with.
     * @param roleMap              XL role to external vendor role map.
     * @param externalUser         external user name tag from header to get user name.
     * @param externalRole         external user role tag from header to get uer role.
     * @param jwtTokenPublicKeyTag the vender's JWT token public key property name.
     * @param jwtTokenTag          the vender's JWT token property name.
     * @return {@link HeaderMapper} for the specific vendor.  Default to Cisco InterSight.
     */
    public static HeaderMapper getHeaderMapper(@Nonnull Optional<ExternalVendor> externalVendor,
            @Nonnull Map<PredefinedRole, String> roleMap, @Nonnull String externalUser,
            @Nonnull String externalRole, @Nonnull String jwtTokenPublicKeyTag,
            @Nonnull String jwtTokenTag) {

        return externalVendor.map(vendor -> EXTERNAL_VENDOR_TO_INTERNAL_USER_MAPPER.get(vendor)
                .generate(roleMap, externalUser, externalRole, jwtTokenPublicKeyTag, jwtTokenTag))
                .orElse(getCiscoIntersightHeaderMapper(roleMap, externalUser, externalRole,
                        jwtTokenPublicKeyTag, jwtTokenTag));
    }

    private static HeaderMapper getCiscoIntersightHeaderMapper(
            Map<PredefinedRole, String> externalRoleSet, @Nonnull String externalUser,
            @Nonnull String externalRole, @Nonnull String jwtTokenPublicKeyTag,
            @Nonnull String jwtToken) {
        final Builder builder = ImmutableMap.<String, String>builder();
        for (String role : PREDEFINED_ROLE_SET) {
            final StringTokenizer tokenizer = new StringTokenizer(
                    externalRoleSet.getOrDefault(PredefinedRole.valueOf(role), ""), ",");
            while (tokenizer.hasMoreTokens()) {
                builder.put(tokenizer.nextToken().trim(), role);
            }
        }
        return new IntersightHeaderMapper(new CaseInsensitiveMap(builder.build()), externalUser, externalRole,
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
        T generate(Map<PredefinedRole, R> roleRMap, R externalUser, R externalRole,
                R jwtTokenPublicKeyTag, R jwtToken);
    }
}