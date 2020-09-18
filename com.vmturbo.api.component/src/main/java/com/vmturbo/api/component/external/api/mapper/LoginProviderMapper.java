package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;

public class LoginProviderMapper {
    private LoginProviderMapper() {
    }

    @Nonnull
    public static String toApi(@Nonnull final PROVIDER provider) {
        switch (provider) {
            case LOCAL:
                return "Local";
            case LDAP:
                return "LDAP";
            default:
                throw new IllegalStateException("Provider " + provider.name() +
                                                " has no mapping to an API-compatible string.");
        }
    }

    @Nonnull
    public static PROVIDER fromApi(@Nonnull final String apiProviderStr) {
        try {
            return PROVIDER.valueOf(apiProviderStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Provider " + apiProviderStr +
                                            " has no mapping to the provider enums. ");
        }
    }
}
