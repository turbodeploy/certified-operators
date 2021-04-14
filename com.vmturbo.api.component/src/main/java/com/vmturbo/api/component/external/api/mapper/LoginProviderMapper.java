package com.vmturbo.api.component.external.api.mapper;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;

public class LoginProviderMapper {

    private static final String LOCAL_STR = "Local";

    private LoginProviderMapper() {
    }

    @Nonnull
    public static String toApi(@Nullable final PROVIDER provider) {
        if (Objects.equals(provider, PROVIDER.LDAP)) {
            return "LDAP";
        } else {
            return LOCAL_STR;
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
