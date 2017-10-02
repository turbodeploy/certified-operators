package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;

public class LoginProviderMapper {
    /**
     * Map to convert the UI/API's expected strings for login provider types to the
     * {@link PROVIDER} enum. Putting the map here in the API component instead of the Auth library
     * to isolate the auth library from additional API influence - internal components shouldn't
     * have to worry about UI strings.
     */
    private static final ImmutableBiMap<String, PROVIDER> PROVIDER_TYPE_MAP =
            ImmutableBiMap.of("Local", PROVIDER.LOCAL,"LDAP", PROVIDER.LDAP);

    private LoginProviderMapper() {}

    @Nonnull
    public static String toApi(@Nonnull final PROVIDER provider) {
        final String apiStr = PROVIDER_TYPE_MAP.inverse().get(provider);
        if (apiStr == null) {
            throw new IllegalStateException(
                    "Provider " + provider + " has no mapping to an API-compatible string." +
                            "The existing mappings are: " + PROVIDER_TYPE_MAP.inverse());
        }
        return apiStr;
    }

    @Nonnull
    public static PROVIDER fromApi(@Nonnull final String apiProviderStr) {
        final PROVIDER provider = PROVIDER_TYPE_MAP.get(apiProviderStr);
        if (provider == null) {
            throw new IllegalStateException(
                    "Provider " + apiProviderStr + " has no mapping to the provider enums. " +
                            "The existing mappings are: " + PROVIDER_TYPE_MAP);
        }
        return provider;
    }


}
