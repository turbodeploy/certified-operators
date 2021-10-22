package com.vmturbo.mediation.webhook.oauth;

import javax.annotation.Nonnull;

/**
 * Enum class to represent grant types to be used for oAuth request.
 */
public enum GrantType {
    /**
     * The client credentials grant type. Requires clientID/clientSecret.
     */
    CLIENT_CREDENTIALS("client_credentials"),
    /**
     * the refresh token grant type. Requires refreshToken.
     */
    REFRESH_TOKEN("refresh_token");

    private final String value;

    GrantType(String value) {
        this.value = value;
    }

    @Nonnull
    public String getValue() {
        return value;
    }
}
