package com.vmturbo.mediation.webhook.oauth;

/**
 * Enum class to represent grant types to be used for oAuth request.
 */
public enum GrantType {
    /**
     * the client credentials grant type.
     */
    CLIENT_CREDENTIALS("client_credentials");

    final String value;

    GrantType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
