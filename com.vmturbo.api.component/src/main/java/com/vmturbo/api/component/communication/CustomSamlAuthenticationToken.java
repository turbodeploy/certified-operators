package com.vmturbo.api.component.communication;

import javax.annotation.Nonnull;

import org.springframework.security.authentication.AbstractAuthenticationToken;

/**
 * Custom Spring authentication/authorization token to carry request IP.
 */
public class CustomSamlAuthenticationToken extends AbstractAuthenticationToken {

    private static final long serialVersionUID = -1949976849306453199L;
    private final String remoteIpAddress;

    /**
     * Custom Spring authentication/authorization token to carry request IP.
     *
     * @param remoteIpAddress request IP adddress
     */
    public CustomSamlAuthenticationToken(@Nonnull final String remoteIpAddress) {
        // not passing in any permission
        super(null);
        this.remoteIpAddress = remoteIpAddress;
    }

    /**
     * Get request IP.
     *
     * @return client reqeust IP.
     */
    public String getRemoteIpAddress() {
        return remoteIpAddress;
    }

    @Override
    public Object getCredentials() {
        return "";
    }

    @Override
    public Object getPrincipal() {
        return "";
    }
}