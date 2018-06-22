package com.vmturbo.auth.api.authorization.kvstore;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;

/**
 * XL component JWT token store to generate component based JWT token.
 */
public interface IComponentJwtStore {

    /**
     * Generate component based JWT token.
     * @return component JWT token signed by component private key
     */
    @Nonnull JWTAuthorizationToken generateToken();

    /**
     * Get current component namespace, e.g. api-1
     * @return component namespace
     */
    @Nonnull String getNamespace();
}
