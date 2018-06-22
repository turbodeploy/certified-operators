package com.vmturbo.kvstore;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Store to store and retrieve public key.
 * Currently it's back by console with structure:
 * Key
 * public_key/api-1
 *           /auth-1
 *           /history-1
 */
public interface IPublicKeyStore {
    /**
     * Retrieve current component namespace, e.g: api-1
     * @return current component namespace
     */
    String getNamespace();

    /**
     * Store component public key.
     * @param value public key
     */
    void putPublicKey(@Nonnull final String value);

    /**
     * Retrieve component public key
     * @param namespace component namespace
     * @return component public key based on namespace
     */
    Optional<String> getPublicKey(@Nonnull final String namespace);
}
