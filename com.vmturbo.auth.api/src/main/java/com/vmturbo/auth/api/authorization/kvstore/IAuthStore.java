package com.vmturbo.auth.api.authorization.kvstore;

import java.util.Optional;

/**
 * The IApiAuthStore represents the local Auth store.
 */
public interface IAuthStore {
    /**
     * The KV store key.
     */
    static final String KV_KEY = "public_key/key";

    /**
     * Retrieves the auth component's public key
     *
     * @return The public key.
     */
    String retrievePublicKey();

    /**
     * Retrieve other component's public key based on namespace, e.g. api-1
     *
     * @param namespace
     * @return
     */
    Optional<String> retrievePublicKey(String namespace);
}
