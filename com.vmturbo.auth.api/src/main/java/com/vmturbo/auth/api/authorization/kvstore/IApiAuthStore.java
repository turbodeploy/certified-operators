package com.vmturbo.auth.api.authorization.kvstore;

import javax.annotation.Nonnull;

/**
 * The IApiAuthStore represents the local Auth store.
 */
public interface IApiAuthStore {
    /**
     * The KV store key.
     */
    static final String KV_KEY = "public_key/key";

    /**
     * Retrieves the public key.
     *
     * @return The public key.
     */
    String retrievePublicKey();
}
