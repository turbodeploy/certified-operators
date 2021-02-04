package com.vmturbo.auth.api.authorization.keyprovider;

import java.security.PrivateKey;
import java.security.PublicKey;

/**
 * An interface for providing (generating, storing and retrieving) private/public key pairs.
 */
public interface KeyProvider {

    /**
     * Retrieves the private key, generating it if needed.
     *
     * @return The private key.
     */
    PrivateKey getPrivateKey();

    /**
     * Retrieves the public key, generating it if needed.
     *
     * @return The public key.
     */
    PublicKey getPublicKey();
}
