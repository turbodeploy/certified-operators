package com.vmturbo.common.api.crypto;

import javax.annotation.Nonnull;

/**
 * A provider for encryption keys, used to encrypt and decrypt sensitive data.
 *
 * <p>A master encryption key will be read from an external source, and used to encrypt/decrypt
 * the internal keys. Individual components each get their own internal key (when needed)</p>
 */
public interface IEncryptionKeyProvider {

    /**
     * Gets the internal, component-specific base64-encoded encryption key.
     *
     * <p>This is used to encrypt and decrypt sensitive data.</p>
     *
     * @return the internal, component-specific base64-encoded encryption key.
     */
    @Nonnull
    String getEncryptionKey();
}
