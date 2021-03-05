package com.vmturbo.auth.api.authorization.keyprovider;

/**
 * Indicates whether the encryption key was imported during the lifetime of this instance.
 */
public interface IKeyImportIndicator {

    /**
     * Return a flag indicating whether the encryption key was imported during the lifetime of this class instance.
     *
     * @return a flag indicating whether the encryption key was imported during the lifetime of this class instance.
     */
    boolean wasEncryptionKeyImported();
}
