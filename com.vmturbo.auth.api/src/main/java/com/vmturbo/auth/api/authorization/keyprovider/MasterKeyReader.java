package com.vmturbo.auth.api.authorization.keyprovider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import com.google.common.io.BaseEncoding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for reading the master encryption key from an external source.
 *
 * <p>The external source would be a Kubernetes secret or from Vault. The current implementation
 * assumes that the secret is injected as a file.</p>
 */
public class MasterKeyReader {

    private static final String PRIMARY_MASTER_SECRET_FILE_PATH = "/home/turbonomic/data/helper_dir/vmt_helper_data_256.out";

    private static final String FALLBACK_MASTER_SECRET_FILE_PATH = "/home/turbonomic/data/helper_dir/fallback_vmt_helper_data_256.out";

    private static final int EXPECTED_BYTE_LENGTH = 32;

    /**
     * The logger.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * A base64-encoded cache of the master key, as read from an external source.
     */
    private String primaryKey;

    /**
     * A base64-encoded cache of the fallback key, as read from an external source.
     */
    private String fallbackKey;

    /**
     * A default constructor.
     */
    public MasterKeyReader() {

    }

    /**
     * Get the primary master key, used to encrypt/decrypt the internal, per-component encryption keys.
     *
     * @return the primary master key
     */
    public synchronized Optional<String> getPrimaryMasterKey() {
        if (primaryKey != null) {
            return Optional.of(primaryKey);
        }

        return readSecretKey(PRIMARY_MASTER_SECRET_FILE_PATH);
    }

    /**
     * Get the fallback master key, used to decrypt already-stored secrets after a master key rotation.
     *
     * @return the fallback master key
     */
    public synchronized  Optional<String> getFallbackMasterKey() {
        if (fallbackKey != null) {
            return Optional.of(fallbackKey);
        }

        return readSecretKey(FALLBACK_MASTER_SECRET_FILE_PATH);
    }

    private Optional<String> readSecretKey(String path) {
        // TODO: (OM-66172) We still need to integrate this with IWO/vault
        //     : Consider creating a default master key (used when no externally-provided key)
        //     : The current implementation is aligned to our current Kubernetes secrets, but that
        //     : can be easily changed anytime between now and when we start using this in production.
        //     : For example, we could read from environment variables instead of a file if that were
        //     : better for Vault & IWO.
        try {
            Path encryptionFile = Paths.get(path);
            if (Files.exists(encryptionFile)) {
                byte[] rawEncryptionKey = Files.readAllBytes(encryptionFile);
                if (EXPECTED_BYTE_LENGTH != rawEncryptionKey.length) {
                    logger.error("Expected an encryption key length of " + EXPECTED_BYTE_LENGTH
                        + ", but actual length was " + rawEncryptionKey.length
                        + ". Key will not be used.");
                    return Optional.empty();
                }
                // Found the secret key
                return Optional.of(BaseEncoding.base64().encode(rawEncryptionKey));
            }
            // Secret file doesn't exist
            return Optional.empty();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return Optional.empty();
        }
    }

}
