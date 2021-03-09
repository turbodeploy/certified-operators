package com.vmturbo.auth.api.authorization.keyprovider;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.io.BaseEncoding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.components.crypto.IEncryptionKeyProvider;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * A provider for encryption keys, used to encrypt and decrypt sensitive data.
 *
 * <p>A master encryption key will be read from an external source, and used to encrypt/decrypt
 * the internal keys. Individual components each get their own internal key (when needed)</p>
 */
public class EncryptionKeyProvider implements IEncryptionKeyProvider, IKeyImportIndicator {

    /**
     * The key to use in the KV store to hold the value for the encryption key.
     */
    private static final String ENCRYPTION_KEY_KV_KEY = "EC256";

    /**
     * The length of the encryption key: 32 bytes, or 256 bits.
     */
    private static final int ENCRYPTION_KEY_LENGTH_IN_BYTES = 32;

    /**
     * The logger.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * Key value store, to store the internal, per-component encryption keys.
     */
    private final KeyValueStore keyValueStore;

    /**
     * For reading the primary and fallback master keys from an external source.
     */
    private final MasterKeyReader masterKeyReader;

    /**
     * A cached copy of the internal, component-specific base64-encoded encryption key.
     *
     * <p>Used to encrypt/decrypt sensitive data.</p>
     */
    private String encryptionKey;

    /**
     * A flag indicating whether the encryption key was imported during the lifetime of this class instance.
     */
    private boolean encryptionKeyImported = false;

    /**
     * Create a provider for encryption keys, used to encrypt and decrypt sensitive data.
     *
     * @param keyValueStore to store the internal, per-component encryption keys.
     * @param masterKeyReader for reading the master key(s) from an external source.
     */
    public EncryptionKeyProvider(@Nonnull final KeyValueStore keyValueStore,
                                 @Nonnull final MasterKeyReader masterKeyReader) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.masterKeyReader = Objects.requireNonNull(masterKeyReader);
    }

    /**
     * Gets the internal, component-specific base64-encoded encryption key.
     *
     * <p>This is used to encrypt and decrypt sensitive data.</p>
     *
     * @return the internal, component-specific base64-encoded encryption key.
     */
    public synchronized @Nonnull String getEncryptionKey() {
        if (encryptionKey != null) {
            return encryptionKey;
        }

        // Get the master key, provided from an external source (like K8s secret or Vault)
        final String primaryMasterKey = masterKeyReader.getPrimaryMasterKey()
            .orElseThrow(() -> new SecurityException("No master encryption key was provided from the external source!"));

        // Check if the encryption key is stored internally
        if (keyValueStore.containsKey(ENCRYPTION_KEY_KV_KEY)) {
            final String cipherText = keyValueStore.get(ENCRYPTION_KEY_KV_KEY)
                .orElseThrow(() -> new SecurityException("The encryption key could not be retrieved!"));
            final byte[] cipherData = BaseEncoding.base64().decode(cipherText);
            encryptionKey = decryptUsingKey(primaryMasterKey, cipherData)
            .orElseGet(() -> {
                final String fallbackKey = masterKeyReader.getFallbackMasterKey()
                    .orElseThrow(() -> new SecurityException("Encryption key could not be decrypted "
                    + "using the provided master key, and no fallback key was provided."));
                String key = decryptUsingKey(fallbackKey, cipherData)
                    .orElseThrow(() -> new SecurityException("Encryption key cannot be decrypted with any "
                        + "of the available primary or fallback master keys."));
                // Decrypted using the fallback key. We need to re-encrypt with the new primary master key.
                // This is a normal part of master key rotation.
                byte[] encryptionKeyBytes = BaseEncoding.base64().decode(key);
                encryptAndStore(primaryMasterKey, encryptionKeyBytes);
                logger.info("Successfully re-encrypted the encryption key using new primary master key.");
                return key;
            });
            logger.info("Successfully decrypted the encryption key.");
            return encryptionKey;
        }

        // The encryption key is not yet stored internally
        // This may be an upgrade from an older version; check the legacy key location
        final byte[] encryptionKeyBytes;
        Optional<byte[]> legacyKey = CryptoFacility.getEncryptionKeyForVMTurboInstance();
        if (legacyKey.isPresent()) {
            // Upgrade a legacy key into the new encrypted storage format
            logger.info("Successfully imported existing encryption key.");
            encryptionKeyBytes = legacyKey.get();
            // If we get here, we also need to set the admin init JWT, so the instance doesn't appear to be new
            // Set a flag so we can key on this later
            encryptionKeyImported = true;
        } else {
            // We don't have the internal encryption key stored, and no legacy key was found. This appears to be a new
            // installation, so generate a new encryption key.
            encryptionKeyBytes = CryptoFacility.getRandomBytes(ENCRYPTION_KEY_LENGTH_IN_BYTES);
            logger.info("Generated new encryption key.");
        }
        // Encrypt the internal encryption key before storing it, using the master encryption key
        encryptAndStore(primaryMasterKey, encryptionKeyBytes);
        logger.info("Persisted encryption key.");
        // Cache the encryption key for future use
        encryptionKey = BaseEncoding.base64().encode(encryptionKeyBytes);
        return encryptionKey;
    }

    /**
     * Return a flag indicating whether the encryption key was imported during the lifetime of this class instance.
     *
     * @return a flag indicating whether the encryption key was imported during the lifetime of this class instance.
     */
    @Override
    public boolean wasEncryptionKeyImported() {
        return encryptionKeyImported;
    }

    /**
     * Encrypt the component-specific encryption key using the master key, and store it.
     *
     * @param masterKey the master key to use for encrypting
     * @param encryptionKeyBytes a byte array representing the encryption key to store
     */
    private void encryptAndStore(final String masterKey, final byte[] encryptionKeyBytes) {
        byte[] encryptedEncryptionKey = CryptoFacility.encrypt(masterKey, encryptionKeyBytes);
        String encodedEncryptedEncryptionKey = BaseEncoding.base64().encode(encryptedEncryptionKey);
        keyValueStore.put(ENCRYPTION_KEY_KV_KEY, encodedEncryptedEncryptionKey);
    }

    /**
     * A convenience method for trying to decrypt some text with a provided encryption key.
     *
     * <p>Since keys may be rotated, we can't be sure that a given key will decrypt the secret.
     * Therefore, we return an Optional.EMPTY, rather than throwing an exception.</p>
     *
     * <p>Currently, this class is using this method to decrypt the internal, per-component
     * encryption key using either the primary or fallback externally-provided master key.</p>
     *
     * @param masterKey the (primary or fallback) master key to use to decrypt the provided data
     * @param cipherData the data to decrypt
     * @return an Optional containing the base64-encoded decrypted string if decryption succeeded
     */
    private Optional<String> decryptUsingKey(String masterKey, byte[] cipherData) {
        try {
            return Optional.of(CryptoFacility.decrypt(null, masterKey, cipherData))
                .map(BaseEncoding.base64()::encode);
        } catch (SecurityException e) {
            // The supplied key was unable to decrypt the text.
            return Optional.empty();
        }
    }

}
