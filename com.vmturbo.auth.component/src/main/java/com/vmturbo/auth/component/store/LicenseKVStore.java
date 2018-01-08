package com.vmturbo.auth.component.store;

import java.io.UnsupportedEncodingException;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.access.prepost.PreAuthorize;

import com.google.common.annotations.VisibleForTesting;

import io.jsonwebtoken.impl.Base64Codec;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * The consul-backed license store that holds the encrypted license information.
 */
@ThreadSafe
public class LicenseKVStore implements ILicenseStore {

    /**
     * The KV license key.
     */
    @VisibleForTesting
    static final String LICENSE_KEY = "license/key";

    /**
     * The charset for the passwords
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * The logger
     */
    private final Logger logger_ = LogManager.getLogger();

    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    private final @Nonnull
    KeyValueStore keyValueStore_;
    /**
     * Locks for write operations on target storages.
     */
    private final Object storeLock_ = new Object();

    /**
     * Constructs the KV store.
     *
     * @param keyValueStore The underlying store backend.
     */
    public LicenseKVStore(@Nonnull final KeyValueStore keyValueStore) {
        keyValueStore_ = Objects.requireNonNull(keyValueStore);
    }

    /**
     * Retrieves the value for the key from the KV store.
     *
     * @param key The key.
     * @return The Optional value.
     */
    private Optional<String> getKVValue(final @Nonnull String key) {
        synchronized (storeLock_) {
            return keyValueStore_.get(key);
        }
    }

    /**
     * Puts the value for the key into the KV store.
     *
     * @param key   The key.
     * @param value The value.
     */
    private void putKVValue(final @Nonnull String key, final @Nonnull String value) {
        synchronized (storeLock_) {
            keyValueStore_.put(key, value);
        }
    }

    /**
     * Retrieves the license.
     * The information may be retrieved by anyone.
     *
     * @return The license if it exists.
     */
    @Nonnull
    @Override
    public Optional<String> getLicense() {
        return getKVValue(LICENSE_KEY).map(value -> {
                    Base64Codec codec = new Base64Codec();
                    byte[] bytes = codec.decode(value);
                    String licenseCipherText;
                    try {
                        licenseCipherText = new String(bytes, CHARSET_CRYPTO);
                        return CryptoFacility.decrypt(licenseCipherText);
                    } catch (UnsupportedEncodingException e) {
                        throw new SecurityException(e);
                    }
                }
        );
    }

    /**
     * Store the encrypted license to consul-backed store.
     *
     * @param license The license in plain text.
     * @throws AuthorizationException In case of user doesn't have Administrator role.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    @Override
    public void populateLicense(@Nonnull final String license) throws AuthorizationException {
        Base64Codec codec = new Base64Codec();
        try {
            String encryptedLicense = codec.encode(CryptoFacility.encrypt(license).getBytes(CHARSET_CRYPTO));
            putKVValue(LICENSE_KEY, encryptedLicense);
        } catch (UnsupportedEncodingException e) {
            throw new SecurityException(e);
        }
    }
}
