package com.vmturbo.auth.component.licensing.store;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.encoders.Base64;
import org.springframework.security.access.prepost.PreAuthorize;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * The consul-backed license store that holds the encrypted license information.
 *
 * The licenses will be stored, encrypted, under the /licenses key, with each sub-keyed using the
 * license's "license key" field. e.g. if a license has key "abcxyz", it will be stored at
 * /licensing/licenses/abcxyz.
 */
@ThreadSafe
public class LicenseKVStore implements ILicenseStore {

    /**
     * The KV license key path. Each license will be stored in a subkey w/the UUID as the name.
     */
    @VisibleForTesting
    static final String LICENSES_KEY = "licensing/licenses";

    static final String SEPARATOR = "/";

    /**
     * The charset for the encoded strings.
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    private static final Logger logger_ = LogManager.getLogger();

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
     * Generate the key-value storage key from the license id. This will append the license key
     * to the license address prefix in the key-value storage area. e.g.
     *
     * license id "111-222" ==> kv key "licensing/licenses/111-222"
     *
     * @param licenseId the license id to generate a KV key for.
     * @return The resulting kv storage key.
     */
    private String createKVKeyFromLicenseId(String licenseId) {
        return LICENSES_KEY + SEPARATOR + licenseId;
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
     * Given a license uuid, find the matching key, fetch the value there, and decode/decrypt the
     * license contained inside.
     *
     * @param uuid the license uuid to search for
     * @return the {@link LicenseDTO} contained in the key, if there is one there. Null, otherwise.
     * @throws IOException if there is any problem decoding/decrypting the license.
     */
    @Override
    public LicenseDTO getLicense(final String uuid) throws IOException {
        String encryptedLicense = getKVValue(createKVKeyFromLicenseId(uuid)).orElse(null);
        if (null == encryptedLicense) {
            return null;
        }
        return decodeAndDecryptLicense(encryptedLicense);
    }

    /**
     * Given a base64-encoded string that is probably hiding a protobuf license object, decode and
     * decrypt the bad boy (or girl).
     *
     * @param encryptedLicense the base-64-encoded, encrypted protobuf license string
     * @return The base-64-decoded, decrypted protobuf license object.
     */
    private LicenseDTO decodeAndDecryptLicense(String encryptedLicense) throws IOException {
        // decode the license string
        byte[] cipherData = Base64.decode(encryptedLicense.getBytes(CHARSET_CRYPTO));
        // decrypt it
        byte[] licenseData = CryptoFacility.decrypt(cipherData);
        // reform is as a protobuf License
        LicenseDTO licenseDTO = LicenseDTO.parseFrom(licenseData);
        return licenseDTO;
    }

    /**
     * Given a LicenseDTO object, convert it to binary, encrypt it, and convert it to a base-64
     * encoded string.
     *
     * @param license The licenseDTO to encode and encrypt.
     * @return An encrypted string containing the license protobuf.
     */
    private String encryptAndEncodeLicense(LicenseDTO license) {
        byte[] encryptedLicense = CryptoFacility.encrypt(license.toByteArray());
        try {
            String encodedEncryptedLicense = new String(Base64.encode(encryptedLicense), CHARSET_CRYPTO);
            return encodedEncryptedLicense;
        } catch (UnsupportedEncodingException uee) {
            // This should never happen. But when it does, it prefers Dos Equis.
            logger_.error(uee);
        }
        return null;
    }

    /**
     * Retrieves all {@link LicenseDTO} in storage.
     *
     * @return A collection of the licenses currently in storage.
     */
    @Nonnull
    @Override
    public Collection<LicenseDTO> getLicenses() throws IOException {
        // get all of the licenses in the KV store
        Map<String,String> encryptedLicenses = keyValueStore_.getByPrefix(LICENSES_KEY);
        try {
            List<LicenseDTO> decryptedLicenses = encryptedLicenses.values().stream()
                    .map(encryptedData -> {
                        try {
                            return decodeAndDecryptLicense(encryptedData);
                        } catch (IOException ioe) {
                            // we can get this exception from either a base64 decoding error or a
                            // protobuf deserialization error. Both are bad news for the license in
                            // storage, so we will error out of this function.
                            logger_.error(ioe);
                            throw new RuntimeException(ioe);
                        }
                    })
                    .collect(Collectors.toList());
            return decryptedLicenses;
        } catch (RuntimeException rte) {
            // Since we had to wrap our IOExceptions as RTE's in the lambda above, give ourselves
            // to rethrow them properly here.
            if (rte.getCause() instanceof IOException) {
                // rethrow the ioe exception
                throw (IOException) rte.getCause();
            }
            // else rethrow the runtime exception
            throw rte;
        }
    }

    /**
     * Add or update a license in the key-value store. The license will be encrypted and
     * base-64-encoded for storage in the key-value store.
     *
     * @param licenseDTO the protobuf license to store.
     */
    @Override
    public void storeLicense(LicenseDTO licenseDTO) {
        String encryptedLicense = encryptAndEncodeLicense(licenseDTO);
        putKVValue(createKVKeyFromLicenseId(licenseDTO.getUuid()), encryptedLicense);
    }

    /**
     * Remove the license with the specified uuid from the key-value storage system.
     *
     * @param uuid the uuid of the license to remove.
     */
    @Override
    public boolean removeLicense(final String uuid) {
        String key = createKVKeyFromLicenseId(uuid);
        Optional<String> existingLicense = getKVValue(key);
        if (existingLicense.isPresent()) {
            synchronized (storeLock_) {
                keyValueStore_.removeKeysWithPrefix(key);
            }
            return true;
        }
        return false;
    }

}
