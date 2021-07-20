package com.vmturbo.common.api.crypto;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.api.utils.EnvironmentUtils;

/**
 * The CryptoFacility is a utility class that provides encryption and secure hash services.
 */
public class CryptoFacility {

    /**
     * If true, use Kubernetes secrets to read in a master encryption key, used to encrypt the
     * internal, per-component encryption keys and store them encrypted (currently in Consul).
     * If false, this data will be read from (legacy) persistent volumes.
     *
     * <p>Note: This feature flag is exposed in a static way to avoid having to refactor the
     * many static methods that already exist in this class. This is expected to be a short-lived
     * situation, until enabling external secrets becomes the default.</p>
     */
    public static boolean enableExternalSecrets = false;

    /**
     * When set (i.e. not null), used to provide the encryption key needed to encrypt/decrypt sensitive
     * data. When null, legacy logic will be used to read the encryption key from a file.
     */
    public static IEncryptionKeyProvider encryptionKeyProvider;

    /**
     * Default encryption key length is version 2 which is 256 bits.
     */
    static final Integer DEFAULT_KEY_LENGTH_VERSION = 2;

    /**
     * The key location property.
     */
    private static final String VMT_ENCRYPTION_KEY_DIR_PARAM = "com.vmturbo.keydir";

    /**
     * The default encryption key location.
     */
    private static final String VMT_ENCRYPTION_KEY_DIR = "/home/turbonomic/data/helper_dir";

    /**
     * The keystore data file name, for 128 bits key-size.
     */
    static final String VMT_ENCRYPTION_KEY_FILE = "vmt_helper_data.out";

    /**
     * The keystore data file name, for 256 bits key-size.
     */
    static final String VMT_ENCRYPTION_KEY_FILE_256 = "vmt_helper_data_256.out";

    /**
     * The charset for the passwords.
     */
    @VisibleForTesting
    static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * The authenticated cipher algorithm.
     */
    private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";

    /**
     * The GCM tag length in bits.
     */
    private static final int GCM_TAG_LENGTH = 128;

    /**
     * The cipher key specification algorithm.
     */
    private static final String KEYSPEC_ALGORITHM = "AES";

    /**
     * The key derivation algorithm.
     */
    private static final String PBKDF2_DERIVATION_ALGORITHM = "PBKDF2WithHmacSHA256";

    /**
     * The salt length.
     */
    public static final int PKCS5_SALT_LENGTH = 0x20;

    /**
     * The PBKDF2 iteration count.
     */
    private static final int PBKDF2_ITERATIONS = 32768;

    /**
     * Encryption version -> key size maps.
     * version 1 -> 128 key size
     * version 2 -> 256 key size
     */
    @VisibleForTesting
    static final Map<Integer, AesParameter> AES_VERSION_KEY_CONFIG_MAP =
            ImmutableMap.<Integer, AesParameter>builder()
                    .put(1, new AesParameter(128, VMT_ENCRYPTION_KEY_FILE))
                    .put(2, new AesParameter(256, VMT_ENCRYPTION_KEY_FILE_256))
                    .build();

    /**
     * The logger.
     */
    private static final Logger logger = LogManager.getLogger(CryptoFacility.class);

    /**
     * The secure random.
     */
    private static final SecureRandom random = new SecureRandom();

    private static final Map<Integer, byte[]> encryptionKeyMap = new ConcurrentHashMap<>();

    /**
     * Disable instantiation of the class.
     */
    private CryptoFacility() {
    }

    /**
     * Decrypts the given string using AES algorithm with authenticated block cipher method.
     *
     * @param ciphertext The string to decrypt.
     * @return The decrypted string.
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static @Nonnull String decrypt(final @Nonnull String ciphertext)
            throws SecurityException {
        return decrypt(null, ciphertext);
    }

    /**
     * Decrypts the given byte array using AES algorithm with authenticated block cipher method.
     * @param cipherBytes The bytes to decrypt
     * @return The decrypted bytes
     * @throws SecurityException if there is a decryption error
     */
    public static @Nonnull byte[] decrypt(final @Nonnull byte[] cipherBytes)
            throws SecurityException {
        return decrypt(null, cipherBytes);
    }

    /**
     * Decrypts the given string using AES algorithm with authenticated block cipher method.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param ciphertext    The string to decrypt.
     * @return The decrypted string.
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static @Nonnull String decrypt(final @Nullable String keySplitValue,
                                          final @Nonnull String ciphertext)
            throws SecurityException {
        return decrypt(keySplitValue, null, ciphertext);
    }

    /**
     * Decrypts the given string using AES algorithm with authenticated block cipher method.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param encryptionKey The base64-encoded encryption key to use. If null, a default encryption
     *                      key will be used. This should only be provided when the master key is
     *                      being used to decrypt the per-component encryption key. This is necessary
     *                      because the per-component encryption key cannot be used to decrypt itself.
     *                      Any other usage of this parameter is discouraged.
     * @param ciphertext    The string to decrypt.
     * @return The decrypted string.
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static @Nonnull String decrypt(final @Nullable String keySplitValue,
                                          final @Nullable String encryptionKey,
                                          final @Nonnull String ciphertext)
        throws SecurityException {
        // Be a little defensive here.
        if (ciphertext == null) {
            throw new SecurityException("Null ciphertext.");
        }
        try {
            final byte[] cipherData = BaseEncoding.base64().decode(ciphertext);
            return new String(decrypt(keySplitValue, encryptionKey, cipherData), CHARSET_CRYPTO);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to decode.", e);
        }
    }

    /**
     * Decrypts the given string using AES algorithm with authenticated block cipher method.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param cipherdata    The ciphered bytes to decrypt.
     * @return The decrypted bytes.
     * @throws SecurityException In the case of any error decrypting the cipher data.
     */
    public static @Nonnull byte[] decrypt(final @Nullable String keySplitValue,
                                          final @Nonnull byte[] cipherdata)
        throws SecurityException {
        return decrypt(keySplitValue, null, cipherdata);
    }

    /**
     * Decrypts the given string using AES algorithm with authenticated block cipher method.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param encryptionKey The base64-encoded encryption key to use. If null, a default encryption
     *                      key will be used. This should only be provided when the master key is
     *                      being used to decrypt the per-component encryption key. This is necessary
     *                      because the per-component encryption key cannot be used to decrypt itself.
     *                      Any other usage of this parameter is discouraged.
     * @param cipherdata    The ciphered bytes to decrypt.
     * @return The decrypted bytes.
     * @throws SecurityException In the case of any error decrypting the cipher data.
     */
    public static @Nonnull byte[] decrypt(final @Nullable String keySplitValue,
                                          final @Nullable String encryptionKey,
                                          final @Nonnull byte[] cipherdata)
            throws SecurityException {
        // Be a little defensive here.
        if (cipherdata == null) {
            throw new SecurityException("Null cipher data.");
        }
        try {
            ByteBuffer buff = ByteBuffer.wrap(cipherdata);
            int version = buff.getInt();
            if (!AES_VERSION_KEY_CONFIG_MAP.containsKey(version)) {
                throw new IllegalArgumentException("The version is unsupported");
            }

            int length = buff.getInt();
            if (length != PKCS5_SALT_LENGTH) {
                throw new SecurityException("Corrupted cipher data.");
            }
            byte[] salt = new byte[length];
            buff.get(salt);

            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            length = buff.getInt();
            if (length != cipher.getBlockSize()) {
                throw new SecurityException("Corrupted cipher data.");
            }
            byte[] nonce = new byte[length];
            buff.get(nonce);

            length = buff.getInt();
            byte[] cipherBytes = new byte[length];
            buff.get(cipherBytes);

            SecretKey key = getDerivedKey(keySplitValue, salt, version, encryptionKey);
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
            return cipher.doFinal(cipherBytes);
        } catch (Exception e) {
            throw new SecurityException("Unable to decrypt.", e);
        }
    }

    /**
     * Returns the random byte sequence of {@code length} bytes.
     *
     * @param length The byte sequence length.
     * @return The random byte sequence.
     */
    public static byte[] getRandomBytes(int length) {
        byte[] data = new byte[length];
        random.nextBytes(data);
        return data;
    }

    /**
     * Generates the key derived from the site secret.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param salt          The salt.
     * @param version       Encryption key length version
     * @param encryptionKey The base64-encoded encryption key to use. If null, a default encryption
     *                      key will be used. This should only be provided when the master key is
     *                      being used to encrypt or decrypt the per-component encryption key. This
     *                      is necessary because the per-component encryption key cannot be used to
     *                      encrypt or decrypt itself. Any other usage of this parameter is discouraged.
     * @return The derived site secret.
     */
    private static SecretKey getDerivedKey(final String keySplitValue,
            final @Nonnull byte[] salt,
            final int version,
            final @Nullable String encryptionKey) {
        try {
            final byte[] encryptionKeyBytes;
            final String siteSecret;
            if (encryptionKey == null) {
                // Retrieve the default site secret, since a specific encryption key was not passed in
                encryptionKeyBytes = getEncryptionKeyForVMTurboInstance(version);
                siteSecret = BaseEncoding.base64().encode(encryptionKeyBytes);
            } else {
                // Use the encryption key that was passed in
                encryptionKeyBytes = BaseEncoding.base64().decode(encryptionKey);
                siteSecret = encryptionKey;
            }

            // In case there is no seed, use the direct key.
            if (keySplitValue == null) {
                return new SecretKeySpec(encryptionKeyBytes, KEYSPEC_ALGORITHM);
            }

            // Convert the site secret to string using Base64.
            byte[] seedData = keySplitValue.getBytes(CHARSET_CRYPTO);
            byte[] finalSalt = new byte[seedData.length + salt.length];
            System.arraycopy(seedData, 0, finalSalt, 0, seedData.length);
            System.arraycopy(salt, 0, finalSalt, seedData.length, salt.length);
            KeySpec specs = new PBEKeySpec(siteSecret.toCharArray(),
                finalSalt, PBKDF2_ITERATIONS,
                AES_VERSION_KEY_CONFIG_MAP.get(version).keyLength);
            SecretKeyFactory kf = SecretKeyFactory.getInstance(PBKDF2_DERIVATION_ALGORITHM);
            return new SecretKeySpec(kf.generateSecret(specs).getEncoded(), KEYSPEC_ALGORITHM);
        } catch (NoSuchAlgorithmException
            | InvalidKeySpecException
            | UnsupportedEncodingException e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Encrypts the given string using authenticated block cipher method.
     *
     * @param plaintext - The string to encrypt
     * @return The encrypted string or null if an error occurred
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static String encrypt(final @Nonnull String plaintext) throws SecurityException {
        return encrypt(null, plaintext);
    }

    /**
     * Encrypts the given bytearray using authenticated block cipher method.
     *
     * @param bytes - The bytearray to encrypt
     * @return The encrypted bytearray or null if an error occurred
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static byte[] encrypt(final @Nonnull byte[] bytes) throws SecurityException {
        return encrypt(null, bytes);
    }

    /**
     * Encrypts the given bytearray using authenticated block cipher method.
     *
     * @param encryptionKey The base64-encoded encryption key to use. If null, a default encryption
     *                      key will be used. This should only be provided when the master key is
     *                      being used to encrypt the per-component encryption key. This is necessary
     *                      because the per-component encryption key cannot be used to encrypt itself.
     *                      Any other usage of this parameter is discouraged.
     * @param bytes - The bytearray to encrypt
     * @return The encrypted bytearray or null if an error occurred
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static byte[] encrypt(final @Nullable String encryptionKey,
                                 final @Nonnull byte[] bytes) throws SecurityException {
        return encrypt(null, bytes, DEFAULT_KEY_LENGTH_VERSION, encryptionKey);
    }

    /**
     * Encrypts the given string using authenticated block cipher method.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param plaintext     The string to encrypt.
     * @return The encrypted string or null if an error occurred
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static String encrypt(final @Nullable String keySplitValue,
                                 final @Nonnull String plaintext)
            throws SecurityException {
        try {
            byte[] encryptedBytes = encrypt(keySplitValue, plaintext.getBytes(CHARSET_CRYPTO),
                   DEFAULT_KEY_LENGTH_VERSION);
            return BaseEncoding.base64().encode(encryptedBytes);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to decode.", e);
        }
    }

    /**
     * Encrypts the given bytearray using authenticated block cipher method.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param bytes     The bytearray to encrypt.
     * @param version   The key length.
     * @return The encrypted string or null if an error occurred
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    @VisibleForTesting
    static byte[] encrypt(final @Nullable String keySplitValue,
            final @Nonnull byte[] bytes,
            final int version)
            throws SecurityException {
        return encrypt(keySplitValue, bytes, version, null);
    }

    /**
     * Encrypts the given bytearray using authenticated block cipher method.
     *
     * @param keySplitValue The user-specified portion of the PBKDF2 salt used to derive a
     *                      split key from the site secret. The value provided
     *                      constitutes the part of the salt utilized by the PBKDF2 algorithm,
     *                      but isn't stored stored with the encrypted data.
     * @param bytes     The bytearray to encrypt.
     * @param version   The key length.
     * @param encryptionKey The base64-encoded encryption key to use. If null, a default encryption
     *                      key will be used. This should only be provided when the master key is
     *                      being used to encrypt the per-component encryption key. This is necessary
     *                      because the per-component encryption key cannot be used to encrypt itself.
     *                      Any other usage of this parameter is discouraged.
     * @return The encrypted string or null if an error occurred
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    @VisibleForTesting
    static byte[] encrypt(final @Nullable String keySplitValue,
                          final @Nonnull byte[] bytes,
                          final int version,
                          final @Nullable String encryptionKey)
        throws SecurityException {
        try {
            byte[] salt = getRandomBytes(PKCS5_SALT_LENGTH);
            SecretKey key = getDerivedKey(keySplitValue, salt, version, encryptionKey);
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

            byte[] nonce = getRandomBytes(cipher.getBlockSize());
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);

            byte[] cipherText = cipher.doFinal(bytes);

            // Compose the string
            // 1 int for version, 3 for length.
            byte[] array = new byte[salt.length + nonce.length + cipherText.length + 16];
            ByteBuffer buff = ByteBuffer.wrap(array);
            buff.putInt(version);
            buff.putInt(salt.length).put(salt);
            buff.putInt(nonce.length).put(nonce);
            buff.putInt(cipherText.length).put(cipherText);
            return array;
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    /**
     * This method gets the encryption key that is stored in the dedicated docker volume or
     * kubernetes secret.

     * @param version The key length.
     * @return The encryption key that is stored in the dedicated docker volume.
     */
    private static synchronized byte[] getEncryptionKeyForVMTurboInstance(final int version) {
        // When the encryption key provide is set, prefer this over local key management
        if (enableExternalSecrets && encryptionKeyProvider != null) {
            return BaseEncoding.base64().decode(encryptionKeyProvider.getEncryptionKey());
        }

        if (encryptionKeyMap.containsKey(version)) {
            return encryptionKeyMap.get(version);
        }

        final String location =
                EnvironmentUtils.getOptionalEnvProperty(VMT_ENCRYPTION_KEY_DIR_PARAM).orElse(VMT_ENCRYPTION_KEY_DIR);
        // The path to the encryption file.
        // If Kubernetes secrets are activated, this file will be populated by a secret instead of a PV.
        Path encryptionFile = Paths.get(location + "/" + AES_VERSION_KEY_CONFIG_MAP.get(version).keyFileName);
        byte[] encryptionKeyForVMTurboInstance;
        try {
            final int numKeyBytes = AES_VERSION_KEY_CONFIG_MAP.get(version).keyLength / 8;
            if (Files.exists(encryptionFile)) {
                encryptionKeyForVMTurboInstance = Files.readAllBytes(encryptionFile);
                if (encryptionKeyForVMTurboInstance.length == numKeyBytes) {
                    return encryptionKeyForVMTurboInstance;
                } else {
                    String renameSuffix = ".damaged_" + System.currentTimeMillis();
                    Path dest = encryptionFile.resolveSibling(renameSuffix);
                    Files.move(encryptionFile, dest, StandardCopyOption.REPLACE_EXISTING);
                    logger.error("The site-specific key is damaged. Renamed to: {}",
                                 dest.getFileName());
                }
            }

            // We don't have the file or it is of the wrong length.
            // If this happens with Kubernetes secrets in use, we'll just have to log an error
            if (enableExternalSecrets) {
                final String errorMessage = "Externally-supplied encryption key is not available "
                + "although external secrets are enabled. Please check that the encryption key "
                + "secret is populated.";
                logger.error(errorMessage);
                throw new SecurityException(errorMessage);
            }

            Path outputDir = Paths.get(location);
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }

            logger.info("Initializing a new encryption key.");
            encryptionKeyForVMTurboInstance = getRandomBytes(numKeyBytes);
            encryptionKeyMap.put(version, encryptionKeyForVMTurboInstance);
            Files.write(encryptionFile, encryptionKeyForVMTurboInstance);
            return encryptionKeyForVMTurboInstance;
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    /**
     * This method gets the encryption key that is stored in the dedicated docker volume or kubernetes secret.
     *
     * <p>This method tries all versions of the encryption key until it finds a match. If no match is found,
     * Optional.empty is returned. This method was introduced in order to support upgrading from the old, PV-based
     * scheme to the newer master key-based approach. </p>
     *
     * @return the encryption key if found, else Optional.empty
     */
    public static synchronized Optional<byte[]> getEncryptionKeyForVMTurboInstance() {
        // There's probably nothing in the cache yet, in the upgrade scenario...
        // But just in case we've already read in the encryption key, let's check
        if (!encryptionKeyMap.isEmpty()) {
            return encryptionKeyMap.values().stream().findFirst();
        }

        for (AesParameter aesParameter : AES_VERSION_KEY_CONFIG_MAP.values()) {
            final String location =
                    EnvironmentUtils.getOptionalEnvProperty(VMT_ENCRYPTION_KEY_DIR_PARAM).orElse(VMT_ENCRYPTION_KEY_DIR);
            // The path to the encryption file.
            // If Kubernetes secrets are activated, this file will be populated by a secret instead of a PV.
            Path encryptionFile = Paths.get(location + "/" + aesParameter.keyFileName);
            byte[] encryptionKeyForVMTurboInstance;
            try {
                final int numKeyBytes = aesParameter.keyLength / 8;
                if (Files.exists(encryptionFile)) {
                    encryptionKeyForVMTurboInstance = Files.readAllBytes(encryptionFile);
                    if (encryptionKeyForVMTurboInstance.length == numKeyBytes) {
                        return Optional.of(encryptionKeyForVMTurboInstance);
                    }
                }
            } catch (IOException e) {
                logger.error("Failure while attempting to read encryption key file: " + e.getMessage(), e);
            }
        }
        // None of the legacy encryption key files were found (or they were the wrong length)
        return Optional.empty();
    }

    /**
     * AES parameters value object.
     */
    private static class AesParameter {
        private final int keyLength;
        private final String keyFileName;

        /**
         * Constructor.
         *
         * @param keyLength   key length
         * @param keyFileName key file name
         */
        AesParameter(final int keyLength, @Nonnull final String keyFileName) {
            this.keyLength = keyLength;
            this.keyFileName = Objects.requireNonNull(keyFileName);
        }
    }
}