package com.vmturbo.components.crypto;

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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.io.BaseEncoding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.crypto.bcrypt.BCrypt;

/**
 * The CryptoFacility is a utility class that provides encryption and secure hash services.
 */
public class CryptoFacility {

    /**
     * The logger.
     */
    private static Logger logger = LogManager.getLogger(CryptoFacility.class);

    /**
     * The key localtion property
     */
    private static final String VMT_ENCRYPTION_KEY_DIR_PARAM = "com.vmturbo.keydir";

    /**
     * The default encryption key location
     */
    private static final String VMT_ENCRYPTION_KEY_DIR = "/home/turbonomic/data/helper_dir";

    /**
     * The keystore data file name
     */
    private static final String VMT_ENCRYPTION_KEY_FILE = "vmt_helper_data.out";

    /**
     * The charset for the passwords
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * The authenticated cipher algorithm.
     */
    private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";

    /**
     * The GCM tag length in bits.
     */
    private static final int GCM_TAG_LENGTH = 128;

    /**
     * The cipher key specification algorithm
     */
    private static final String KEYSPEC_ALGORITHM = "AES";

    /**
     * The key derivation algorithm
     */
    private static final String PBKDF2_DERIVATION_ALGORITHM = "PBKDF2WithHmacSHA256";

    /**
     * The salt length
     */
    public static final int PKCS5_SALT_LENGTH = 0x20;

    /**
     * The PBKDF2 iteration count
     */
    private static final int PBKDF2_ITERATIONS = 32768;

    /**
     * The cipher key length
     */
    private static final int KEY_LENGTH_BITS = 128;

    /**
     * The version
     */
    private static final int VERSION = 1;

    /**
     * The secure random
     */
    private static final SecureRandom random = new SecureRandom();

    /**
     * The encryption key
     */
    private static byte[] encryptionKeyForVMTurboInstance = null;

    /**
     * The magic cookie.
     */
    private static final String MAGIC_COOKIE_V1 = "$_1_$VMT$$";

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
        // Be a little defensive here.
        if (ciphertext == null) {
            throw new SecurityException("Null ciphertext.");
        }
        try {
            final byte[] cipherData = BaseEncoding.base64().decode(ciphertext);
            return new String(decrypt(keySplitValue, cipherData), CHARSET_CRYPTO);
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
        // Be a little defensive here.
        if (cipherdata == null) {
            throw new SecurityException("Null cipher data.");
        }
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            ByteBuffer buff = ByteBuffer.wrap(cipherdata);
            int version = buff.getInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("The version is unsupported");
            }

            int length = buff.getInt();
            if (length != PKCS5_SALT_LENGTH) {
                throw new SecurityException("Corrupted cipher data.");
            }
            byte[] salt = new byte[length];
            buff.get(salt);

            length = buff.getInt();
            if (length != cipher.getBlockSize()) {
                throw new SecurityException("Corrupted cipher data.");
            }
            byte[] nonce = new byte[length];
            buff.get(nonce);

            length = buff.getInt();
            byte[] cipherBytes = new byte[length];
            buff.get(cipherBytes);

            SecretKey key = getDerivedKey(keySplitValue, salt);
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
            byte[] decryptedData = cipher.doFinal(cipherBytes);
            return decryptedData;
        } catch (Exception e) {
            throw new SecurityException("Unable to decrypt.", e);
        }
    }


    /**
     * Use secure (and slow) algorithm for one-way password hashing.
     * TODO(Michael): Replace with an industry standard algorithm.
     *
     * @param plainText The plain text.
     * @return The secure hash.
     */
    public static @Nonnull String secureHash(@Nonnull String plainText) {
        String salt = BCrypt.gensalt(10);
        return BCrypt.hashpw(plainText, salt);
    }

    /**
     * Check whether the supplied password is the one that was hashed into hashed.
     *
     * @param hashed   The previously hashed password.
     * @param password The password in plain text.
     * @return {@code true} if the supplied password is the one that was hashed into hashed.
     */
    public static boolean checkSecureHash(@Nonnull String hashed, @Nonnull String password) {
        try {
            return BCrypt.checkpw(password, hashed);
        } catch (IllegalArgumentException ex) {
            return false;
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
     * @return The derived site secret.
     */
    private static SecretKey getDerivedKey(final String keySplitValue,
                                           final @Nonnull byte[] salt) {
        // In case there is no seed, use the direct key.
        if (keySplitValue == null) {
            return new SecretKeySpec(getEncryptionKeyForVMTurboInstance(), KEYSPEC_ALGORITHM);
        }

        try {
            // Convert the site secret to string using Base64.
            byte[] seedData = keySplitValue.getBytes(CHARSET_CRYPTO);
            byte[] finalSalt = new byte[seedData.length + salt.length];
            System.arraycopy(seedData, 0, finalSalt, 0, seedData.length);
            System.arraycopy(salt, 0, finalSalt, seedData.length, salt.length);
            byte[] siteSecretBytes = getEncryptionKeyForVMTurboInstance();
            String siteSecret = BaseEncoding.base64().encode(siteSecretBytes);
            KeySpec specs = new PBEKeySpec(siteSecret.toCharArray(),
                                           finalSalt,
                                           PBKDF2_ITERATIONS,
                                           KEY_LENGTH_BITS);
            SecretKeyFactory kf = SecretKeyFactory.getInstance(PBKDF2_DERIVATION_ALGORITHM);
            return new SecretKeySpec(kf.generateSecret(specs).getEncoded(), KEYSPEC_ALGORITHM);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException |
                UnsupportedEncodingException e) {
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
            byte[] encryptedBytes = encrypt(keySplitValue, plaintext.getBytes(CHARSET_CRYPTO));
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
     * @return The encrypted string or null if an error occurred
     * @throws SecurityException In the case of any error decrypting the ciphertext.
     */
    public static byte[] encrypt(final @Nullable String keySplitValue,
                                 final @Nonnull byte[] bytes)
            throws SecurityException {
        try {
            byte[] salt = getRandomBytes(PKCS5_SALT_LENGTH);
            SecretKey key = getDerivedKey(keySplitValue, salt);
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

            byte[] nonce = getRandomBytes(cipher.getBlockSize());
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);

            byte[] cipherText = cipher.doFinal(bytes);

            // Compose the string
            // 1 int for version, 3 for length.
            byte[] array = new byte[salt.length + nonce.length + cipherText.length + 16];
            ByteBuffer buff = ByteBuffer.wrap(array);
            buff.putInt(VERSION);
            buff.putInt(salt.length).put(salt);
            buff.putInt(nonce.length).put(nonce);
            buff.putInt(cipherText.length).put(cipherText);
            return array;
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }

    /**
     * This method gets the encryption key that is stored in the dedicated docker volume.
     *
     * @return The encryption key that is stored in the dedicated docker volume.
     */
    private static synchronized byte[] getEncryptionKeyForVMTurboInstance() {
        if (encryptionKeyForVMTurboInstance != null) {
            return encryptionKeyForVMTurboInstance;
        }

        final String location =
                System.getProperty(VMT_ENCRYPTION_KEY_DIR_PARAM, VMT_ENCRYPTION_KEY_DIR);
        Path encryptionFile = Paths.get(location + "/" + VMT_ENCRYPTION_KEY_FILE);
        try {
            if (Files.exists(encryptionFile)) {
                encryptionKeyForVMTurboInstance = Files.readAllBytes(encryptionFile);
                if (encryptionKeyForVMTurboInstance.length == KEY_LENGTH_BITS / 8) {
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
            Path outputDir = Paths.get(location);
            if (!Files.exists(outputDir)) {
                Path dir = Files.createDirectories(outputDir);
            }

            encryptionKeyForVMTurboInstance = getRandomBytes(KEY_LENGTH_BITS / 8);
            Files.write(encryptionFile, encryptionKeyForVMTurboInstance);
        } catch (Exception e) {
            throw new SecurityException(e);
        }

        return encryptionKeyForVMTurboInstance;
    }
}
