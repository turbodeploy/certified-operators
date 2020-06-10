package com.vmturbo.licensing.utils;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.xml.bind.DatatypeConverter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.crypto.bcrypt.BCrypt;

public class TurboEncryptionUtil {

    /**
     * Supported Hash functions
     */
    public enum HashFunc {
        SHA1, MD5
    }

    /**
     * The default encryption key location
     */
    public static final String VMT_ENCRYPTION_KEY_DIR = "data/output";

    /**
     * The key localtion property
     */
    public static final String VMT_ENCRYPTION_KEY_DIR_PARAM = "com.vmturbo.keydir";

    /**
     * The keystore data file name
     */
    public static final String VMT_ENCRYPTION_KEY_FILE = "vmt_helper_data.out";

    /**
     * The charset for the passwords
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * The cipher algorithm
     */
    private static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";

    /**
     * The fast cipher algorithm
     */
    private static final String FAST_CIPHER_ALGORITHM = "AES/ECB/PKCS5Padding";

    /**
     * The authenticated cipher algorithm.
     */
    private static final String CIPHER_ALGORITHM_AUTHENTICATED = "AES/GCM/NoPadding";

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
    private static final String PBKDF2_DERIVATION_ALGORITHM = "PBKDF2WithHmacSHA1";

    /**
     * The salt length
     */
    private static final int PKCS5_SALT_LENGTH = 0x20;

    /**
     * The VMTurbo instance encryption keyt length.
     */
    private static final int VMT_INSTANCE_KEY_LENGTH = 0x20;

    /**
     * The cipher key length
     */
    private static final int KEY_LENGTH = 128;

    /**
     * The PBKDF2 iteration count
     */
    private static final int ITERATIONS = 32768;

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
    private static String encryptionKeyForVMTurboInstance = null;

    /**
     * The encryption key with Java 7 encoding.
     * We presume here that Java 7 leaves only one replacement character '0xFFFD'
     * at the end of UTF-8 encoded string.
     */
    private static String encryptionKeyForVMTurboInstanceJava7 = null;

    /**
     * The magic cookie.
     */
    private static final String MAGIC_COOKIE_V1 = "$_1_$VMT$$";

    private static final SecretKey SESSION_KEY = generateSessionKey(KEYSPEC_ALGORITHM, 128);


    /**
     * Disable instantiation of the class.
     */
    private TurboEncryptionUtil() {
    }


    private static final Logger logger = LogManager.getLogger(TurboEncryptionUtil.class);


    /**
     * Hashes the given string using the specified function.
     * It is a legacy support for most of the cases where MD5 is used in its current form.
     * Others are handled using {@link #hash(String, HashFunc)} with MD5 as a parameter.
     * This is used by the Login manager and service.
     * NB! Needs to be replaced.
     *
     * @param toHash hash string.
     * @return The hex string. Might be invalid one.
     */
    public static @Nonnull
    String hashMD5Legacy(@Nonnull String toHash) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(toHash.getBytes(StandardCharsets.ISO_8859_1));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(Integer.toHexString(0x00FF & b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new SecurityException("Exception when trying to hash.", e);
        }
    }

    /**
     * Hashes the given string using the specified function.
     *
     * @param toHash The string to be hashed.
     * @param func   The hash function.
     * @return The hashed string in hex.
     */
    public static String hash(String toHash, HashFunc func) {
        try {
            MessageDigest md = null;
            switch (func) {
                case SHA1:
                    md = MessageDigest.getInstance("SHA1");
                    break;
                case MD5:
                    md = MessageDigest.getInstance("MD5");
                    break;
            }
            return bytes2Hex(md.digest(toHash.getBytes(StandardCharsets.ISO_8859_1)));
        } catch (Exception e) {
            throw new SecurityException("Exception when trying to hash.", e);
        }
    }

    /**
     * Use secure (and slow) hashing algorithm. The BCrypt is the one being chosen here.
     *
     * @param plainText The plain text.
     * @return The secure hash.
     */
    public static @Nonnull
    String secureHash(@Nonnull String plainText) {
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
            // Will be thrown in case the SALT has not been generated by BCrypt.
            return false;
        }
    }

    /**
     * Converts a byte[] to a readable Hex string.
     *
     * @param bytes The byte array to convert to hex string.
     * @return The hex string representation of the byte array.
     */
    public static String bytes2Hex(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        if (bytes.length == 0) {
            return "";
        }
        return DatatypeConverter.printHexBinary(bytes).toLowerCase();
    }

    // *** New encryption


    /**
     * Encrypt message using provided key.
     * Use to encrypt messages temporary stored in memory
     *
     * @param key   key
     * @param value message for encryption
     * @return encrypted message
     * @throws NoSuchAlgorithmException
     * @throws NoSuchPaddingException
     * @throws InvalidKeyException
     * @throws IllegalBlockSizeException
     * @throws BadPaddingException
     */
    private static byte[] fastEncrypt(SecretKey key, byte[] value) throws NoSuchAlgorithmException,
            NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        final Cipher cipher = Cipher.getInstance(FAST_CIPHER_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(value);
    }

    /**
     * Decrypt message using provided key.
     * Use to decrypt messages temporary stored in memory
     *
     * @param key key
     * @param ct  message for decryption
     * @return decrypted message
     * @throws NoSuchAlgorithmException
     * @throws NoSuchPaddingException
     * @throws InvalidKeyException
     * @throws IllegalBlockSizeException
     * @throws BadPaddingException
     */
    private static byte[] fastDecrypt(SecretKey key, byte[] ct) throws NoSuchAlgorithmException,
            NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        {
            final Cipher cipher = Cipher.getInstance(FAST_CIPHER_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, key);
            byte[] pt = cipher.doFinal(ct);
            return pt;
        }

    }

    /**
     * Generate random encryption key.
     *
     * @param algorithm encryption algorithm
     * @param keySize   key size
     * @return encryption key
     */
    private static SecretKey generateSessionKey(String algorithm, int keySize) {
        try {
            final KeyGenerator keygen = KeyGenerator.getInstance(algorithm);
            keygen.init(keySize);
            return keygen.generateKey();
        } catch (NoSuchAlgorithmException ex) {
            return null;
        }
    }

}

