package com.vmturbo.components.crypto;

import static com.vmturbo.components.crypto.CryptoFacility.CHARSET_CRYPTO;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Date;

import com.google.common.io.BaseEncoding;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verify {@link CryptoFacility}.
 */
public class CryptoFacilityTest {

    private static final String VALUE = "subject";

    /**
     * Rule.
     */
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Before.
     * @throws Exception when failing to create temp folder.
     */
    @Before
    public void setUp() throws Exception {
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());
    }

    /**
     * Verify empty string.
     */
    @Test
    public void testCryptoEmpty() {
        String plaintext = "";
        String ciphertext = CryptoFacility.encrypt(plaintext);
        assertEquals(plaintext, CryptoFacility.decrypt(ciphertext));
    }

    /**
     * Verify base encryption case.
     */
    @Test
    public void testCrypto() {
        String data = (new Date()).toString();
        String ciphertext = CryptoFacility.encrypt(data);
        assertEquals(data, CryptoFacility.decrypt(ciphertext));
    }

    /**
     * Verify supporting key/value case, encrypting value.
     */
    @Test
    public void testCryptoSubject() {
        String data = (new Date()).toString();
        String ciphertext = CryptoFacility.encrypt(VALUE, data);
        assertEquals(data, CryptoFacility.decrypt(VALUE, ciphertext));
    }

    /**
     * Verify support encrypt and decrypt with both AES 128 and 256 key lengths.
     *
     * @throws UnsupportedEncodingException when failing to encode.
     */
    @Test
    public void testEncryptBothKeyLength() throws UnsupportedEncodingException {
        String ciphertextWith128BitsKey = BaseEncoding.base64()
                .encode(CryptoFacility.encrypt(null, VALUE.getBytes(CHARSET_CRYPTO), 1));
        assertEquals(VALUE, CryptoFacility.decrypt(null, ciphertextWith128BitsKey));

        String ciphertextWith256BitsKey = BaseEncoding.base64()
                .encode(CryptoFacility.encrypt(null, VALUE.getBytes(CHARSET_CRYPTO), 2));
        assertEquals(VALUE, CryptoFacility.decrypt(null, ciphertextWith256BitsKey));
    }

    /**
     * Verify supporting encrypting/decrypting using a provided encryption key.
     *
     * <p>This is necessary only when using the master key to encrypt/decrypt the internal, per-
     * component key.</p>
     */
    @Test
    public void testMasterKey() {
        // Prepare
        final String masterKey = BaseEncoding.base64().encode(CryptoFacility.getRandomBytes(32));
        final byte[] encryptionKeyToStore = CryptoFacility.getRandomBytes(32);
        // Act
        final byte[] ciphertext = CryptoFacility.encrypt(masterKey, encryptionKeyToStore);
        final byte[] result = CryptoFacility.decrypt(null, masterKey, ciphertext);
        // Verify
        assertArrayEquals(encryptionKeyToStore, result);
    }

    /**
     * Verify supporting encrypting/decrypting using a provided encryption key.
     *
     * <p>This is necessary only when using the master key to encrypt/decrypt the internal, per-
     * component key.</p>
     */
    @Test
    public void testMasterKeyWithSplitKey() {
        // Prepare
        final String masterKey = BaseEncoding.base64().encode(CryptoFacility.getRandomBytes(32));
        final byte[] encryptionKeyToStore = CryptoFacility.getRandomBytes(32);
        // Act
        final byte[] ciphertext = CryptoFacility.encrypt(VALUE, encryptionKeyToStore, 2, masterKey);
        final byte[] result = CryptoFacility.decrypt(VALUE, masterKey, ciphertext);
        // Verify
        assertArrayEquals(encryptionKeyToStore, result);
    }

    /**
     * Verify exception is thrown with random cipher text.
     */
    @Test(expected = SecurityException.class)
    public void testEncryptNegative() {
        CryptoFacility.decrypt("test123wrong");
    }

    /**
     * Verify supporting key/value case, if wrong key is provided exception is thrown.
     */
    @Test(expected = SecurityException.class)
    public void testCryptoSubjectWrongSubject() {
        String data = (new Date()).toString();
        String ciphertext = CryptoFacility.encrypt(VALUE, data);
        assertEquals(data, CryptoFacility.decrypt("subjectBad", ciphertext));
    }
}
