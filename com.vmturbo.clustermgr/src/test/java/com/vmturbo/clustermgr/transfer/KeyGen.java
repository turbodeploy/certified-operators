package com.vmturbo.clustermgr.transfer;

import java.nio.charset.Charset;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;

/**
 * Will be used ONLY when new set of private/public keys has to be generated.
 * This set of keys will be used to encrypt/decrypt the session keys generated
 * for each telemetry data transfer.
 */
public class KeyGen {
    /**
     * The Charset.
     */
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * The algorithm.
     */
    private static final String ALGORITHM = "RSA";

    /**
     * The key size.
     * We use 3072 bit, as it appears a  good compromise between a minimum of 2048 bit and the
     * performance hit. The 4096 bit is not yet required at this point.
     */
    private final static int KEYSIZE = 3072;

    /**
     * The authenticated cipher algorithm.
     */
    private KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
        keyGen.initialize(KEYSIZE, SecureRandom.getInstanceStrong());
        return keyGen.generateKeyPair();
    }

    /**
     * Encodes the public key.
     *
     * @param publicKey The public key.
     * @return The public key encoded as Base64 UTF-8 string.
     */
    private String encodePublicKey(final PublicKey publicKey) {
        return new String(Base64.getEncoder().encode(publicKey.getEncoded()), CHARSET);
    }

    /**
     * Encodes the private key.
     *
     * @param privateKey The private key.
     * @return The private key encoded as Base64 UTF-8 string.
     */
    private String encodePrivateKey(final PrivateKey privateKey) {
        return new String(Base64.getEncoder().encode(privateKey.getEncoded()), CHARSET);
    }

    /**
     * Generate the key.
     *
     * @param args Not used.
     * @throws Exception Won't get thrown.
     */
    public static void main(String[] args) throws Exception {
        KeyGen keyGen = new KeyGen();
        KeyPair keyPair = keyGen.generateKeyPair();
        String publicKey = keyGen.encodePublicKey(keyPair.getPublic());
        String privateKey = keyGen.encodePrivateKey(keyPair.getPrivate());

        // Test. Decode the private key and decrypt something encrypted by the public key.
        byte[] keyBytes = privateKey.getBytes("UTF-8");
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(keyBytes));
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PrivateKey key = kf.generatePrivate(spec);

        // Test against 512-bit byte array.
        byte[] data = new byte[0x40]; // 64 bytes.
        SecureRandom.getInstanceStrong().nextBytes(data);

        // Encrypt.
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, keyPair.getPublic());
        byte[] cipherText = cipher.doFinal(data);

        // Decrypt.
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] plainText = cipher.doFinal(cipherText);
        if (!Arrays.equals(data, plainText)) {
            System.err.println("The key generation failed.");
            System.exit(1);
        }
        System.out.println("public: " + publicKey);
        System.out.println("private: " + privateKey);
    }
}
