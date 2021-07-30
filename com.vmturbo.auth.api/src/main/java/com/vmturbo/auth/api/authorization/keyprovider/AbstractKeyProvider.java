package com.vmturbo.auth.api.authorization.keyprovider;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.kvstore.IPublicKeyStore;

/**
 * A base implementation for a KeyProvider, providing (generating, storing and retrieving) private/public key pairs.
 */
public abstract class AbstractKeyProvider implements KeyProvider {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The charset for the passwords.
     */
    protected static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * A place to publish the public key after its creation.
     */
    private final IPublicKeyStore publicKeyStore;

    /**
     * The private key.
     *
     * <p>Lazy loaded. It is protected by synchronization on the instance.</p>
     */
    protected PrivateKey privateKey = null;

    private PublicKey publicKey = null;

    /**
     * Create a new KeyProvider.
     *
     * @param publicKeyStore the keyStore to use for storing the public key, once created
     */
    public AbstractKeyProvider(@Nonnull final IPublicKeyStore publicKeyStore) {
        this.publicKeyStore = Objects.requireNonNull(publicKeyStore);
    }

    /**
     * Gets the private key that is stored in the dedicated docker volume.
     *
     * @return The private key that is stored in the dedicated docker volume.
     */
    protected synchronized @Nonnull PrivateKey getPrivateKeyInternal() {
        if (doesPrivateKeyExist()) {
            String cipherText = getEncryptedPrivateKey();
            privateKey = JWTKeyCodec.decodePrivateKey(CryptoFacility.decrypt(cipherText));
            return privateKey;
        }

        // We don't have the private key file, so generate a new one.
        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        String publicKeyEncoded = JWTKeyCodec.encodePublicKey(keyPair.getPublic());

        // Encrypt the private key before storing it, using the secret encryption key
        String privateKeyEncrypted = CryptoFacility.encrypt(privateKeyEncoded);
        persistPrivateKey(privateKeyEncrypted);
        logger.info("Persisted private key.");
        // The public key is stored unencrypted, so that other services can use it to validate our JWTs
        publicKeyStore.putPublicKey(publicKeyEncoded);
        logger.info("Stored public key");
        privateKey = keyPair.getPrivate();
        publicKey = keyPair.getPublic();

        return privateKey;
    }

    @Override
    public PublicKey getPublicKey() {
        if (publicKey != null) {
            return publicKey;
        }
        publicKey = publicKeyStore.getPublicKey(publicKeyStore.getNamespace())
            .map(JWTKeyCodec::decodePublicKey)
            .orElse(null);
        return publicKey;
    }

    protected abstract boolean doesPrivateKeyExist();

    protected abstract String getEncryptedPrivateKey();

    protected abstract void persistPrivateKey(@Nonnull String privateKeyEncrypted);
}
