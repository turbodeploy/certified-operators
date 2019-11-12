package com.vmturbo.auth.api.authorization.kvstore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.ArrayList;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Lists;

import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.IPublicKeyStore;

/**
 * {@inheritDoc}
 */
public class ComponentJwtStore implements IComponentJwtStore {
    /**
     * The keystore data file name
     */
    private static final String VMT_PRIVATE_KEY_FILE = "vmt_helper_kv.inout";

    /**
     * The charset for the passwords
     */
    private static final String CHARSET_CRYPTO = "UTF-8";
    private static final ArrayList<String> ADMIN_ROLE = Lists.newArrayList(SecurityConstant.ADMINISTRATOR);

    private static final Logger logger = LogManager.getLogger();

    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    private final @Nonnull IPublicKeyStore keyValueStore_;

    /**
     * The private key.
     * It is protected by synchronization on the instance.
     */
    private PrivateKey privateKey_ = null;

    private String keyDir;

    /**
     * Create a {@link ComponentJwtStore}.
     *
     * @param keyValueStore The key-value store to put public keys into.
     * @param identityGeneratorPrefix The prefix to use for identity generation in the provided component.
     * @param keyDir The directory to put the private key.
     */
    public ComponentJwtStore(@Nonnull final IPublicKeyStore keyValueStore,
                             final long identityGeneratorPrefix,
                             final String keyDir) {
        this.keyValueStore_ = keyValueStore;
        this.keyDir = keyDir;
        IdentityGenerator.initPrefix(identityGeneratorPrefix);
        initPKI();
    }

    // Ensure key pairs are available, and public key are stored remotely (consul).
    private void initPKI() {
        getEncryptionKeyForVMTurboInstance();
    }

    /**
     * This method gets the private key that is stored in the dedicated docker volume.
     *
     * @return The private key that is stored in the dedicated docker volume.
     */
    private synchronized @Nonnull PrivateKey getEncryptionKeyForVMTurboInstance() {
        if (privateKey_ != null) {
            return privateKey_;
        }

        Path encryptionFile = Paths.get(keyDir + "/" + VMT_PRIVATE_KEY_FILE);
        try {
            if (Files.exists(encryptionFile)) {
                byte[] keyBytes = Files.readAllBytes(encryptionFile);
                String cipherText = new String(keyBytes, CHARSET_CRYPTO);
                privateKey_ = JWTKeyCodec.decodePrivateKey(CryptoFacility.decrypt(cipherText));
                return privateKey_;
            }

            // We don't have the file or it is of the wrong length.
            Path outputDir = Paths.get(keyDir);
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }

            KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
            String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair);
            String publicKeyEncoded = JWTKeyCodec.encodePublicKey(keyPair);

            // Persist
            Files.write(encryptionFile,
                    CryptoFacility.encrypt(privateKeyEncoded).getBytes(CHARSET_CRYPTO));
            logger.info("Persisted private key.");
            keyValueStore_.putPublicKey(publicKeyEncoded);
            logger.info("Stored public key");
            privateKey_ = keyPair.getPrivate();
        } catch (IOException e) {
            throw new SecurityException(e);
        }

        return privateKey_;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @Nonnull JWTAuthorizationToken generateToken() {
        final PrivateKey privateKey = getEncryptionKeyForVMTurboInstance();
        String compact = Jwts.builder()
                .setSubject(keyValueStore_.getNamespace())
                .claim(IAuthorizationVerifier.ROLE_CLAIM, ADMIN_ROLE)
                .compressWith(CompressionCodecs.GZIP)
                .signWith(SignatureAlgorithm.ES256, privateKey)
                .compact();
        return new JWTAuthorizationToken(compact);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNamespace() {
        return keyValueStore_.getNamespace();
    }
}
