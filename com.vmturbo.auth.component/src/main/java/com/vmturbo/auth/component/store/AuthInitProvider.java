package com.vmturbo.auth.component.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Auth component initialization.
 */
public class AuthInitProvider {
    /**
     * The keystore data file name.
     */
    private static final String VMT_PRIVATE_KEY_FILE = "vmt_helper_kv.inout";
    /**
     * The init claim.
     */
    private static final String CLAIM = "initStatus";

    /**
     * The charset for the passwords.
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * The init status.
     */
    private static final String INIT_SUBJECT = "admin";
    /**
     * The admin initialization file name.
     */
    private static final String VMT_INIT_KEY_FILE = "vmt_helper_init.inout";
    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    final @Nonnull
    KeyValueStore keyValueStore_;
    final Supplier<String> keyValueDir;
    private final Logger logger_ = LogManager.getLogger();
    /**
     * The private key. It is protected by synchronization on the instance.
     */
    PrivateKey privateKey_ = null;

    public AuthInitProvider(KeyValueStore keyValueStore, Supplier<String> keyValueDir) {
        this.keyValueStore_ = keyValueStore;
        this.keyValueDir = keyValueDir;
    }

    /**
     * This method gets the private key that is stored in the dedicated docker volume.
     *
     * @return The private key that is stored in the dedicated docker volume.
     */
    synchronized @Nonnull PrivateKey getEncryptionKeyForVMTurboInstance() {
        if (privateKey_ != null) {
            return privateKey_;
        }

        final String location = keyValueDir.get();
        Path encryptionFile = Paths.get(location + "/" + VMT_PRIVATE_KEY_FILE);
        try {
            if (Files.exists(encryptionFile)) {
                byte[] keyBytes = Files.readAllBytes(encryptionFile);
                String cipherText = new String(keyBytes, CHARSET_CRYPTO);
                privateKey_ = JWTKeyCodec.decodePrivateKey(CryptoFacility.decrypt(cipherText));
                return privateKey_;
            }

            // We don't have the file or it is of the wrong length.
            Path outputDir = Paths.get(location);
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }

            KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
            String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair);
            String publicKeyEncoded = JWTKeyCodec.encodePublicKey(keyPair);

            // Persist
            Files.write(encryptionFile,
                    CryptoFacility.encrypt(privateKeyEncoded).getBytes(CHARSET_CRYPTO));
            keyValueStore_.put(IAuthStore.KV_KEY, publicKeyEncoded);
            privateKey_ = keyPair.getPrivate();
        } catch (IOException e) {
            throw new SecurityException(e);
        }

        return privateKey_;
    }

    /**
     * Checks whether the admin user has been instantiated. When the XL first starts up, there is no
     * user defined in the XL. The first required step is to instantiate an admin user. This method
     * checks whether an admin user has been instantiated in XL.
     *
     * @return {@code true} iff the admin user has been instantiated.
     * @throws SecurityException In case of an error performing the check.
     */
    public boolean checkAdminInit() throws SecurityException {
        // Make sure we have initialized the site secret.
        getEncryptionKeyForVMTurboInstance();
        // The file contains the flag that specifies whether admin user has been initialized.
        final String location = keyValueDir.get();
        Path encryptionFile = Paths.get(location + "/" + VMT_INIT_KEY_FILE);
        try {
            if (Files.exists(encryptionFile)) {
                byte[] keyBytes = Files.readAllBytes(encryptionFile);
                String cipherText = new String(keyBytes, CHARSET_CRYPTO);
                String token = CryptoFacility.decrypt(cipherText);
                Optional<String> key = keyValueStore_.get(IAuthStore.KV_KEY);
                if (!key.isPresent()) {
                    throw new SecurityException("The public key is unavailable");
                }
                final Jws<Claims> claims = Jwts.parser()
                        .setSigningKey(JWTKeyCodec.decodePublicKey(key.get()))
                        .parseClaimsJws(token);
                final String status = (String)claims.getBody().get(CLAIM);
                // Check subject.
                if (!INIT_SUBJECT.equals(claims.getBody().getSubject())) {
                    throw new SecurityException(
                            "The admin instantiation status has been tampered with.");
                }
                // Check status validity
                if (!"true".equals(status) && !"false".equals(status)) {
                    throw new SecurityException(
                            "The admin instantiation status has been tampered with.");
                }
                return Boolean.parseBoolean(status);
            } else {
                logger_.info("The admin user hasn't been initialized yet.");
            }
            return false;
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Initializes an admin user and creates predefined external groups for all roles in XL. When
     * the XL first starts up, there is no user defined in the XL. The first required step is to
     * instantiate an admin user and creates predefined external groups for all roles in XL. This
     * should only be called once. If it is called more than once, this method will return {@code
     * false}.
     *
     * @param userName The user name.
     * @param password The password.
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of an error initializing the admin user.
     */
    public boolean initAdmin(final @Nonnull String userName, final @Nonnull String password)
            throws SecurityException {
        // Make sure we have initialized the site secret.
        getEncryptionKeyForVMTurboInstance();
        final String location = keyValueDir.get();
        Path encryptionFile = Paths.get(location + "/" + VMT_INIT_KEY_FILE);
        try {
            if (Files.exists(encryptionFile)) {
                return false;
            }

            String compact = Jwts.builder()
                    .setSubject(INIT_SUBJECT)
                    .claim(CLAIM, "true")
                    .compressWith(CompressionCodecs.GZIP)
                    .signWith(SignatureAlgorithm.ES256, privateKey_)
                    .compact();

            // Persist the initialization status.
            Files.write(encryptionFile, CryptoFacility.encrypt(compact).getBytes(CHARSET_CRYPTO));
            return true;
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }
}