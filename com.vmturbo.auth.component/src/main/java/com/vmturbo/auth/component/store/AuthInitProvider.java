package com.vmturbo.auth.component.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.keyprovider.IKeyImportIndicator;
import com.vmturbo.auth.api.authorization.keyprovider.KeyProvider;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Auth component initialization.
 */
public class AuthInitProvider {

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
     * The admin initialization JWT file name.
     *
     * <p>This is only relevant when auth secrets are disabled and the legacy logic (with PVs)
     * is being used.</p>
     */
    private static final String VMT_INIT_KEY_FILE = "vmt_helper_init.inout";

    /**
     * The KV store (Consul) key for storing the JWT used to determine if the admin user has been created.
     *
     * <p>This is only relevant when auth secrets are enabled.</p>
     */
    private static final String KV_JWT_KEY = "public_key/jwt";

    private final Logger logger_ = LogManager.getLogger();

    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    private final KeyValueStore keyValueStore_;

    /**
     * Directory to store the admin JWT (when not using auth secrets).
     */
    private final Supplier<String> keyValueDir;

    /**
     * For providing (generating, storing and retrieving) private/public key pairs.
     */
    private final KeyProvider keyProvider;

    /**
     * If true, use Kubernetes secrets to read in the sensitive Auth data (like encryption keys and
     * private/public key pairs). If false, this data will be read from (legacy) persistent volumes.
     */
    private final boolean enableExternalSecrets;

    /**
     * Create the AuthInitProvider.
     *
     * @param keyValueStore a place to store public keys and JWT tokens
     * @param keyValueDir directory to store the admin JWT (when not using auth secrets)
     * @param enableExternalSecrets whether to enable sourcing encryption keys through kubernetes secrets
     * @param keyProvider for providing (generating, storing and retrieving) private/public key pairs
     * @param keyImportIndicator indicates whether a security key import is in progress
     */
    public AuthInitProvider(@Nonnull final KeyValueStore keyValueStore,
                            @Nonnull final Supplier<String> keyValueDir,
                            boolean enableExternalSecrets,
                            @Nonnull final KeyProvider keyProvider,
                            @Nullable IKeyImportIndicator keyImportIndicator) {
        this.keyValueStore_ = Objects.requireNonNull(keyValueStore);
        this.keyValueDir = Objects.requireNonNull(keyValueDir);
        this.enableExternalSecrets = enableExternalSecrets;
        this.keyProvider = Objects.requireNonNull(keyProvider);
        checkForImportedKey(keyImportIndicator);
    }

    private void initKeys() {
        // Make sure we have initialized the site secret.
        keyProvider.getPrivateKey();
    }

    /**
     * Check whether an existing encryption key was recently imported (i.e., from the legacy PV storage). If so,
     * then we need to ensure that the admin JWT is set so that users won't be prompted to re-create the
     * administrator user in existing environments.
     * @param keyImportIndicator
     */
    private void checkForImportedKey(@Nullable IKeyImportIndicator keyImportIndicator) {
        if (keyImportIndicator != null) {
            // Ensure the security keys have been initialized
            initKeys();
            // If the encryption key was just imported...
            if (keyImportIndicator.wasEncryptionKeyImported()) {
                // ... then we know this is an existing installation which has just been upgraded.
                // Specifically, we expect this to happen when migrating from the legacy Persistent Volume-based
                // approach for storing encryption keys to the newer, "Master Key" approach.
                // Mark the administrator user as already initialized. This is necessary since the admin JWT will not
                // be migrated.
                try {
                    logger_.info("Setting the admin init flag to 'true' as part of a data migration.");
                    initAdminUsingSecrets();
                    logger_.info("Successfully set the admin init flag.");
                } catch (Exception e) {
                    // Do not blow up spring initialization if an error occurs--just log it.
                    logger_.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Checks whether the admin user has been instantiated. When the XL first starts up, there is no
     * user defined in the XL. The first required step is to instantiate an admin user. This method
     * checks whether an admin user has been instantiated in XL.
     *
     * <p>We indicate that the admin user has been created by creating a JWT, which provides a
     * tamper-resistant record of the event. Currently, we store this JWT in Consul but in our
     * older deployments we stored it on a persistent volume.</p>
     *
     * @return {@code true} iff the admin user has been instantiated.
     * @throws SecurityException In case of an error performing the check.
     */
    public boolean checkAdminInit() throws SecurityException {
        initKeys();

        if (enableExternalSecrets) {
            // Use K8s secrets
            return checkAdminInitUsingSecrets();
        }
        // else, Legacy behavior

        // The file contains the flag that specifies whether admin user has been initialized.
        final String location = keyValueDir.get();
        Path encryptionFile = Paths.get(location + "/" + VMT_INIT_KEY_FILE);
        try {
            if (Files.exists(encryptionFile)) {
                //TODO: We need to write the public key to consul using our special format
                // Call JWTKeyCodec.encodePublicKey(keyPair) to do this
                // It may be easier to generate the public key rather than read it from a secret!
                byte[] keyBytes = Files.readAllBytes(encryptionFile);
                String cipherText = new String(keyBytes, CHARSET_CRYPTO);
                String token = CryptoFacility.decrypt(cipherText);
                return parseAdminJwt(token);
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
        initKeys();

        if (enableExternalSecrets) {
            // Use K8s secrets
            return initAdminUsingSecrets();
        }
        // else, Legacy behavior

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
                    .signWith(SignatureAlgorithm.ES256, keyProvider.getPrivateKey())
                    .compact();

            // Persist the initialization status.
            Files.write(encryptionFile, CryptoFacility.encrypt(compact).getBytes(CHARSET_CRYPTO));
            return true;
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Checks using secrets whether the admin user has been instantiated. When the XL first starts up,
     * there is no user defined in the XL. The first required step is to instantiate an admin user.
     * This method checks whether an admin user has been instantiated in XL.
     *
     * <p>We indicate that the admin user has been created by creating a JWT, which provides a
     * tamper-resistant record of the event. When using auth secrets, we store this JWT in Consul.</p>
     *
     * @return {@code true} iff the admin user has been instantiated.
     * @throws SecurityException In case of an error performing the check.
     */
    private boolean checkAdminInitUsingSecrets() throws SecurityException {
        if (!enableExternalSecrets) {
            throw new IllegalStateException("Cannot check admin init using secrets when auth secrets"
                + " are not enabled!");
        }

        // The JWT stored in consul contains the flag that specifies whether admin user has been initialized.
        return keyValueStore_.get(KV_JWT_KEY)
            .map(CryptoFacility::decrypt)
            .map(this::parseAdminJwt)
            .orElseGet(() -> {
            logger_.debug("The admin user hasn't been initialized yet.");
            return false;
        });
    }

    private boolean parseAdminJwt(final String token) {
        //TODO: Consider caching the public key--why keep going to Consul?
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
    }

    /**
     * Initializes an admin user using secrets and creates predefined external groups for all roles
     * in XL. When XL first starts up, there is no user defined in the XL. The first required step is
     * to instantiate an admin user and creates predefined external groups for all roles in XL. This
     * should only be called once. If it is called more than once, this method will return {@code
     * false}.
     *
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of an error initializing the admin user.
     */
    private boolean initAdminUsingSecrets()
        throws SecurityException {
        if (!enableExternalSecrets) {
            throw new IllegalStateException("Cannot check admin init using secrets when auth secrets"
                + " are not enabled!");
        }

        final Optional<String> possibleJwt = keyValueStore_.get(KV_JWT_KEY);
        if (possibleJwt.isPresent()) {
            logger_.debug("Cannot initialize the admin user once it has already been initialized!");
            return false;
        }

        String compact = Jwts.builder()
            .setSubject(INIT_SUBJECT)
            .claim(CLAIM, "true")
            .compressWith(CompressionCodecs.GZIP)
            .signWith(SignatureAlgorithm.ES256, keyProvider.getPrivateKey())
            .compact();
        String encryptedCompact = CryptoFacility.encrypt(compact);

        // Persist the initialization status.
        keyValueStore_.put(KV_JWT_KEY, encryptedCompact);
        logger_.debug("Admin user initialization has begun.");
        return true;
    }

}