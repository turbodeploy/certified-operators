package com.vmturbo.auth.component.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;

import com.vmturbo.api.enums.UserRole;
import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.authentication.AuthenticationException;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.GroupDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.component.store.sso.SsoUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.IP_ADDRESS_CLAIM;
import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.UUID_CLAIM;

/**
 * The consul-backed authentication store that holds the users information.
 */
@ThreadSafe
public class AuthProvider {

    /**
     * The KV prefix.
     */
    @VisibleForTesting
    static final String PREFIX = "users/";

    /**
     * The KV AD prefix.
     */
    private static final String PREFIX_AD = "ad/info";

    private static final String PREFIX_GROUP = "groups/";

    /**
     * The key location property
     */
    private static final String VMT_PRIVATE_KEY_DIR_PARAM = "com.vmturbo.kvdir";

    /**
     * The default encryption key location
     */
    private static final String VMT_PRIVATE_KEY_DIR = "/home/turbonomic/data/kv";

    /**
     * The keystore data file name
     */
    private static final String VMT_PRIVATE_KEY_FILE = "vmt_helper_kv.inout";

    /**
     * The admin initialization file name
     */
    private static final String VMT_INIT_KEY_FILE = "vmt_helper_init.inout";

    /**
     * The charset for the passwords
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    /**
     * The expiration time is 10 minutes.
     */
    private static final int TOKEN_EXPIRATION_MIN = 10;
    public static final String UNABLE_TO_AUTHORIZE_THE_USER = "Unable to authorize the user: ";
    public static final String WITH_GROUP = " with group: ";
    public static final String AUDIT_SUCCESS_SUCCESS_AUTHENTICATING_USER =
            "AUDIT::SUCCESS: Success authenticating user: ";

    /**
     * The private key.
     * It is protected by synchronization on the instance.
     */
    private PrivateKey privateKey_ = null;

    /**
     * The logger
     */
    private final Logger logger_ = LogManager.getLogger(AuthProvider.class);

    /**
     * The JSON builder.
     */
    private static final Gson GSON = new GsonBuilder().create();

    /**
     * The init claim.
     */
    private static final String CLAIM = "initStatus";

    /**
     * The init status.
     */
    private static final String INIT_SUBJECT = "admin";

    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    private final @Nonnull KeyValueStore keyValueStore_;

    /**
     * Locks for write operations on target storages.
     */
    private final Object storeLock_ = new Object();

    /**
     * The AD provider.
     */
    private final @Nonnull SsoUtil ssoUtil;

    /**
     * The transient SSO group-based users.
     */
    private final Map<String, String> ssoUsersToUuid_ =
            Collections.synchronizedMap(new HashMap<>());

    /**
     * The identity generator prefix
     */
    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix_;

    /**
     * Constructs the KV store.
     *
     * @param keyValueStore The underlying store backend.
     */
    public AuthProvider(@Nonnull final KeyValueStore keyValueStore) {
        keyValueStore_ = Objects.requireNonNull(keyValueStore);
        ssoUtil = new SsoUtil();
        IdentityGenerator.initPrefix(identityGeneratorPrefix_);
    }

    /**
     * Generates an AUTH token for the specified user.
     *
     * @param userName The user name.
     * @param uuid     The UUID.
     * @param roles    The role names.
     * @return The generated JWT token.
     */
    private @Nonnull JWTAuthorizationToken generateToken(final @Nonnull String userName,
                                                         final @Nonnull String uuid,
                                                         final @Nonnull List<String> roles) {
        final PrivateKey privateKey = getEncryptionKeyForVMTurboInstance();
        String compact = Jwts.builder()
                             .setSubject(userName)
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, roles)
                             .claim(UUID_CLAIM, uuid)
                             .compressWith(CompressionCodecs.GZIP)
                             .signWith(SignatureAlgorithm.ES256, privateKey)
                             .compact();
        return new JWTAuthorizationToken(compact);
    }


    /**
     * Generates an AUTH token for the specified user with remote IP address.
     *
     * @param userName  The user name.
     * @param uuid      The UUID.
     * @param roles     The role names.
     * @param ipAddress The remote IP address.
     * @return The generated JWT token.
     */
    private @Nonnull JWTAuthorizationToken generateToken(final @Nonnull String userName,
                                                         final @Nonnull String uuid,
                                                         final @Nonnull List<String> roles,
                                                         final @Nonnull String ipAddress) {
        final PrivateKey privateKey = getEncryptionKeyForVMTurboInstance();
        String compact = Jwts.builder()
                .setSubject(userName)
                .claim(IP_ADDRESS_CLAIM, ipAddress)
                .claim(IAuthorizationVerifier.ROLE_CLAIM, roles)
                .claim(UUID_CLAIM, uuid)
                .compressWith(CompressionCodecs.GZIP)
                .signWith(SignatureAlgorithm.ES256, privateKey)
                .compact();
        return new JWTAuthorizationToken(compact);
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

        final String location = System.getProperty(VMT_PRIVATE_KEY_DIR_PARAM, VMT_PRIVATE_KEY_DIR);
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
     * Retrieves the User object in JSON form from the KV store.
     *
     * @param provider The provider.
     * @param userName The user name.
     * @return The key for the User object in JSON form.
     */
    private String composeUserInfoKey(final @Nonnull AuthUserDTO.PROVIDER provider,
                                      final @Nonnull String userName) {
        return PREFIX + provider.name() + "/" + userName.toUpperCase();
    }

    /**
     * Retrieves the Group object in JSON form from the KV store.
     *
     * @param group The user name.
     * @return The key for the Group object in JSON form.
     */
    private String composeGroupInfoKey(final @Nonnull String group) {
        return PREFIX_GROUP + group.toUpperCase();
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
     * Removes the value for the key in the KV store.
     *
     * @param key The key.
     */
    private void removeKVKey(final @Nonnull String key) {
        synchronized (storeLock_) {
            keyValueStore_.remove(key);
        }
    }

    /**
     * Checks whether the admin user has been instantiated.
     * When the XL first starts up, there is no user defined in the XL.
     * The first required step is to instantiate an admin user.
     * This method checks whether an admin user has been instantiated in XL.
     *
     * @return {@code true} iff the admin user has been instantiated.
     * @throws SecurityException In case of an error performing the check.
     */
    public boolean checkAdminInit() throws SecurityException {
        // Make sure we have initialized the site secret.
        getEncryptionKeyForVMTurboInstance();
        // The file contains the flag that specifies whether admin user has been initialized.
        final String location = System.getProperty(VMT_PRIVATE_KEY_DIR_PARAM, VMT_PRIVATE_KEY_DIR);
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
                final Jws<Claims> claims =
                        Jwts.parser().setSigningKey(JWTKeyCodec.decodePublicKey(key.get()))
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
     * Initializes an admin user.
     * When the XL first starts up, there is no user defined in the XL.
     * The first required step is to instantiate an admin user.
     * This should only be called once. If it is called more than once, this method will return
     * {@code false}.
     *
     * @param userName The user name.
     * @param password The password.
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of an error initializing the admin user.
     */
    public boolean initAdmin(final @Nonnull String userName,
                             final @Nonnull String password)
            throws SecurityException {
        // Make sure we have initialized the site secret.
        getEncryptionKeyForVMTurboInstance();
        final String location = System.getProperty(VMT_PRIVATE_KEY_DIR_PARAM, VMT_PRIVATE_KEY_DIR);
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

            // Persist the intialization status.
            Files.write(encryptionFile,
                        CryptoFacility.encrypt(compact).getBytes(CHARSET_CRYPTO));
            return addImpl(AuthUserDTO.PROVIDER.LOCAL, userName, password,
                           ImmutableList.of("ADMINISTRATOR"));
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Authenticates an AD user.
     *
     * @param info     The user info.
     * @param password The password.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case of error authenticating AD user.
     */
    private @Nonnull JWTAuthorizationToken authenticateADUser(final @Nonnull UserInfo info,
                                                              final @Nonnull String password)
            throws AuthenticationException {
        reloadSSOConfiguration();
        try {
            ssoUtil.authenticateADUser(info.userName, password);
        } catch (SecurityException e) {
            throw new AuthenticationException(e);
        }
        logger_.info("AUDIT::SUCCESS: Success authenticating user: " + info.userName);
        return generateToken(info.userName, info.uuid, info.roles);
    }


    /**
     * Authorize an SAML user.
     *
     * @param info     The user info.
     * @param ipaddress The user request IP address.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case of error authenticating AD user.
     */
    private @Nonnull JWTAuthorizationToken authorizeSAMLUser(final @Nonnull UserInfo info,
                                                             final @Nonnull String ipaddress) {
        logger_.info("AUDIT::SUCCESS: Success authorizing user: " + info.userName);
        return generateToken(info.userName, info.uuid, info.roles, ipaddress);
    }

    /**
     * Authenticates the user by group membership.
     *
     * @param userName The user name.
     * @param password The password.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case we failed to authenticate user against SSO group.
     */
    private @Nonnull JWTAuthorizationToken authenticateADGroup(final @Nonnull String userName,
                                                               final @Nonnull String password)
            throws AuthenticationException {
        // Get the LDAP servers we can query.  If there are none, there's no point in going on.
        try {
            reloadSSOConfiguration();
            @Nonnull Collection<String> ldapServers = ssoUtil.findLDAPServersInWindowsDomain();
            // only perform LDAP authentication where ldap server(s) are avaliable
            if (!ldapServers.isEmpty()) {
                String role = ssoUtil.authenticateUserInGroup(userName, password, ldapServers);
                if (role != null) {
                    logger_.info("AUDIT::SUCCESS: Success authenticating user: " + userName);
                    String uuid = ssoUsersToUuid_.get(userName);
                    if (uuid == null) {
                        uuid = String.valueOf(IdentityGenerator.next());
                        ssoUsersToUuid_.put(userName, uuid);
                    }
                    return generateToken(userName, uuid, ImmutableList.of(role));
                }
                throw new AuthenticationException("Unable to authenticate the user " + userName);
            }
            throw new AuthenticationException("Unable to authenticate the user " + userName);
        } catch (SecurityException e) {
            // removed the exception stack to limit system information leakage
            throw new AuthenticationException("Unable to authenticate the user " + userName);
        }
    }


    /**
     * Authorize the SAML user by group membership.
     *
     * @param userName The user name.
     * @param groupName The group name.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case we failed to authenticate user against group.
     */
    private @Nonnull JWTAuthorizationToken authorizeSAMLGroup(final @Nonnull String userName,
                                                               final @Nonnull String groupName,
                                                               final @Nonnull String ipAddress     )
            throws AuthorizationException {
            reloadSSOConfiguration();
            return ssoUtil.authorizeSAMLUserInGroup(userName, groupName).map(role -> {
                        logger_.info(AUDIT_SUCCESS_SUCCESS_AUTHENTICATING_USER + userName);
                        String uuid = ssoUsersToUuid_.get(userName);
                        if (uuid == null) {
                            uuid = String.valueOf(IdentityGenerator.next());
                            ssoUsersToUuid_.put(userName, uuid);
                        }
                        return generateToken(userName, uuid, ImmutableList.of(role),ipAddress);
                    }
            ).orElseThrow(() -> new AuthorizationException(UNABLE_TO_AUTHORIZE_THE_USER
                    + userName + WITH_GROUP + groupName));
    }

    /**
     * Authenticates the user.
     *
     * @param userName The user name.
     * @param password The password.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case of error authenticating the user. We can get
     *                                 {@link SecurityException} in case of
     * @throws SecurityException       In case of an internal error while authenticating an user.
     */
    public @Nonnull JWTAuthorizationToken authenticate(final @Nonnull String userName,
                                                       final @Nonnull String password)
            throws AuthenticationException, SecurityException {
        // Try local users first.
        Optional<String> json = getKVValue(composeUserInfoKey(AuthUserDTO.PROVIDER.LOCAL,
                                                              userName));
        if (!json.isPresent()) {
            json = getKVValue(composeUserInfoKey(AuthUserDTO.PROVIDER.LDAP, userName));
        }

        if (json.isPresent()) {
            try {
                String jsonData = json.get();
                UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
                // Check the authentication.
                if (!info.unlocked) {
                    throw new AuthenticationException("AUDIT::NEGATIVE: Account is locked");
                }
                if (AuthUserDTO.PROVIDER.LOCAL.equals(info.provider)) {
                    if (!CryptoFacility.checkSecureHash(info.passwordHash, password)) {
                        throw new AuthenticationException("AUDIT::NEGATIVE: Hash mismatch");
                    }

                    logger_.info("AUDIT::SUCCESS: Success authenticating user: " + userName);
                    return generateToken(info.userName, info.uuid, info.roles);
                } else {
                    return authenticateADUser(info, password);
                }
            } catch (AuthenticationException e) {
                logger_.error("AUDIT::FAILURE:AUTH: Error authenticating user: " + userName);
                throw e;
            } catch (Exception e) {
                logger_.error("AUDIT::FAILURE:AUTH: Error authenticating user: " + userName);
                throw new SecurityException("Authentication failed", e);
            }
        }
        return authenticateADGroup(userName, password);
    }

    /**
     * Authenticates the user when IP address is available.
     *
     * @param userName The user name.
     * @param password The password.
     * @param ipAddress The user's IP address.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case of error authenticating the user. We can get
     *                                 {@link SecurityException} in case of
     * @throws SecurityException       In case of an internal error while authenticating an user.
     */
    public @Nonnull JWTAuthorizationToken authenticate(final @Nonnull String userName,
                                                       final @Nonnull String password,
                                                       final @Nonnull String ipAddress)
            throws AuthenticationException, SecurityException {
        // Try local users first.
        Optional<String> json = getKVValue(composeUserInfoKey(AuthUserDTO.PROVIDER.LOCAL,
                                                              userName));
        if (!json.isPresent()) {
            json = getKVValue(composeUserInfoKey(AuthUserDTO.PROVIDER.LDAP, userName));
        }

        if (json.isPresent()) {
            try {
                String jsonData = json.get();
                UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
                // Check the authentication.
                if (!info.unlocked) {
                    throw new AuthenticationException("AUDIT::NEGATIVE: Account is locked");
                }
                if (AuthUserDTO.PROVIDER.LOCAL.equals(info.provider)) {
                    if (!CryptoFacility.checkSecureHash(info.passwordHash, password)) {
                        // removed "Hash mismatch" to avoid leaking internal authentication algorithm
                        throw new AuthenticationException("AUDIT::NEGATIVE: " +
                                "The User Name or Password is Incorrect");
                    }

                    logger_.info("AUDIT::SUCCESS: Success authenticating user: " + userName);
                    return generateToken(info.userName, info.uuid, info.roles, ipAddress);
                } else {
                    return authenticateADUser(info, password);
                }
            } catch (AuthenticationException e) {
                logger_.error("AUDIT::FAILURE:AUTH: Error authenticating user: " + userName);
                throw e;
            } catch (Exception e) {
                logger_.error("AUDIT::FAILURE:AUTH: Error authenticating user: " + userName);
                throw new SecurityException("Authentication failed", e);
            }
        }
        return authenticateADGroup(userName, password);
    }


    /**
     * Authenticates the user when IP address is available.
     *
     * @param userName The user name.
     * @param groupName The password.
     * @param ipAddress The user's IP address.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthorizationException In case of error authenticating the user. We can get
     *                                 {@link SecurityException} in case of
     * @throws SecurityException       In case of an internal error while authorizing an user.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nonnull JWTAuthorizationToken authorize(final @Nonnull String userName,
                                                       final @Nonnull String groupName,
                                                       final @Nonnull String ipAddress)
            throws SecurityException, AuthorizationException {
        return authorizeSAMLGroup(userName, groupName, ipAddress);
    }


    /**
     * Authorize SAML user.
     *
     * @param userName user name
     * @param ipAddress user IP address
     * @return user JWT token
     * @throws AuthorizationException if SAML user doesn't exist
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nonnull JWTAuthorizationToken authorize(final @Nonnull String userName,
                                                    final @Nonnull String ipAddress)
            throws AuthorizationException {
        // Try local users first.
        Optional<String> json = getKVValue(composeUserInfoKey(PROVIDER.LDAP, userName));

        if (json.isPresent()) {
            try {
                String jsonData = json.get();
                UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
                // Check the authentication.
                if (!info.unlocked) {
                    throw new AuthorizationException("AUDIT::NEGATIVE: Account is locked");
                }
                return authorizeSAMLUser(info, ipAddress);

            } catch (Exception e) {
                logger_.error("AUDIT::FAILURE:AUTH: Error authorizing user: " + userName);
                throw new AuthorizationException(e);
            }
        }
        throw new AuthorizationException("Authorization failed: " + userName);
    }

    /**
     * Adds the user.
     * Used by the {@link #initAdmin(String, String)} and
     * {@link #add(AuthUserDTO.PROVIDER, String, String, List)}.
     *
     * @param provider  The provider.
     * @param userName  The user name.
     * @param password  The password.
     * @param roleNames The roles.
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of an error parsing or decrypting the data.
     */
    private boolean addImpl(final @Nonnull AuthUserDTO.PROVIDER provider,
                            final @Nonnull String userName,
                            final @Nonnull String password,
                            final @Nonnull List<String> roleNames)
            throws SecurityException {
        Optional<String> json = getKVValue(composeUserInfoKey(provider, userName));
        if (json.isPresent()) {
            logger_.error("AUDIT::FAILURE:EXISTING: Error adding existing user: " + userName);
            return false;
        }

        try {
            UserInfo info = new UserInfo();
            info.provider = provider;
            info.userName = userName;
            if (AuthUserDTO.PROVIDER.LOCAL.equals(provider)) {
                info.passwordHash = CryptoFacility.secureHash(password);
            }
            info.uuid = String.valueOf(IdentityGenerator.next());
            info.unlocked = true;
            info.roles = roleNames;
            putKVValue(composeUserInfoKey(provider, userName), GSON.toJson(info));
            logger_.info("AUDIT::SUCCESS: Success adding user: " + userName);
            return true;
        } catch (Exception e) {
            logger_.error("Error adding user", e);
            logger_.error("AUDIT::FAILURE:AUTH: Error adding user: " + userName);
            return false;
        }
    }

    /**
     * Lists all defined users.
     *
     * @return The list of all users.
     * @throws SecurityException In case of an error listing users.
     */
    public @Nonnull List<AuthUserDTO> list() throws SecurityException {
        Map<String, String> users;
        synchronized (storeLock_) {
            users = keyValueStore_.getByPrefix(PREFIX);
        }

        List<AuthUserDTO> list = new ArrayList<>();
        for (String jsonData : users.values()) {
            UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
            AuthUserDTO dto = new AuthUserDTO(info.provider, info.userName, null, null, info.uuid, null,
                                              info.roles);
            list.add(dto);
        }
        return list;
    }

    /**
     * Adds the user.
     *
     * @param provider  The provider: Local or AD.
     * @param userName  The user name.
     * @param password  The password.
     * @param roleNames The roles.
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of an error adding the user.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public boolean add(final @Nonnull AuthUserDTO.PROVIDER provider,
                       final @Nonnull String userName,
                       final @Nonnull String password,
                       final @Nonnull List<String> roleNames)
            throws SecurityException {
        return addImpl(provider, userName, password, roleNames);
    }

    /**
     * Sets the password for a user defined in the LOCAL provider.
     * Will verify the existing password if supplied (UI doesn't right now).
     *
     * @param userName    The user name.
     * @param password    The password.
     * @param passwordNew The password. Could be missing.
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of empty new password or verifying the
     *                           existing password if supplied.
     */
    public boolean setPassword(final @Nonnull String userName,
                               final @Nullable String password,
                               final @Nonnull String passwordNew)
            throws SecurityException {
        Optional<String> json = getKVValue(composeUserInfoKey(AuthUserDTO.PROVIDER.LOCAL,
                                                              userName));
        if (!json.isPresent()) {
            logger_.error("AUDIT::FAILURE:UNKNOWN: Error modifying unknown user: " +
                          userName);
            return false;
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (!userName.equals(auth.getPrincipal())) {
            boolean administrator = false;
            for (GrantedAuthority ga : auth.getAuthorities()) {
                if ("ROLE_ADMINISTRATOR".equals(ga.getAuthority())) {
                    administrator = true;
                    break;
                }
            }

            if (!administrator) {
                logger_.error("AUDIT::FAILURE:AUTH: Not owner or administrator: " + userName);
                throw new SecurityException("Not owner or administrator");
            }
        }

        try {
            String jsonData = json.get();
            UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
            // Check the authentication.
            // We add this bypass, since MT does not provide the existing password.
            if (password != null && !CryptoFacility.checkSecureHash(info.passwordHash, password)) {
                throw new SecurityException("AUDIT::NEGATIVE: Password mismatch");
            }
            // Update password if necessary.
            if (passwordNew.isEmpty()) {
                throw new SecurityException("Empty new password");
            }
            info.passwordHash = CryptoFacility.secureHash(passwordNew);
            // Update KV store.
            putKVValue(composeUserInfoKey(AuthUserDTO.PROVIDER.LOCAL, userName), GSON.toJson(info));
            logger_.info("AUDIT::SUCCESS: Success modifying user: " + userName);
            return true;
        } catch (SecurityException e) {
            logger_.error("AUDIT::FAILURE:AUTH: Error setting password for " + userName);
            throw e;
        }
    }

    /**
     * Replaces user roles.
     *
     * @param provider  The provider.
     * @param userName  The user name.
     * @param roleNames The roles.
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of an error replacing user's roles.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public boolean setRoles(final @Nonnull AuthUserDTO.PROVIDER provider,
                            final @Nonnull String userName,
                            final @Nonnull List<String> roleNames)
            throws SecurityException {

        Optional<String> json = getKVValue(composeUserInfoKey(provider, userName));
        if (!json.isPresent()) {
            logger_.error("AUDIT::FAILURE:UNKNOWN: Error modifying unknown user: " +
                          userName);
            return false;
        }

        try {
            String jsonData = json.get();
            UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
            info.roles = roleNames;
            // Update KV store.
            putKVValue(composeUserInfoKey(provider, userName), GSON.toJson(info));
            logger_.info("AUDIT::SUCCESS: Success modifying user: " + userName);
            return true;
        } catch (Exception e) {
            logger_.error("AUDIT::FAILURE:AUTH: Error modifying user: " + userName);
            return false;
        }
    }

    /**
     * Removes the user.
     *
     * @param uuid The user's UUID or name.
     * @return {@code true} iff successful.
     * @throws SecurityException In case of an error deleting the user.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public boolean remove(final @Nonnull String uuid) throws SecurityException {
        // Look for the correct user.
        Map<String, String> users;
        synchronized (storeLock_) {
            users = keyValueStore_.getByPrefix(PREFIX);
        }

        UserInfo infoFound = null;
        for (String jsonData : users.values()) {
            UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
            if (info.uuid.equals(uuid) || info.userName.equals(uuid)) {
                infoFound = info;
                break;
            }
        }

        if (infoFound == null) {
            logger_.error("AUDIT::FAILURE:UNKNOWN: Error removing unknown user: " +
                          uuid);
            return false;
        }

        // Don't allow removing the last local admin user.
        if (isRemovingLastLocalAdminUser(infoFound, users)) {
            logger_.error("AUDIT::Don't allow to remove last local admin user: " + uuid);
            return false;
        }

        try {
            removeKVKey(composeUserInfoKey(infoFound.provider, infoFound.userName));
            logger_.info("AUDIT::SUCCESS: Success removing user: " + uuid);
            return true;
        } catch (Exception e) {
            logger_.error("AUDIT::FAILURE:AUTH: Error removing user: " + uuid);
            return false;
        }
    }

    /**
     * Locks the user.
     *
     * @param dto The user DTO.
     * @return {@code true} iff successful.
     * @throws SecurityException In case of an error locking the user.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public boolean lock(final @Nonnull AuthUserDTO dto) throws SecurityException {
        return setUserLockStatus(dto, true);
    }

    /**
     * Unlocks the user.
     *
     * @param dto The user DTO.
     * @return {@code true} iff successful.
     * @throws SecurityException In case of an error unlocking the user.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public boolean unlock(final @Nonnull AuthUserDTO dto) throws SecurityException {
        return setUserLockStatus(dto, false);
    }

    /**
     * Locks/Unlocks the user.
     * In case we are already locked/unlocked (The lockStatus is true/false), it is a NO-OP.
     *
     * @param dto        The user DTO.
     * @param lockStatus The lock/unlock flag. {@code true} to lock, {@code false} to unlock.
     * @return {@code true} iff successful.
     * @throws SecurityException In case of an error parsing or decrypting the data.
     */
    private boolean setUserLockStatus(final @Nonnull AuthUserDTO dto,
                                      final boolean lockStatus)
            throws SecurityException {
        AuthUserDTO.PROVIDER provider = dto.getProvider();
        String userName = dto.getUser();
        Optional<String> json = getKVValue(composeUserInfoKey(provider, userName));
        final String msgForFlag = lockStatus ? "lock" : "unlock";
        if (!json.isPresent()) {
            logger_.error("AUDIT::FAILURE:UNKNOWN: Error " + msgForFlag + "ing unknown user: " +
                          userName);
            return false;
        }

        try {
            String jsonData = json.get();
            UserInfo info = GSON.fromJson(jsonData, UserInfo.class);

            // In case we are already locked/unlocked (The lockStatus is true/false), it is a NO-OP.
            if (info.unlocked == !lockStatus) {
                logger_.info("AUDIT::SUCCESS: User already " + msgForFlag + "ed: " + userName);
            } else {
                info.unlocked = !lockStatus;
                // Update KV store.
                putKVValue(composeUserInfoKey(provider, userName), GSON.toJson(info));
            }

            logger_.info("AUDIT::SUCCESS: Success " + msgForFlag + "ing user: " + userName);
            return true;
        } catch (Exception e) {
            logger_.error("AUDIT::FAILURE:AUTH: Error " + msgForFlag + "ing user: " + userName);
            return false;
        }
    }

    // Active Directory

    /**
     * Reloads the AD info.
     *
     * @throws SecurityException In case of an error loading the AD info.
     */
    private void reloadSSOConfiguration() throws SecurityException {
        Optional<String> json = getKVValue(PREFIX_AD);
        boolean isAdAvailable = json.isPresent();
        List <GroupDTO> groups = getGroups();
        if (!isAdAvailable && groups.size() == 0) {
            return;
        }
        // only reset when it's required.
        ssoUtil.reset();
        // always load group, since it's shared by AD and SAML
        groups.forEach(group -> {
            ssoUtil.putGroup(group.getDisplayName(), group.getRoleName());
        });
        if (isAdAvailable) {
            ActiveDirectoryDTO result = GSON.fromJson(json.get(), ActiveDirectoryDTO.class);

            ssoUtil.setDomainName(result.getDomainName());
            ssoUtil.setSecureLoginProvider(result.isSecure());
            ssoUtil.setLoginProviderURI(result.getLoginProviderURI());
        }
    }

    /**
     * Returns the list of Active Directory DTOs which represent list of AD servers that can
     * be located using the data in the {@link }ActiveDirectoryDTO}s as well as the list of AD
     * groups associated with each of these DTOs.
     *
     * @return The list of Active Directory DTOs.
     * @throws SecurityException In case we couldn't retrieve the list of the active directories.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nonnull
    List<ActiveDirectoryDTO> getActiveDirectories() throws SecurityException {
        Optional<String> json = getKVValue(PREFIX_AD);
        if (!json.isPresent()) {
            return Collections.emptyList();
        }

        try {
            ActiveDirectoryDTO result = GSON.fromJson(json.get(), ActiveDirectoryDTO.class);
            return ImmutableList.of(result);
        } catch (Exception e) {
            throw new SecurityException("Error retrieving active directories");
        }
    }

    /**
     * Create the Active Directory object Active Directory DTOs which represent list of AD
     * servers that can
     * be located using the data in the {@link }ActiveDirectoryDTO}s.
     *
     * @param inputDTO The Active Directory representation.
     * @return The Active Directory DTO.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nonnull ActiveDirectoryDTO createActiveDirectory(final @Nonnull ActiveDirectoryDTO inputDTO) {
        String domain = inputDTO.getDomainName() == null ? "" : inputDTO.getDomainName();
        String url = inputDTO.getLoginProviderURI() == null ? "" : inputDTO.getLoginProviderURI();
        // In case the provider URL is empty, we use domain for AD servers lookup.
        if (!url.isEmpty()) {
            if (inputDTO.isSecure()) {
                url = "ldaps://" + url + ":636/";
            } else {
                url = "ldap://" + url + ":389/";
            }
        }
        // We set a non-null array to groups here, so that later we can avoid the null checks.
        ActiveDirectoryDTO result = new ActiveDirectoryDTO(domain, url, inputDTO.isSecure(),
                                                           new ArrayList<>());
        putKVValue(PREFIX_AD, GSON.toJson(result));
        ssoUtil.setDomainName(domain);
        ssoUtil.setSecureLoginProvider(inputDTO.isSecure());
        ssoUtil.setLoginProviderURI(url);

        // We enforce the original semantics of ActiveDirectoryDTO having groups == null to
        // designate the empty list.
        result.setGroups(null);
        return result;
    }

    /**
     * Returns the list of SSO group objects.
     *
     * @return The list of SSO group objects.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nonnull List<GroupDTO> getGroups() {
        Map<String, String> groups;
        synchronized (storeLock_) {
            groups = keyValueStore_.getByPrefix(PREFIX_GROUP);
        }

        List<GroupDTO> list = new ArrayList<>();
        for (String jsonData : groups.values()) {
            GroupDTO info = GSON.fromJson(jsonData, GroupDTO.class);
            list.add(info);
        }
        return list;
    }

    /**
     * Creates an Active Directory group.
     *
     * @param adGroupInputDto The description of an SSO group to be created.
     * @return The {@link GroupDTO} object.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nullable
    GroupDTO createGroup(
            final @Nonnull GroupDTO adGroupInputDto) {
        Optional<String> json = getKVValue(composeGroupInfoKey(adGroupInputDto.getDisplayName()));
        try {

            if (!json.isPresent()) {
                ssoUtil.putGroup(adGroupInputDto.getDisplayName(), adGroupInputDto.getRoleName());
                GroupDTO g =
                        new GroupDTO(adGroupInputDto.getDisplayName(),
                                                    adGroupInputDto.getType(),
                                                    adGroupInputDto.getRoleName());
                putKVValue(composeGroupInfoKey(adGroupInputDto.getDisplayName()), GSON.toJson(g));
                return g;
            } else {
                GroupDTO g = GSON.fromJson(json.get(), GroupDTO.class);
                // We are changing it.
                g.setRoleName(adGroupInputDto.getRoleName());
                putKVValue(composeGroupInfoKey(adGroupInputDto.getDisplayName()), GSON.toJson(g));
                return g;
            }
        } catch (Exception e) {
            throw new SecurityException("Error creating or changing active directory group");
        }
    }

    /**
     * Changes the Active Directory group.
     *
     * @param adGroupInputDto The Active Directory group creation request.
     * @return The {@link GroupDTO} indicating success.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nullable
    GroupDTO changeActiveDirectoryGroup(final @Nonnull GroupDTO adGroupInputDto) {
        // This method is not invoked, as the API layer method that should invoke it, does not
        // get invoked by the UI.
        return null;
    }

    /**
     * Deletes the group.
     *
     * @param groupName The group name.
     * @return {@code true} iff the group existed before this call.
     */
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    public @Nonnull Boolean deleteGroup(final @Nonnull String groupName) {
        Optional<String> json = getKVValue(composeGroupInfoKey(groupName));
        if (!json.isPresent()) {
            throw new SecurityException("Error retrieving external group.");
        }

        try {
            removeKVKey(composeGroupInfoKey(groupName));
            ssoUtil.deleteGroup(groupName);
            return true;
        } catch (Exception e) {
            throw new SecurityException("Error retrieving group");
        }
    }

    /**
     * The internal user information structure.
     * <p> <tt>uuid discussion</tt> </p>
     * Current approach: <p>
     * From Gary Zeng:
     * uuid is String type, since according to RFC 4122 (https://tools.ietf.org/html/rfc4122),
     * it includes non-numerical characters.
     * e.g. 0f4f8d29-577d-456f-bfc7-71dbeacf5300 (version 4).
     * Should we use UUID as the unique identifier for user object?
     * In legacy, we used uuid as unique identifier for user, e.g. in login.config.topology.
     * uuid="_4T_7kwY-Ed-WUKbEYSVIDw" name="administrator"
     * For XL, I currently don’t see a strong case of changing it.
     * Should AO and other components use user’s UUID as unique identifier?
     * if #2 is valid, probably other components should use UUID too.
     * </p>>
     *
     * Alternatives <p>
     * 1. replace uuid (String) with oid (long).</p>
     * From Mark Laff:
     * In my opinion, we should not care that legacy calls the field in the API a uuid –
     * we should create and manage OIDs within XL, and return them in string from where the UX
     * expects a UUID. The UX never parses the string; that would be really bad. So it doesn’t
     * matter that there are no letters or ‘-‘ in a uuid, just that it is unique. We will,
     * over time, migrate the field name in the API from ‘uuid’ to ‘oid’. We just need to insist
     * that the UX never-never-never uses what it thinks is a know ‘uuid’ in a REST API call to XL.
     * As an extra added interesting item – ‘uuid’s in legacy are not guaranteed unique.
     * For example, I’m pretty sure that if you clone a VM in VCenter you will end up with the
     * same UUID (or it may have to do with storages or networks). The only way to guarantee
     * uniqueness is to use our OID generator, which is already in use in Legacy in some places.
     * So the REST API uses the wrong names, but I think going with longs is still the right approach.
     * 2. replace uuid (String) to uuid (long).<p>
     * Similar to the #1, just keeping the variable 'uuid' instead of changing it to oid.</p>
     */
    @VisibleForTesting
    static class UserInfo {
        AuthUserDTO.PROVIDER provider;

        String userName;

        String uuid;

        String passwordHash;

        List<String> roles;

        boolean unlocked;
    }

    /**
     * Test if there is only one local admin user.
     *
     * @param users the list of user
     * @return true if there is only one local user has ADMINISTRATOR role.
     */
    private boolean isRemovingLastLocalAdminUser(UserInfo userInfo, final Map<String, String> users) {
        if (userInfo.roles.contains(UserRole.ADMINISTRATOR.name()) // the user the admin user
                && userInfo.provider.equals(PROVIDER.LOCAL)) { // the provider is local
            List<UserInfo> userInfoList = new ArrayList<>();
            for (String jsonData : users.values()) {
                UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
                userInfoList.add(info);
            }
            long administratorCount = userInfoList.stream()
                    .filter(user -> user.provider.equals(PROVIDER.LOCAL)
                            && user.roles.contains(UserRole.ADMINISTRATOR.name()))
                    .count();
            return administratorCount == 1;
        }
        return false;
    }
}
