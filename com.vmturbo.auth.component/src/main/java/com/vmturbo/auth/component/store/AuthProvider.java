package com.vmturbo.auth.component.store;

import static com.vmturbo.auth.api.authorization.IAuthorizationVerifier.PROVIDER_CLAIM;
import static com.vmturbo.auth.api.authorization.IAuthorizationVerifier.SCOPE_CLAIM;
import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.IP_ADDRESS_CLAIM;
import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.UUID_CLAIM;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_SECURITY_GROUPS_SET;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.api.enums.UserRole;
import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authentication.AuthenticationException;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.component.store.sso.SsoUtil;
import com.vmturbo.auth.component.widgetset.WidgetsetDbStore;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

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
     * KV prefix for external group users.
     */
    private static final String PREFIX_GROUP_USERS = "groupusers/";

    /**
     * The keystore data file name.
     */
    private static final String VMT_PRIVATE_KEY_FILE = "vmt_helper_kv.inout";

    /**
     * The admin initialization file name.
     */
    private static final String VMT_INIT_KEY_FILE = "vmt_helper_init.inout";

    /**
     * The charset for the passwords.
     */
    private static final String CHARSET_CRYPTO = "UTF-8";

    private static final String UNABLE_TO_AUTHORIZE_THE_USER = "Unable to authorize the user: ";
    private static final String WITH_GROUP = " with group: ";
    private static final String AUDIT_SUCCESS_SUCCESS_AUTHENTICATING_USER =
            "AUDIT::SUCCESS: Success authenticating user: ";

    /**
     * The private key.
     * It is protected by synchronization on the instance.
     */
    private PrivateKey privateKey_ = null;

    private final Logger logger_ = LogManager.getLogger(AuthProvider.class);
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
     * The identity generator prefix
     */
    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix_;

    private final Supplier<String> keyValueDir;

    /**
     * We may sometimes call the group service to verify scope groups.
     */
    private final Optional<GroupServiceBlockingStub> groupServiceClient;

    /**
     * Store for managing widgetsets belonging to a user.
     */
    private final WidgetsetDbStore widgetsetDbStore;

    /**
     * Constructs the KV store -- without support for validating shared groups.
     *
     * @param keyValueStore The underlying store backend.
     * @param keyValueDir Function to provide the directory to store private key value data.
     */
    @VisibleForTesting
    public AuthProvider(@Nonnull final KeyValueStore keyValueStore,
                        @Nonnull final Supplier<String> keyValueDir) {
        this(keyValueStore, null, keyValueDir, null);
    }

    /**
     * Constructs the KV store.
     *
     * @param keyValueStore The underlying store backend.
     * @param groupServiceClient gRPC client to access the group service.
     * @param keyValueDir Function to provide the directory to store private key value data.
     * @param widgetsetDbStore The store for managing widgetsets.
     */
    public AuthProvider(@Nonnull final KeyValueStore keyValueStore,
                        @Nullable final GroupServiceBlockingStub groupServiceClient,
                        @Nonnull final Supplier<String> keyValueDir,
                        @Nullable final WidgetsetDbStore widgetsetDbStore) {
        keyValueStore_ = Objects.requireNonNull(keyValueStore);
        ssoUtil = new SsoUtil();
        this.keyValueDir = keyValueDir;
        IdentityGenerator.initPrefix(identityGeneratorPrefix_);
        this.groupServiceClient = Optional.ofNullable(groupServiceClient);
        this.widgetsetDbStore = widgetsetDbStore;
    }

    /**
     * Generates an AUTH token for the specified user.
     *
     * @param userName The user name.
     * @param uuid     The UUID.
     * @param roles    The role names.
     * @param scopeGroups the groups in the scope to which the user has access
     * @param provider The login provider.
     * @return The generated JWT token.
     */
    private @Nonnull JWTAuthorizationToken generateToken(final @Nonnull String userName,
                                                         final @Nonnull String uuid,
                                                         final @Nonnull List<String> roles,
                                                         final @Nullable List<Long> scopeGroups,
                                                         final @Nonnull AuthUserDTO.PROVIDER provider) {
        final PrivateKey privateKey = getEncryptionKeyForVMTurboInstance();
        JwtBuilder jwtBuilder = Jwts.builder()
                             .setSubject(userName)
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, roles)
                             .claim(UUID_CLAIM, uuid)
                             .claim(PROVIDER_CLAIM, provider)
                             .compressWith(CompressionCodecs.GZIP)
                             .signWith(SignatureAlgorithm.ES256, privateKey);
        // any scopes, does the user have?
        if (CollectionUtils.isNotEmpty(scopeGroups)) {
            jwtBuilder.claim(SCOPE_CLAIM, scopeGroups);
        }
        String compact = jwtBuilder.compact();
        return new JWTAuthorizationToken(compact);
    }


    /**
     * Generates an AUTH token for the specified user with remote IP address.
     *
     * @param userName  The user name.
     * @param uuid      The UUID.
     * @param roles     The role names.
     * @param scopeGroups The groups in the scope to which the user has access
     * @param ipAddress The remote IP address.
     * @param provider The login provider.
     * @return The generated JWT token.
     */
    private @Nonnull JWTAuthorizationToken generateToken(final @Nonnull String userName,
                                                         final @Nonnull String uuid,
                                                         final @Nonnull List<String> roles,
                                                         final @Nullable List<Long> scopeGroups,
                                                         final @Nonnull String ipAddress,
                                                         final @Nonnull AuthUserDTO.PROVIDER provider) {
        final PrivateKey privateKey = getEncryptionKeyForVMTurboInstance();
        JwtBuilder jwtBuilder = Jwts.builder()
                .setSubject(userName)
                .claim(IP_ADDRESS_CLAIM, ipAddress)
                .claim(IAuthorizationVerifier.ROLE_CLAIM, roles)
                .claim(UUID_CLAIM, uuid)
                .claim(PROVIDER_CLAIM, provider)
                .compressWith(CompressionCodecs.GZIP)
                .signWith(SignatureAlgorithm.ES256, privateKey);
        // any scopes, does the user have?
        if (CollectionUtils.isNotEmpty(scopeGroups)) {
            jwtBuilder.claim(SCOPE_CLAIM, scopeGroups);
        }
        String compact = jwtBuilder.compact();

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
     * Retrieves the External Group object in JSON form from the KV store.
     *
     * @param externalGroup The user name.
     * @return The key for the External Group object in JSON form.
     */
    private String composeExternalGroupInfoKey(final @Nonnull String externalGroup) {
        return PREFIX_GROUP + externalGroup.toUpperCase();
    }

    /**
     * Compose the key path for the given user in external group, which will look like:
     * "groupusers/{groupName}/{userName}".
     *
     * @param externalGroup name of the external group
     * @param userName name of user in the given external group
     * @return key for the user in external group
     */
    private String composeExternalGroupUserInfoKey(final @Nonnull String externalGroup,
                                                   final @Nonnull String userName) {
        return PREFIX_GROUP_USERS + externalGroup.toUpperCase() + "/" + userName.toUpperCase();
    }

    /**
     * Compose the key path for the external group which keeps users belonging to the group, which
     * will look like: "groupusers/{groupName}/".
     *
     * @param externalGroup name of the external group
     * @return key for the external group where all belonging users are kept
     */
    private String composeExternalGroupUsersInfoKey(final @Nonnull String externalGroup) {
        return PREFIX_GROUP_USERS + externalGroup.toUpperCase() + "/";
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
     * It's similar to: consul kv delete key
     *
     * @param key The key.
     */
    private void removeKVKey(final @Nonnull String key) {
        synchronized (storeLock_) {
            keyValueStore_.removeKey(key);
        }
    }

    /**
     * Delete keys start with prefix from the key value store. No effect if the key is absent.
     * It's similar to: consul kv delete -recurse prefix.
     *
     * @param prefix The prefix in kv store.
     */
    private void removeKVKeysWithPrefix(final @Nonnull String prefix) {
        synchronized (storeLock_) {
            keyValueStore_.removeKeysWithPrefix(prefix);
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
     * Initializes an admin user and creates predefined external groups for all roles in XL.
     * When the XL first starts up, there is no user defined in the XL.
     * The first required step is to instantiate an admin user and creates predefined external groups for all roles in XL.
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
            Files.write(encryptionFile,
                        CryptoFacility.encrypt(compact).getBytes(CHARSET_CRYPTO));
            try {
                String adminUser = addImpl(AuthUserDTO.PROVIDER.LOCAL, userName, password,
                    ImmutableList.of(ADMINISTRATOR), null);
                createPredefinedExternalGroups();
                return !adminUser.isEmpty();
            }  catch (IllegalArgumentException e) {
                return false;
            }
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }

    private void createPredefinedExternalGroups() {
        PREDEFINED_SECURITY_GROUPS_SET.forEach(securityGroup -> addSecurityGroupImpl(securityGroup));
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
            logger_.warn("AUDIT::FAILURE:AUTH: Failed authentication for user: " + info.userName);
            throw new AuthenticationException(e);
        }
        logger_.info("AUDIT::SUCCESS: Success authenticating user: " + info.userName);
        return generateToken(info.userName, info.uuid, info.roles, info.scopeGroups, info.provider);
    }


    /**
     * Authorize an SAML user.
     *
     * @param info     The user info.
     * @param ipaddress The user request IP address.
     * @return The JWTAuthorizationToken if successful.
     */
    private @Nonnull JWTAuthorizationToken authorizeSAMLUser(final @Nonnull UserInfo info,
                                                             final @Nonnull String ipaddress) {
        logger_.info("AUDIT::SUCCESS: Success authorizing user: " + info.userName);
        return generateToken(info.userName, info.uuid, info.roles, info.scopeGroups, ipaddress,
                info.provider);
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
                SecurityGroupDTO userGroup = ssoUtil.authenticateUserInGroup(userName, password, ldapServers);
                if (userGroup != null) {
                    logger_.info("AUDIT::SUCCESS: Success authenticating user: " + userName);
                    // persist user in external group if it's not added before
                    final String uuid = addExternalGroupUser(userGroup.getDisplayName(), userName);
                    return generateToken(userName, uuid, ImmutableList.of(userGroup.getRoleName()),
                            userGroup.getScopeGroups(), AuthUserDTO.PROVIDER.LDAP);
                }
            }
            throw new AuthenticationException("Unable to authenticate the user " + userName);
        } catch (SecurityException e) {
            // removed the exception stack to limit system information leakage
            throw new AuthenticationException("Unable to authenticate the user " + userName);
        }
    }

    /**
     * Authorize the SAML user by external group membership.
     *
     * @param userName The user name.
     * @param externalGroupName The external group name.
     * @param ipAddress the IP address that the user logged on from
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthorizationException In case we failed to authenticate user against external group.
     */
    private @Nonnull JWTAuthorizationToken authorizeSAMLGroup(
        final @Nonnull String userName,
        final @Nonnull String externalGroupName,
        final @Nonnull String ipAddress) throws AuthorizationException {

        reloadSSOConfiguration();
        return ssoUtil.authorizeSAMLUserInGroup(userName, externalGroupName).map(externalGroup -> {
                logger_.info(AUDIT_SUCCESS_SUCCESS_AUTHENTICATING_USER + userName);
                // persist user in external group if it's not added before
                final String uuid = addExternalGroupUser(externalGroupName, userName);
                return generateToken(userName, uuid, ImmutableList.of(externalGroup.getRoleName()),
                    externalGroup.getScopeGroups(), ipAddress, AuthUserDTO.PROVIDER.LDAP);
            }
        ).orElseThrow(() -> new AuthorizationException(UNABLE_TO_AUTHORIZE_THE_USER
            + userName + WITH_GROUP + externalGroupName));
    }

    /**
     * Authenticates the user.
     *
     * @param userName The user name.
     * @param password The password.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case of error authenticating the user.
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
                    logger_.warn("AUDIT::FAILURE:AUTH: Account is locked: " + userName);
                    throw new AuthenticationException("AUDIT::NEGATIVE: Account is locked");
                }
                if (AuthUserDTO.PROVIDER.LOCAL.equals(info.provider)) {
                    if (!HashAuthUtils.checkSecureHash(info.passwordHash, password)) {
                        logger_.warn("AUDIT::FAILURE:AUTH: Invalid credentials provided for user: " + userName);
                        throw new AuthenticationException("AUDIT::NEGATIVE: The User Name or Password is Incorrect");
                    }
                    logger_.info("AUDIT::SUCCESS: Success authenticating user: " + userName);
                    return generateToken(info.userName, info.uuid, info.roles, info.scopeGroups, info.provider);
                } else {
                    return authenticateADUser(info, password);
                }
            } catch (AuthenticationException e) {
                // prevent next catch clause from wrapping this exception; all paths here
                // log the authentication failure
                throw e;
            } catch (Exception e) {
                logger_.error("AUDIT::FAILURE:AUTH: Error authenticating user: " + userName);
                throw new SecurityException("Authentication failed", e);
            }
        }
        // use group-based authentication
        return authenticateADGroup(userName, password);
    }

    /**
     * Authenticates the user when IP address is available.
     *
     * @param userName The user name.
     * @param password The password.
     * @param ipAddress The user's IP address.
     * @return The JWTAuthorizationToken if successful.
     * @throws AuthenticationException In case of error authenticating the user.
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
                    logger_.warn("AUDIT::FAILURE:AUTH: Account is locked: " + userName);
                    throw new AuthenticationException("AUDIT::NEGATIVE: Account is locked");
                }
                if (AuthUserDTO.PROVIDER.LOCAL.equals(info.provider)) {
                    if (!HashAuthUtils.checkSecureHash(info.passwordHash, password)) {
                        // removed "Hash mismatch" to avoid leaking internal authentication algorithm
                        logger_.warn("AUDIT::FAILURE:AUTH: Invalid credentials provided for user: " + userName);
                        throw new AuthenticationException("AUDIT::NEGATIVE: " +
                                "The User Name or Password is Incorrect");
                    }

                    logger_.info("AUDIT::SUCCESS: Success authenticating user: " + userName);
                    return generateToken(info.userName, info.uuid, info.roles, info.scopeGroups,
                            ipAddress, info.provider);
                } else {
                    return authenticateADUser(info, password);
                }
            } catch (AuthenticationException e) {
                // prevent next clause from wrapping this exception. All paths here have already
                // loggged the authentication failure
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
     * @throws AuthorizationException In case of error authenticating the user.
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
                    logger_.warn("AUDIT::FAILURE:AUTH: Account is locked: " + userName);
                    throw new AuthorizationException("AUDIT::NEGATIVE: Account is locked");
                }
                return authorizeSAMLUser(info, ipAddress);
            } catch (AuthorizationException e) {
                // this is to prevent next catch clause from grabbing this one
                throw e;
            } catch (Exception e) {
                logger_.error("AUDIT::FAILURE:AUTH: Failure during authorization: " + userName);
                throw new AuthorizationException(e);
            }
        }
        throw new AuthorizationException("Authorization failed: " + userName);
    }

    /**
     * Validate that all of the groups are acceptable, if the user is a "shared" user.
     *
     * @param roles the list of roles the user has
     * @param scopeGroups the list of scope group ids to validate
     * @throws SecurityException if the system can't support scope validation
     * @throws IllegalArgumentException if the scope is invalid.
     */
    protected void validateScopeGroups(final @Nonnull List<String> roles, final @Nullable List<Long> scopeGroups)
        throws SecurityException, IllegalArgumentException {
        // validate the scope groups, if the user is in a "shared" role.
        if (CollectionUtils.isNotEmpty(scopeGroups) && UserScopeUtils.containsSharedRole(roles)) {
            if (! groupServiceClient.isPresent()) {
                logger_.warn("Cannot validate Shared user group -- validation is disabled.");
                throw new SecurityException("Cannot validate Shared user group -- validation is disabled.");
            }
            GroupServiceBlockingStub groupClient = groupServiceClient.get();
            Iterator<Grouping> groups = groupClient.getGroups(GetGroupsRequest.newBuilder()
                            .setGroupFilter(GroupFilter.newBuilder()
                                            .addAllId(scopeGroups).build())
                            .build());

            while (groups.hasNext()) {
                Collection<UIEntityType> entityTypes = GroupProtoUtil.getEntityTypes(groups.next());
                final Set<String> groupEntityTypes = entityTypes
                                .stream()
                                .map(UIEntityType::apiStr)
                                .collect(Collectors.toSet());
                if (entityTypes.size() == 0 || !UserScopeUtils.SHARED_USER_ENTITY_TYPES.containsAll(groupEntityTypes)) {
                    // invalid group assignment
                    throw new IllegalArgumentException("Shared users can only be scoped to groups of VM's or Applications!");
                }
            }
        }
    }

    /**
     * Adds the user.
     * Used by the {@link #initAdmin(String, String)} and
     * {@link #add(AuthUserDTO.PROVIDER, String, String, List, List)}.
     *
     * @param provider  The provider.
     * @param userName  The user name.
     * @param password  The password.
     * @param roleNames The roles.
     * @param scopeGroups The group id's in the user scope (if any).
     * @return The uuid of the user that was added, or empty string if it fails.
     * @throws SecurityException In case of an error parsing or decrypting the data.
     */
    private String addImpl(final @Nonnull AuthUserDTO.PROVIDER provider,
                            final @Nonnull String userName,
                            final @Nonnull String password,
                            final @Nonnull List<String> roleNames,
                            final @Nullable List<Long> scopeGroups)
            throws SecurityException, IllegalArgumentException {
        Optional<String> json = getKVValue(composeUserInfoKey(provider, userName));
        if (json.isPresent()) {
            throw new IllegalArgumentException("A user with name " + userName + " already exists.");
        }

        validateScopeGroups(roleNames, scopeGroups);

        try {
            UserInfo info = new UserInfo();
            info.provider = provider;
            info.userName = userName;
            if (AuthUserDTO.PROVIDER.LOCAL.equals(provider)) {
                info.passwordHash = HashAuthUtils.secureHash(password);
            }
            info.uuid = String.valueOf(IdentityGenerator.next());
            info.unlocked = true;
            // ensure role are upper case for any I/O operations, here is saving to Consul.
            info.roles = roleNames.stream().map(String::toUpperCase).collect(Collectors.toList());
            info.scopeGroups = scopeGroups;
            putKVValue(composeUserInfoKey(provider, userName), GSON.toJson(info));
            logger_.info("AUDIT::SUCCESS: Success adding user: " + userName);
            return info.uuid;
        } catch (Exception e) {
            logger_.error("Error adding user", e);
            logger_.error("AUDIT::FAILURE:AUTH: Error adding user: " + userName);
            return "";
        }
    }

    /**
     * Adds the user in the given external group. The scope or role for this user is not saved
     * since it should always come from the external group and it may change.
     *
     * @param externalGroup The external group name.
     * @param userName The user name.
     * @return The uuid of the user that was added, or empty string if it fails.
     * @throws SecurityException In case of an error parsing or decrypting the data.
     */
    private String addExternalGroupUser(@Nonnull String externalGroup,
                                        @Nonnull String userName) throws SecurityException {
        final String externalGroupUserInfoKey = composeExternalGroupUserInfoKey(externalGroup, userName);
        Optional<String> json = getKVValue(externalGroupUserInfoKey);
        if (json.isPresent()) {
            // user has already been added, return the uuid directly
            UserInfo info = GSON.fromJson(json.get(), UserInfo.class);
            return info.uuid;
        }

        try {
            UserInfo info = new UserInfo();
            // this is needed to show correct login provider for api "/me", otherwise it shows LOCAL
            info.provider = AuthUserDTO.PROVIDER.LDAP;
            info.userName = userName;
            info.uuid = String.valueOf(IdentityGenerator.next());
            info.unlocked = true;
            putKVValue(externalGroupUserInfoKey, GSON.toJson(info));
            logger_.info("AUDIT::SUCCESS: Success adding user {} in external group {} ", userName, externalGroup);
            return info.uuid;
        } catch (Exception e) {
            logger_.error("Error adding user {} in external group {} ", userName, externalGroup);
            logger_.error("AUDIT::FAILURE:AUTH: Error adding user {} in external group {} ", userName, externalGroup);
            return "";
        }
    }

    /**
     * Lists all defined users.
     *
     * @return The list of all users.
     * @throws SecurityException In case of an error listing users.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public @Nonnull List<AuthUserDTO> list() throws SecurityException {
        Map<String, String> users;
        synchronized (storeLock_) {
            users = keyValueStore_.getByPrefix(PREFIX);
        }
        // ensure role are upper case for any I/O operations, here is retrieving from Consul.
        return users.values().stream()
            .map(jsonData -> {
                UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
                return new AuthUserDTO(info.provider, info.userName, null, null,
                    info.uuid, null, info.roles.stream().map(String::toUpperCase)
                        .collect(Collectors.toList()), info.scopeGroups);
            })
            .filter(authUserDTO -> mayAlterUserWithRoles(authUserDTO.getRoles()))
            .collect(Collectors.toList());
    }

    /**
     * Find the username for the given user oid.
     *
     * @param userOid oid of the user to look for
     * @return optional username if it exists, or empty
     */
    public Optional<String> findUsername(long userOid) {
        final String userUuid = Long.toString(userOid);
        // try to find from local/ldap/saml users
        Optional<String> username = findUsername(userUuid, PREFIX);
        if (username.isPresent()) {
            return username;
        }
        // if not found, also try to find from the external group users
        return findUsername(userUuid, PREFIX_GROUP_USERS);
    }

    /**
     * Find the username for the given user oid, from the given folder.
     *
     * @param userUuid string id of the user to look for
     * @param kvPrefix kv prefix of the folder to look for users in
     * @return optional username if it exists, or empty
     */
    private Optional<String> findUsername(@Nonnull String userUuid,
                                          @Nonnull String kvPrefix) {
        Map<String, String> users;
        synchronized (storeLock_) {
            users = keyValueStore_.getByPrefix(kvPrefix);
        }
        return users.values().stream()
                .map(jsonData -> GSON.fromJson(jsonData, UserInfo.class))
                .filter(userInfo -> StringUtils.equals(userInfo.uuid, userUuid))
                .map(userInfo -> userInfo.userName)
                .findFirst();
    }

    /**
     * Get oids of all the persisted users which belong to the given external group.
     *
     * @param externalGroupName name of the external group to get users
     * @return list of user oids
     */
    @Nonnull
    private List<Long> getUserIdsInExternalGroup(@Nonnull String externalGroupName) {
        final Map<String, String> users;
        synchronized (storeLock_) {
            users = keyValueStore_.getByPrefix(composeExternalGroupUsersInfoKey(externalGroupName));
        }
        return users.values().stream()
                .map(jsonData -> GSON.fromJson(jsonData, UserInfo.class))
                .map(userInfo -> Long.valueOf(userInfo.uuid))
                .collect(Collectors.toList());
    }

    /**
     * Adds the user.
     *
     * @param provider  The provider: Local or AD.
     * @param userName  The user name.
     * @param password  The password.
     * @param roleNames The roles.
     * @param scopeGroups The entity groups in the user scope, if any.
     * @return The uuid of the user that was added or an empty string if it fails.
     * @throws SecurityException In case of an error adding the user.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public String add(final @Nonnull AuthUserDTO.PROVIDER provider,
                       final @Nonnull String userName,
                       final @Nonnull String password,
                       final @Nonnull List<String> roleNames,
                       final @Nullable List<Long> scopeGroups)
            throws SecurityException {
        // only administrators may create another administrator user
        if (mayAlterUserWithRoles(roleNames)) {
            return addImpl(provider, userName, password, roleNames, scopeGroups);
        } else {
            throw new SecurityException("Only a user with ADMINISTRATOR role user may create " +
                "another user with ADMINISTRATOR role.");
        }
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

        // check to see if the requesting user is allowed to change the password for the given user
        if (!changePasswordAllowed(userName)) {
            logger_.error("AUDIT::FAILURE:AUTH: Cannot change Password - not owner " +
                "or administrator: " + userName);
            throw new SecurityException("Cannot change Passwword - not owner or administrator");
        }

        try {
            String jsonData = json.get();
            UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
            // Check the authentication.
            // We add this bypass, since MT does not provide the existing password.
            if (password != null && !HashAuthUtils.checkSecureHash(info.passwordHash, password)) {
                throw new SecurityException("AUDIT::NEGATIVE: Password mismatch");
            }
            // Update password if necessary.
            if (passwordNew.isEmpty()) {
                throw new SecurityException("Empty new password");
            }
            info.passwordHash = HashAuthUtils.secureHash(passwordNew);
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
     * Despite the name of the function, this method includes setting scope groups as well. This is
     * likely going to be a temporary condition, as we ultimately expect to move scope assignment
     * into the role itself. This future change is being defined as part of the upcoming
     * <a href="https://vmturbo.atlassian.net/wiki/spaces/PMTES/pages/174076798/Custom+User+Roles">overhaul
     * of how user role / permissioning works</a>.
     *
     * @param provider  The provider.
     * @param userName  The user name.
     * @param roleNames The roles.
     * @param scopeGroups The list of scope groupe defining the user's entity access, if applicable.
     * @return The {@code true} iff successful.
     * @throws SecurityException In case of an error replacing user's roles.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public ResponseEntity<String> setRoles(final @Nonnull AuthUserDTO.PROVIDER provider,
                                           final @Nonnull String userName,
                                           final @Nonnull List<String> roleNames,
                                           final @Nullable List<Long> scopeGroups) {

        Optional<String> json = getKVValue(composeUserInfoKey(provider, userName));
        if (!json.isPresent()) {
            logger_.error("AUDIT::FAILURE:UNKNOWN: Error modifying unknown user: " + userName);
            return new ResponseEntity<>("Error modifying unknown user: " + userName, HttpStatus.BAD_REQUEST);
        }

        final UserInfo info = GSON.fromJson(json.get(), UserInfo.class);
        // Don't allow modifying role for the last local admin user
        Map<String, String> allUsers;
        synchronized (storeLock_) {
            allUsers = keyValueStore_.getByPrefix(PREFIX);
        }
        if (isLastLocalAdminUser(info, allUsers) && !CollectionUtils.isEqualCollection(info.roles,
            roleNames)) {
            logger_.error("AUDIT::Don't allow modifying role for last local admin user: " +
                userName);
            return new ResponseEntity<>("Not allowed to modify role for last local " +
                "administrator user: " + userName, HttpStatus.FORBIDDEN);
        }

        // Don't allow SITE_ADMIN users to modify ADMINISTRATOR users
        if (!mayAlterUserWithRoles(info.roles)) {
            logger_.error("AUDIT::Don't allow SITE_ADMIN user to modify role for admin user: " +
                userName);
            return new ResponseEntity<>("SITE_ADMIN not allowed to modify role for " +
                "administrator user: " + userName, HttpStatus.FORBIDDEN);
        }

        try {
            validateScopeGroups(roleNames, scopeGroups);
        } catch (IllegalArgumentException iae) {
            // any user-actionable problems would come back as illegal argument exceptions
            return new ResponseEntity(iae.getMessage(), HttpStatus.BAD_REQUEST);
        }

        try {
            // ensure role are upper case for any I/O operations, here is saving to Consul.
            info.roles = roleNames.stream().map(String::toUpperCase).collect(Collectors.toList());
            info.scopeGroups = scopeGroups;
            // Update KV store.
            putKVValue(composeUserInfoKey(provider, userName), GSON.toJson(info));
            logger_.info("AUDIT::SUCCESS: Success modifying user: " + userName);
            return new ResponseEntity<>("users://" + userName, HttpStatus.OK);
        } catch (Exception e) {
            logger_.error("AUDIT::FAILURE:AUTH: Error modifying user: " + userName, e);
            return new ResponseEntity<>("Error modifying user: " + userName, HttpStatus.BAD_REQUEST);
        }
    }

    /**
     * Removes the user.
     *
     * @param uuid The user's UUID or name.
     * @return {@code Optional<AuthUserDTO>} iff successful.
     * @throws SecurityException In case of an error deleting the user.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public Optional<AuthUserDTO> remove(final @Nonnull String uuid) throws SecurityException {
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
            return Optional.empty();
        }

        // Don't allow a SITE_ADMIN user to remove an ADMINISTRATOR user
        if (!mayAlterUserWithRoles(infoFound.roles)) {
            logger_.error("AUDIT::FAILURE:UNKNOWN: SITE_ADMIN user may not remove ADMINISTRATOR : " +
                uuid);
            return Optional.empty();
        }

        // Don't allow removing the last local admin user.
        if (isLastLocalAdminUser(infoFound, users)) {
            logger_.error("AUDIT::Don't allow to remove last local admin user: " + uuid);
            return Optional.empty();
        }

        try {
            removeKVKey(composeUserInfoKey(infoFound.provider, infoFound.userName));
            logger_.info("AUDIT::SUCCESS: Success removing user: " + uuid);
            return Optional.of(new AuthUserDTO(infoFound.provider, infoFound.userName, null, null, infoFound.uuid, null,
                infoFound.roles, infoFound.scopeGroups));
        } catch (Exception e) {
            logger_.error("AUDIT::FAILURE:AUTH: Error removing user: " + uuid);
            return Optional.empty();
        }
    }

    /**
     * Locks the user.
     *
     * @param dto The user DTO.
     * @return {@code true} iff successful.
     * @throws SecurityException In case of an error locking the user.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public boolean lock(final @Nonnull AuthUserDTO dto) throws SecurityException {
        if (!mayAlterUserWithRoles(dto.getRoles())) {
            throw new SecurityException("SITE_ADMIN may not lock ADMINISTRATOR user: " +
                dto.getUser());
        }
        return setUserLockStatus(dto, true);
    }

    /**
     * Unlocks the user.
     *
     * @param dto The user DTO.
     * @return {@code true} iff successful.
     * @throws SecurityException In case of an error unlocking the user.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public boolean unlock(final @Nonnull AuthUserDTO dto) throws SecurityException {
        if (!mayAlterUserWithRoles(dto.getRoles())) {
            throw new SecurityException("SITE_ADMIN may not unlock ADMINISTRATOR user: " +
                dto.getUser());
        }
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
        List <SecurityGroupDTO> adGroups = getSecurityGroups();
        if (!isAdAvailable && adGroups.size() == 0) {
            return;
        }
        // only reset when it's required.
        ssoUtil.reset();
        // always load group, since it's shared by AD and SAML
        adGroups.forEach(adGroup -> {
            ssoUtil.putSecurityGroup(adGroup.getDisplayName(), adGroup);
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
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
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
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public @Nonnull ActiveDirectoryDTO createActiveDirectory(final @Nonnull ActiveDirectoryDTO inputDTO) {
        String domain = inputDTO.getDomainName() == null ? "" : inputDTO.getDomainName();
        String url = createLoginProviderURI(inputDTO.getLoginProviderURI(), inputDTO.isSecure());

        // We set a non-null array to AD groups here, so that later we can avoid the null checks.
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
     * Create LDAP login provider url based on the url provided by user. It will prepend a default
     * protocol prefix or append a default port postfix if the input url doesn't have it.
     *
     * @param inputUrl ldap url provided by user
     * @param isSecure whether or not to use secure connection
     * @return LDAP url used for AD authentication
     */
    public String createLoginProviderURI(@Nullable String inputUrl, boolean isSecure) {
        if (inputUrl == null) {
            // In case the provider URL is empty, we use domain for AD servers lookup.
            return "";
        }

        // default protocol prefix and port postfix based on secure flag
        final String defaultProtocolPrefix = isSecure ? "ldaps://" : "ldap://";
        final String defaultPortPostfix = isSecure ? ":636" : ":389";

        if (inputUrl.contains("://")) {
            // protocol is already provided
            final String[] parts = inputUrl.split("://");
            if (parts.length != 2) {
                // invalid ldap url since there is more than 1 "://"
                throw new IllegalArgumentException("Invalid LDAP url: " + inputUrl);
            }

            if (parts[1].contains(":")) {
                // port is provided, use user-provided port
                // e.g. "ldap://ad.foo.com:3268"
                return defaultProtocolPrefix + parts[1];
            } else {
                // port is not provided, append a port number
                // e.g. "ldap://ad.foo.com"
                return defaultProtocolPrefix + parts[1] + defaultPortPostfix;
            }
        } else {
            // protocol is not provided
            if (inputUrl.contains(":")) {
                // port is provided, prepend a protocol prefix
                // e.g. "ad.foo.com:3268"
                return defaultProtocolPrefix + inputUrl;
            } else {
                // none of port or protocol is provided, prepend protocol and append port
                // e.g. "ad.foo.com"
                return defaultProtocolPrefix + inputUrl + defaultPortPostfix;
            }
        }
    }

    /**
     * Returns the list of SSO group objects.
     *
     * @return The list of SSO group objects.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public @Nonnull List<SecurityGroupDTO> getSecurityGroups() {
        Map<String, String> ssoGroups;
        synchronized (storeLock_) {
            ssoGroups = keyValueStore_.getByPrefix(PREFIX_GROUP);
        }

        List<SecurityGroupDTO> list = new ArrayList<>();
        for (String jsonData : ssoGroups.values()) {
            SecurityGroupDTO info = GSON.fromJson(jsonData, SecurityGroupDTO.class);
            list.add(info);
        }
        return list;
    }

    /**
     * Creates an Active Directory group.
     *
     * @param adGroupInputDto The description of an SSO group to be created.
     * @return The {@link SecurityGroupDTO} object.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public @Nullable
    SecurityGroupDTO createSecurityGroup(final @Nonnull SecurityGroupDTO adGroupInputDto) {
        final String adGroupName = adGroupInputDto.getDisplayName();
        Optional<String> json = getKVValue(composeExternalGroupInfoKey(adGroupName));
        if (json.isPresent()) {
            throw new SecurityException("Creating active directory group which already exists: " + adGroupName);
        }

        return addSecurityGroupImpl(adGroupInputDto);
    }

    private SecurityGroupDTO addSecurityGroupImpl(@Nonnull final SecurityGroupDTO adGroupInputDto) {
        String adGroupName = adGroupInputDto.getDisplayName();
        try {
            ssoUtil.putSecurityGroup(adGroupName, adGroupInputDto);
            // ensure role are upper case for any I/O operations, here is saving to Consul.
            SecurityGroupDTO g = new SecurityGroupDTO(adGroupName,
                adGroupInputDto.getType(),
                adGroupInputDto.getRoleName().toUpperCase(),
                adGroupInputDto.getScopeGroups());
            putKVValue(composeExternalGroupInfoKey(adGroupName), GSON.toJson(g));
            return g;
        } catch (Exception e) {
            throw new SecurityException("Error creating active directory group: " + adGroupName);
        }
    }

    /**
     * Update the Active Directory group.
     *
     * @param adGroupInputDto The Active Directory group creation request.
     * @return The {@link SecurityGroupDTO} indicating success.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public @Nullable
    SecurityGroupDTO updateSecurityGroup(final @Nonnull SecurityGroupDTO adGroupInputDto) {
        final String adGroupName = adGroupInputDto.getDisplayName();
        Optional<String> json = getKVValue(composeExternalGroupInfoKey(adGroupName));
        if (!json.isPresent()) {
            throw new SecurityException("No active directory group with name: " + adGroupName);
        }

        try {
            // recreate this object with type, role name and scope groups from the input param
            // ensure role are upper case for any I/O operations, here is saving to Consul.
            SecurityGroupDTO g = new SecurityGroupDTO(adGroupName,
                adGroupInputDto.getType(),
                adGroupInputDto.getRoleName().toUpperCase(),
                adGroupInputDto.getScopeGroups());
            putKVValue(composeExternalGroupInfoKey(adGroupName), GSON.toJson(g));
            return g;
        } catch (Exception e) {
            throw new SecurityException("Error updating active directory group: " + adGroupName);
        }
    }

    /**
     * Deletes the group.
     *
     * @param groupName The group name.
     * @return {@code true} iff the group existed before this call.
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public @Nonnull Boolean deleteSecurityGroup(final @Nonnull String groupName) {
        Optional<String> json = getKVValue(composeExternalGroupInfoKey(groupName));
        if (!json.isPresent()) {
            throw new SecurityException("Error retrieving external group.");
        }

        try {
            removeKVKey(composeExternalGroupInfoKey(groupName));
            ssoUtil.deleteSecurityGroup(groupName);
            return true;
        } catch (Exception e) {
            throw new SecurityException("Error retrieving external group");
        }
    }

    /**
     * Deletes the group, and also handle the widgetsets owned by users in this group by
     * transferring them to current user.
     *
     * @param groupName The group name.
     * @return {@code true} iff deleting group and transferring widgetsets successfully
     */
    @PreAuthorize("hasAnyRole('ADMINISTRATOR', 'SITE_ADMIN')")
    public Boolean deleteSecurityGroupAndTransferWidgetsets(final @Nonnull String groupName) {
        final Boolean result = deleteSecurityGroup(groupName);
        try {
            // transfer all the widgetsets owned by users belonging to the external group to current user
            Optional<Long> currentUserOid = SAMLUserUtils.getAuthUserDTO()
                    .map(AuthUserDTO::getUuid)
                    .map(Long::valueOf);
            if (!currentUserOid.isPresent()) {
                // this should not happen, as it should not arrive here if no user logged in
                logger_.error("No user found in context, unable to transfer widgetsets owned by" +
                        " users in group: {}", groupName);
                return false;
            }
            widgetsetDbStore.transferOwnership(getUserIdsInExternalGroup(groupName), currentUserOid.get());

            // we should also delete the node starts with "/groupusers/groupName/", which contains
            // all the users belonging to this external group
            removeKVKeysWithPrefix(composeExternalGroupUsersInfoKey(groupName));
            return result;
        } catch (Exception e) {
            throw new SecurityException("Error transferring widgetsets from users in external " +
                    "group: " + groupName + ", " + e.getMessage());
        }
    }

    /**
     * To change a user's password, the request must either come from the user or from
     * a user with ADMINISTRATOR role.
     *
     * @param userName the user whose password is about to be changed
     * @return true if the requesting user is allowed to change this user's password
     */
    @VisibleForTesting
    boolean changePasswordAllowed(@Nonnull final String userName) {
        final Authentication authentication =
                SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return false;
        }
        return hasRoleAdministrator(authentication) || SAMLUserUtils.getAuthUserDTO()
                .map(authUserDTO -> userName.equals(authUserDTO.getUser()))
                .orElse(false);
   }

    /**
     * Test to see if the requesting user may alter (create/update/delete) a user with
     * the given roles. Only an ADMINISTRATOR may alter other users with ADMINISTRATOR role.
     *
     * @param roleNames the list of roles to be assigned to the new user
     * @return true if the requesting user may create a new user with the given roles
     */
    @VisibleForTesting
    boolean mayAlterUserWithRoles(@Nonnull final List<String> roleNames) {
        final Authentication authentication =
                SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return false;
        }
        return (hasRoleAdministrator(authentication) || !roleMatched(roleNames, ADMINISTRATOR));
    }

    /**
     * Does the current user have role ADMINISTRATOR? The security defintions all have the
     * string "ROLE_" prepended to the Turbonomic role.
     *
     * @return true iff the current user has role ADMINISTRATOR
     */
    private boolean hasRoleAdministrator(@Nonnull Authentication authentication) {
        return authentication.getAuthorities().stream()
            .anyMatch(ga -> "ROLE_ADMINISTRATOR".equals(ga.getAuthority()));
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

        List<Long> scopeGroups;

        boolean unlocked;
    }

    /**
     * Test if there is only one local admin user.
     *
     * @param userInfo the requested user information.
     * @param users the list of user.
     * @return true if there is only one local user has ADMINISTRATOR role.
     */
    boolean isLastLocalAdminUser(@Nonnull final UserInfo userInfo,
                                         @Nonnull final Map<String, String> users) {

        if (roleMatched(userInfo.roles, UserRole.ADMINISTRATOR.name())
                // the user the admin user
                && userInfo.provider.equals(PROVIDER.LOCAL)) { // the provider is local
            List<UserInfo> userInfoList = new ArrayList<>();
            for (String jsonData : users.values()) {
                UserInfo info = GSON.fromJson(jsonData, UserInfo.class);
                userInfoList.add(info);
            }
            long administratorCount = userInfoList.stream()
                    .filter(user -> user.provider.equals(PROVIDER.LOCAL)
                            && roleMatched(user.roles, UserRole.ADMINISTRATOR.name()))
                    .count();
            return administratorCount == 1;
        }
        return false;
    }

    /**
     * Match user roles and intended role case insensitively.
     *
     * @param roles            the list of roles user have
     * @param intendedRoleName the  role name intended to be matched.
     * @return true if one of the provided role match role name case insensitively.
     */
    private boolean roleMatched(final @Nonnull List<String> roles,
            final @Nonnull String intendedRoleName) {
        return roles.stream().anyMatch(role -> role.equalsIgnoreCase(intendedRoleName));
    }
}
