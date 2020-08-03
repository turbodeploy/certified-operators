package com.vmturbo.auth.api.authorization.jwt;

import java.security.PublicKey;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.IAuthorizationToken;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * The JWT authentication verifier. All methods are thread safe.
 */
@ThreadSafe
public class JWTAuthorizationVerifier implements IAuthorizationVerifier {
    public static final String AUTH_FAIL_AUTHENTICATION_HAS_FAILED = "::AUTH:FAIL: Authentication has failed";
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(JWTAuthorizationVerifier.class);

    /**
     * The clock skew in seconds.
     * Will pass the verification when the difference between an expiration time and current time
     * is within this value.
     */
    private static final int CLOCK_SKEW_SEC = 60;

    /**
     * The JWT claim for an UUID field.
     */
    public static final String UUID_CLAIM = "UUID";

    /**
     * The JWT claim for an IP address field.
     */
    public static final String IP_ADDRESS_CLAIM = "IpAddress";

    /**
     * The tokens cache.
     */
    @VisibleForTesting
    final Map<IAuthorizationToken, EntryStruct> tokensCache_ =
            Collections.synchronizedMap(new HashMap<>());

    /**
     * The KV store
     */
    private final IAuthStore authStore;

    /**
     * The key used to sign a JWT token.
     */
    private String keyString;

    /**
     * The actual public key.
     */
    private PublicKey signatureVerificationKey_;

    /**
     * For testing only.
     *
     * @param publicKey The auth component public key.
     */
    @VisibleForTesting
    JWTAuthorizationVerifier(final @Nonnull PublicKey publicKey) {
        signatureVerificationKey_ = publicKey;
        this.authStore = null;
    }

    /**
     * Creates the verifier.
     * In case this bean is autowired, we want default constructor
     */
    private JWTAuthorizationVerifier() {
        authStore = null;
    }

    /**
     * Creates the verifier.
     * @param authStore store to retrieve auth and other component's public key
     */
    public JWTAuthorizationVerifier(final @Nonnull IAuthStore authStore) {
        this.authStore = authStore;
    }

    /**
     * Performs a match between roleSet and expectedRoles.
     * Each role in the expectedRoles must be present in the roleSet.
     *
     * @param roleSet       The role set claimed by token.
     * @param expectedRoles The roles that the caller expects to have.
     * @throws AuthorizationException In case any of the expected roles(s) are not present in the
     *                                claimed set.
     */
    private void rolesMatch(final Collection<String> roleSet,
                            final @Nonnull Collection<String> expectedRoles)
            throws AuthorizationException {
        // In case we don't expect any roles in the token, simply return.
        if (expectedRoles.isEmpty()) {
            return;
        }
        // In case the claimed role set is empty, deny the authorization.
        if (roleSet == null || !roleSet.containsAll(expectedRoles)) {
            throw new AuthorizationException("The principal does not have a required role");
        }
    }

    /**
     * Performs a match between scope and expectedScope.
     *
     * Each group id in the expectedScope must be present in the presented scope.
     *
     * @param scope       The scope claimed by token.
     * @param expectedScope The scope that the caller expects to have.
     * @throws AuthorizationException If the expected and actual scopes do not match.
     */
    /*
    private void scopeMatch(final Collection<Long> scope,
                            final Collection<Long> expectedScope)
            throws AuthorizationException {


        if (!CollectionUtils.isEqualCollection(scope, expectedScope)) {
            throw new AuthorizationException("The principal does not have a required role");
        }

        // In case we don't expect any roles in the token, simply return.
        if (expectedRoles.isEmpty()) {
            return;
        }
        // In case the claimed role set is empty, deny the authorization.
        if (roleSet == null || !roleSet.containsAll(expectedRoles)) {
            throw new AuthorizationException("The principal does not have a required role");
        }
    }
    */

    /**
     * Verifies the validity of the token.
     *
     * @param token         The existing authentication token.
     * @param expectedRoles The collection of roles the caller claims to have.
     * @return The principal and roles.
     * @throws AuthorizationException In case of verification failure.
     */
    @Override
    public AuthUserDTO verify(final @Nonnull IAuthorizationToken token,
                              final @Nonnull Collection<String> expectedRoles)
            throws AuthorizationException {
        // Re-initialize the verification key if needed.
        // We use lazy initialization so that an order in which Spring initializes things does
        // not cause the failure to obtain the public key. We might or might not have the Consul
        // properly started when the verifier is being constructed.
        synchronized (this) {
            if (signatureVerificationKey_ == null && keyString == null) {
                keyString = authStore.retrievePublicKey();
                if (keyString != null) {
                    signatureVerificationKey_ = JWTKeyCodec.decodePublicKey(keyString);
                }
            }

            if (signatureVerificationKey_ == null) {
                throw new AuthorizationException("Uninitialized crypto environment");
            }
        }

        // Perform the expiration verification here to avoid signature verification below.
        EntryStruct entry = tokensCache_.get(token);
        if (entry != null) {
            // Check for the expiration first.
            if (entry.timestamp_ms_ <= 0 || entry.timestamp_ms_ > System.currentTimeMillis()) {
                // See if we have the required role.
                // We don't need to remove the token from the cache, as we only test it against the
                // expected roles.
                rolesMatch(entry.roles_, expectedRoles);
                logger.info("::AUTH:SUCCESS: Authentication successful");
                return entry.asAuthUserDTO();
            } else {
                tokensCache_.remove(token);
                logger.error("::AUTH:FAIL: Authentication token has expired");
                throw new AuthorizationException(
                        "::AUTH:FAIL: Authentication token has expired");
            }
        }
        // The cache doesn't contain the token.
        try {
            // We allow 60 seconds clock skew.
            final Jws<Claims> claims =
                    Jwts.parser().setAllowedClockSkewSeconds(CLOCK_SKEW_SEC)
                            .setSigningKey(signatureVerificationKey_)
                            .parseClaimsJws(token.getCompactRepresentation());
            @SuppressWarnings("unchecked")
            final List<String> rolesPayload = (List<String>)claims.getBody().get(ROLE_CLAIM);

            // Check the role match if needed.
            rolesMatch(rolesPayload, expectedRoles);
            entry = getEntryStruct(rolesPayload, claims);
            tokensCache_.put(token, entry);
            return entry.asAuthUserDTO();
        } catch (Exception e) {
            logger.error(AUTH_FAIL_AUTHENTICATION_HAS_FAILED);
            throw new AuthorizationException(e);
        }
    }

    private EntryStruct getEntryStruct(final @Nonnull List<String> rolesPayload,
                                       final Jws<Claims> claims) {
        final EntryStruct entry;

        final Claims claimsBody = claims.getBody();
        // Add this to the cache.
        logger.info("::AUTH:SUCCESS: Authentication successful for " +
                claimsBody.getSubject());

        // Add the new entry to the cache.
        // Take into account the clock skew.
        final long expirationTime_ms =
                (claimsBody.getExpiration() == null)
                        ? Long.MIN_VALUE
                        : claimsBody.getExpiration().getTime();
        String subject = claimsBody.getSubject();
        String uuid = claimsBody.get(UUID_CLAIM, String.class);
        List<Long> scopeGroups = (List<Long>) claimsBody.get(SCOPE_CLAIM);
        String provider = claimsBody.get(PROVIDER_CLAIM, String.class);
        entry = new EntryStruct(expirationTime_ms + CLOCK_SKEW_SEC * 1000L,
                rolesPayload, scopeGroups, subject, uuid,
                provider != null ? AuthUserDTO.PROVIDER.valueOf(provider) : null);
        return entry;
    }

    @Override
    public AuthUserDTO verifyComponent(final JWTAuthorizationToken token,
                                       final String component) throws AuthorizationException {
        return authStore.retrievePublicKey(component)
                .map(JWTKeyCodec::decodePublicKey)
                .map(publicKey ->
                        Jwts.parser().setAllowedClockSkewSeconds(CLOCK_SKEW_SEC)
                                .setSigningKey(publicKey)
                                .parseClaimsJws(token.getCompactRepresentation())
                ).map(claimsJws ->
                                getEntryStruct((List<String>)claimsJws.getBody().get(ROLE_CLAIM), claimsJws)
                ).map(EntryStruct::asAuthUserDTO)
                .orElseThrow(() -> new AuthorizationException(AUTH_FAIL_AUTHENTICATION_HAS_FAILED));
    }

    /**
     * The cached entry structure.
     */
    private static class EntryStruct {
        /**
         * The expiration time.
         */
        final long timestamp_ms_;

        /**
         * The roles
         */
        final List<String> roles_;

        /**
         * The scope groups
         */
        final List<Long> scope_groups_;

        /**
         * The subject.
         */
        final String principal_;

        /**
         * The subject UUID.
         */
        final String uuid_;

        /**
         * The subject UUID.
         */
        final AuthUserDTO.PROVIDER provider_;

        /**
         * Constructs the struct.
         *
         * @param timestamp_ms The expiration time.
         * @param roles        The roles.
         * @param principal    The principal.
         * @param uuid         The UUID.
         * @param provider     The login provider.
         */
        EntryStruct(final long timestamp_ms, final List<String> roles, final List<Long> scope_groups,
                    final String principal, final String uuid, final AuthUserDTO.PROVIDER provider) {
            timestamp_ms_ = timestamp_ms;
            roles_ = roles;
            scope_groups_ = scope_groups;
            principal_ = principal;
            uuid_ = uuid;
            provider_ = provider;
        }

        /**
         * Constructs a {@link AuthUserDTO} object from this entry.
         *
         * @return A {@link AuthUserDTO} object from this entry.
         */
        AuthUserDTO asAuthUserDTO() {
            return new AuthUserDTO(provider_, principal_, null, null, uuid_, null,
                    roles_ == null ? roles_ : ImmutableList.copyOf(roles_),
                    scope_groups_);
        }
    }
}
