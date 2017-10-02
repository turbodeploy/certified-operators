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
import com.vmturbo.auth.api.authorization.kvstore.IApiAuthStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * The JWT authentication verifier. All methods are thread safe.
 */
@ThreadSafe
public class JWTAuthorizationVerifier implements IAuthorizationVerifier {
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
     * The tokens cache.
     */
    @VisibleForTesting
    final Map<IAuthorizationToken, EntryStruct> tokensCache_ =
            Collections.synchronizedMap(new HashMap<>());

    /**
     * The KV store
     */
    private final IApiAuthStore apiKVStore_;

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
     * @param publicKey The public key.
     */
    @VisibleForTesting JWTAuthorizationVerifier(final @Nonnull PublicKey publicKey) {
        signatureVerificationKey_ = publicKey;
        apiKVStore_ = null;
    }

    /**
     * Creates the verifier.
     * In case this bean is autowired, we want default constructor
     */
    private JWTAuthorizationVerifier() {
        apiKVStore_ = null;
    }

    /**
     * Creates the verifier.
     */
    public JWTAuthorizationVerifier(final @Nonnull IApiAuthStore apiKVStore) {
        apiKVStore_ = apiKVStore;
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
                keyString = apiKVStore_.retrievePublicKey();
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

            // Add this to the cache.
            logger.info("::AUTH:SUCCESS: Authentication successful for " +
                        claims.getBody().getSubject());

            // Add the new entry to the cache.
            // Take into account the clock skew.
            final long expirationTime_ms =
                    (claims.getBody().getExpiration() == null) ? Long.MIN_VALUE :
                    claims.getBody().getExpiration().getTime();
            String subject = claims.getBody().getSubject();
            String uuid = claims.getBody().get(UUID_CLAIM, String.class);
            entry = new EntryStruct(expirationTime_ms + CLOCK_SKEW_SEC * 1000L,
                                    rolesPayload, subject, uuid);
            tokensCache_.put(token, entry);
            return entry.asAuthUserDTO();
        } catch (Exception e) {
            logger.error("::AUTH:FAIL: Authentication has failed");
            throw new AuthorizationException(e);
        }
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
         * The subject.
         */
        final String principal_;

        /**
         * The subject UUID.
         */
        final String uuid_;

        /**
         * Constructs the struct.
         *
         * @param timestamp_ms The expiration time.
         * @param roles        The roles.
         * @param principal    The principal.
         * @param uuid         The UUID.
         */
        EntryStruct(final long timestamp_ms, final List<String> roles, final String principal,
                    final String uuid) {
            timestamp_ms_ = timestamp_ms;
            roles_ = roles;
            principal_ = principal;
            uuid_ = uuid;
        }

        /**
         * Constructs a {@link AuthUserDTO} object from this entry.
         *
         * @return A {@link AuthUserDTO} object from this entry.
         */
        AuthUserDTO asAuthUserDTO() {
            return new AuthUserDTO(principal_, uuid_,
                                   roles_ == null ? roles_ : ImmutableList.copyOf(roles_));
        }
    }
}
