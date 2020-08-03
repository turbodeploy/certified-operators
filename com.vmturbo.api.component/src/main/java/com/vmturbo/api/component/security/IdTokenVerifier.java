package com.vmturbo.api.component.security;

import java.security.PublicKey;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.authentication.AuthenticationException;

/**
 * ID/JWT token verifier interface. It's used to decouple vendor specific token verification.
 */
public interface IdTokenVerifier {

    /**
     * The clock skew in seconds.
     * Will pass the verification when the difference between an expiration time and current time
     * is within this value.
     */
    int CLOCK_SKEW_SEC = 60;

    /**
     * Verify vender's JWT token. It could be previous supported version.
     *
     * @param jwtPublicKey public key to verify JWT token.
     * @param jwtToken JWT token to verify.
     * @param clockSkewSecond clock skew seconds.
     * @return {@link Pair} with claimed username and role in JWT token.
     * @throws AuthenticationException if JWT token verification failed.
     */
    Pair<String, String> verify(@Nonnull Optional<PublicKey> jwtPublicKey,
            @Nonnull Optional<String> jwtToken, long clockSkewSecond)
            throws AuthenticationException;

    /**
     * Verify vender's JWT token current version.
     *
     * @param jwtPublicKey public key to verify JWT token.
     * @param jwtToken JWT token to verify.
     * @param clockSkewSecond clock skew seconds.
     * @return {@link Pair} with claimed username and role in JWT token.
     * @throws AuthenticationException if JWT token verification failed.
     */
    Pair<String, String> verifyLatest(@Nonnull Optional<PublicKey> jwtPublicKey,
                                @Nonnull Optional<String> jwtToken, long clockSkewSecond)
            throws AuthenticationException;
}
