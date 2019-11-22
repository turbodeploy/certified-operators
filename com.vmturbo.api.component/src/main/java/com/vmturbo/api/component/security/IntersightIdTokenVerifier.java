package com.vmturbo.api.component.security;

import java.security.PublicKey;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.jsonwebtoken.Jwts;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.authentication.AuthenticationException;

/**
 * Cisco intersight specific JWT token verifier.
 */
public class IntersightIdTokenVerifier implements IdTokenVerifier {

    private static final String SUB = "sub";
    private static final String ROLES = "Roles";

    /**
     * Verifies the validity of the token.
     *
     * @param jwtToken The existing authentication token.
     * @param jwtPublicKey The collection of roles the caller claims to have.
     * @return An {@link Pair} object describing the token's principal as well as its
     *         authorized role.
     * @throws AuthenticationException if verification fails.
     */
    @Override
    public Pair<String, String> verify(@Nonnull final Optional<PublicKey> jwtPublicKey,
            @Nonnull final Optional<String> jwtToken, final long clockSkewSecond)
            throws AuthenticationException {
        return jwtPublicKey.flatMap(publicKey -> jwtToken.map(token -> Jwts.parser()
                .setAllowedClockSkewSeconds(clockSkewSecond)
                .setSigningKey(publicKey)
                .parseClaimsJws(token)))
                .map(claimsJws -> new Pair(String.valueOf(claimsJws.getBody().get(SUB)),
                        String.valueOf(claimsJws.getBody().get(ROLES))))
                .orElseThrow(() -> new AuthenticationException("Failed to validate JWT token"));
    }
}