package com.vmturbo.api.component.security;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.authentication.AuthenticationException;
import io.jsonwebtoken.Jwts;

import javax.annotation.Nonnull;
import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;

/**
 * Cisco intersight specific JWT token verifier.
 */
public class IntersightIdTokenVerifier implements IdTokenVerifier {

    private static final String SUB = "sub";
    private static final String ROLES = "Roles";
    private static final String INTERSIGHT = "intersight";

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

    @Override
    public Pair<String, String> verifyLatest(@Nonnull Optional<PublicKey> jwtPublicKey,
                                             @Nonnull Optional<String> jwtToken,
                                             long clockSkewSecond) throws AuthenticationException {
        return jwtPublicKey.flatMap(publicKey -> jwtToken.map(token -> Jwts.parser()
                .setAllowedClockSkewSeconds(clockSkewSecond)
                .setSigningKey(publicKey)
                .parseClaimsJws(token)))
                .map(claimsJws -> new Pair(claimsJws.getBody().get(SUB, String.class),
                        claimsJws.getBody().get(INTERSIGHT, Map.class).get("roles")))
                .orElseThrow(() -> new AuthenticationException("Failed to validate JWT token"));
    }
}