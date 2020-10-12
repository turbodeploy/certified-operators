package com.vmturbo.api.component.security;

import java.security.Key;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.authentication.AuthenticationException;

/**
 * Cisco intersight specific JWT token verifier.
 */
public class IntersightIdTokenVerifier implements IdTokenVerifier {

    @VisibleForTesting
    static final String INTERSIGHT = "intersight";
    private static final String SUB = "sub";
    private static final String ROLES = "Roles";
    private static final List<String> INNTERSIGHT_ROLES =
            ImmutableList.<String>builder().add("Account Administrator")
                    .add("Read-Only")
                    .add("Workload Optimizer Administrator")
                    .add("Workload Optimizer Automator")
                    .add("Workload Optimizer Deployer")
                    .add("Workload Optimizer Advisor")
                    .add("Workload Optimizer Observer")
                    .build();
    private final String permissionTag;

    /**
     * Constructor.
     *
     * @param permissionTag JWT token claim tag
     */
    public IntersightIdTokenVerifier(@Nonnull final String permissionTag) {
        this.permissionTag = permissionTag;
    }

    /**
     * Get the role from JWT claim. It supports two cases.
     * <ul>
     * <li> single intersight role </li>
     * <li> list of intersight roles </li>
     * </ul>
     *
     * @param claim JWT claim
     * @return single Intersight role in INNTERSIGHT_ROLES list.
     */
    private static String getRole(@Nonnull final Object claim) {
        // Is single role?
        if (claim instanceof String) {
            return getMatchedRole((String)claim).orElse("");
        } else if (claim instanceof ArrayList) { // multiple roles, return the one based on agreement
            return getMatchedRole((ArrayList<String>)claim).orElse("");
        }
        return "";
    }

    /**
     * Input could be:
     * "Device Technician,Workload Optimizer Administrator"
     * or
     * "Workload Optimizer Administrator"
     * Output: matched roles
     *
     * @param claim JWT claim
     * @return matched roles
     */
    @NotNull
    private static Optional<String> getMatchedRole(@Nonnull final String claim) {
        final StringTokenizer tokenizer = new StringTokenizer(claim, ",");
        if (tokenizer.countTokens() > 1) {
            final Builder listBuilder = ImmutableList.<String>builder();

            while (tokenizer.hasMoreTokens()) {
                listBuilder.add(tokenizer.nextToken().trim());
            }
            return getMatchedRole(listBuilder.build());
        }
        return Optional.ofNullable(claim);
    }

    // Role priority is descending order in INNTERSIGHT_ROLES list.
    private static Optional<String> getMatchedRole(@Nonnull final List<String> list) {
        for (String role : INNTERSIGHT_ROLES) {
            if (list.contains(role)) {
                return Optional.of(role);
            }
        }
        return Optional.empty();
    }

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
    public Pair<String, String> verifyLatest(@Nonnull Optional<? extends Key> jwtPublicKey,
            @Nonnull Optional<String> jwtToken, long clockSkewSecond)
            throws AuthenticationException {
        return jwtPublicKey.flatMap(publicKey -> jwtToken.map(token -> Jwts.parser()
                .setAllowedClockSkewSeconds(clockSkewSecond)
                .setSigningKey(publicKey)
                .parseClaimsJws(token)))
                .map(claimsJws -> getClaims(claimsJws))
                .orElseThrow(() -> new AuthenticationException("Failed to validate JWT token"));
    }

    // Helper to get claim
    @NotNull
    private Pair getClaims(@Nonnull final Jws<Claims> claimsJws) {
        final String roles = getRole(claimsJws.getBody().get(INTERSIGHT, Map.class).get(permissionTag));
        final String sub = claimsJws.getBody().get(SUB, String.class);
        return new Pair(sub, roles);
    }
}