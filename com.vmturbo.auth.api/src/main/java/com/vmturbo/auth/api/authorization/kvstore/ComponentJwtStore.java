package com.vmturbo.auth.api.authorization.kvstore;

import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Lists;

import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.keyprovider.KeyProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.kvstore.IPublicKeyStore;

/**
 * XL component JWT token store to generate component based JWT token.
 */
public class ComponentJwtStore implements IComponentJwtStore {

    private static final ArrayList<String> ADMIN_ROLE = Lists.newArrayList(SecurityConstant.ADMINISTRATOR);

    /**
     * The key/value store.
     */
    @GuardedBy("storeLock")
    private final @Nonnull IPublicKeyStore keyValueStore_;

    private final KeyProvider keyProvider;

    /**
     * Create a {@link ComponentJwtStore}.
     *
     * @param keyValueStore The key-value store to put public keys into.
     * @param identityGeneratorPrefix The prefix to use for identity generation in the provided component.
     * @param keyProvider the provider for the private key.
     */
    public ComponentJwtStore(@Nonnull final IPublicKeyStore keyValueStore,
                             final long identityGeneratorPrefix,
                             final KeyProvider keyProvider) {
        this.keyValueStore_ = Objects.requireNonNull(keyValueStore);
        this.keyProvider = Objects.requireNonNull(keyProvider);
        IdentityGenerator.initPrefix(identityGeneratorPrefix);
        initPKI();
    }

    // Ensure key pairs are available, and public key are stored remotely (consul).
    private void initPKI() {
        keyProvider.getPrivateKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @Nonnull JWTAuthorizationToken generateToken() {
        final PrivateKey privateKey = keyProvider.getPrivateKey();
        String compact = Jwts.builder()
                .setSubject(keyValueStore_.getNamespace())
                .claim(IAuthorizationVerifier.ROLE_CLAIM, ADMIN_ROLE)
                .compressWith(CompressionCodecs.GZIP)
                .signWith(SignatureAlgorithm.ES256, privateKey)
                .compact();
        return new JWTAuthorizationToken(compact);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNamespace() {
        return keyValueStore_.getNamespace();
    }
}
