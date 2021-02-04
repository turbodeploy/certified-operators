package com.vmturbo.auth.api.authorization.keyprovider;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.kvstore.IPublicKeyStore;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * A KV-backed key provider.
 *
 * <p>For providing (generating, storing and retrieving) private/public key pairs. Public and
 * private keys will be stored in the KV store. Private keys are stored encrypted.</p>
 */
public class KVKeyProvider extends AbstractKeyProvider {

    /**
     * The key to use in the KV store for storing the encrypted private key.
     *
     * <p>Note: We are indulging in a little bit of obfuscation here. An attacker would have no
     * way to know that the encrypted file actually represents a private key--unless we tell them
     * by labeling it as such!</p>
     */
    private static final String PRIVATE_KEY_KV_KEY = "checksum";

    private final KeyValueStore keyValueStore;

    /**
     * Create a new Consul-based key provider.
     *
     * @param publicKeyStore for storing the public key
     * @param keyValueStore for storing the private key
     */
    public KVKeyProvider(@Nonnull final IPublicKeyStore publicKeyStore,
                         @Nonnull final KeyValueStore keyValueStore) {
        super(publicKeyStore);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
    }

    @Override
    protected boolean doesPrivateKeyExist() {
        return keyValueStore.containsKey(PRIVATE_KEY_KV_KEY);
    }

    @Override
    protected String getEncryptedPrivateKey() {
        return keyValueStore.get(PRIVATE_KEY_KV_KEY)
            .orElseThrow(() -> new SecurityException("The private key could not be retrieved!"));
    }

    @Override
    protected void persistPrivateKey(@Nonnull final String privateKeyEncrypted) {
        keyValueStore.put(PRIVATE_KEY_KV_KEY, privateKeyEncrypted);
    }
}
