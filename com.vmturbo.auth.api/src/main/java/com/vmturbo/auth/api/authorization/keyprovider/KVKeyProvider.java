package com.vmturbo.auth.api.authorization.keyprovider;

import java.security.PrivateKey;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.kvstore.IPublicKeyStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.Lock;

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
    /**
     * Tag for private key lock, it will have "lock/private-key" path on Consul.
     */
    private static final String PRIVATE_KEY = "private-key";

    /**
     * Consul session name.
     */
    private static final String SESSION = PRIVATE_KEY + "-SESSION";

    private final KeyValueStore keyValueStore;

    private final Logger logger_ = LogManager.getLogger();

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
        return keyValueStore.get(PRIVATE_KEY_KV_KEY).orElseThrow(
                () -> new SecurityException("The private key could not be retrieved!"));
    }

    @Override
    protected void persistPrivateKey(@Nonnull final String privateKeyEncrypted) {
        keyValueStore.put(PRIVATE_KEY_KV_KEY, privateKeyEncrypted);
    }

    @Override
    public PrivateKey getPrivateKey() {
        if (super.privateKey != null) {
            return super.privateKey;
        }
        // create a distribute lock in Consul to avoid contentions
        final Lock lock = keyValueStore.lock(SESSION, PRIVATE_KEY);
        try {
            if (lock.lock(true)) {
                logger_.debug("Successfully acquired lock for {}", PRIVATE_KEY);
                return super.getPrivateKeyInternal();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger_.error("Failed to acquire private key lock, the thread is interrupted, : ", e.getMessage());
        } catch (RuntimeException e) {
            logger_.error("Failed to get private key.", e.getMessage());
        } finally {
            lock.unlock();
        }
        throw new SecurityException("Failed to acquire private key lock.");
    }
}
