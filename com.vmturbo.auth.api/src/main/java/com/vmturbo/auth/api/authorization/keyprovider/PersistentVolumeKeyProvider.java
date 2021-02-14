package com.vmturbo.auth.api.authorization.keyprovider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

import com.vmturbo.kvstore.IPublicKeyStore;

/**
 * A Persistent Volume-based key provider.
 *
 * <p>For providing (generating, storing and retrieving) private/public key pairs. Private keys are
 * stored encrypted in the persistent volume. Public keys are stored in the KV store.</p>
 *
 * <p>Note: This is legacy logic. The newer implementation is {@link KVKeyProvider}.</p>
 */
public class PersistentVolumeKeyProvider extends AbstractKeyProvider {

    /**
     * The keystore data file name.
     */
    protected static final String VMT_PRIVATE_KEY_FILE = "vmt_helper_kv.inout";

    private final Path keyDir;

    private final Path encryptionFilePath;

    /**
     * Create a KeyProvider using the legacy logic and persistent volumes.
     *
     * @param publicKeyStore to store the public key
     * @param keyDir directory in the persistent volume to store the private key
     */
    public PersistentVolumeKeyProvider(@Nonnull final IPublicKeyStore publicKeyStore,
                                       @Nonnull final String keyDir) {
        super(publicKeyStore);
        this.keyDir = Paths.get(keyDir);
        this.encryptionFilePath = Paths.get(keyDir + "/" + VMT_PRIVATE_KEY_FILE);
    }

    @Override
    protected boolean doesPrivateKeyExist() {
        return Files.exists(getEncryptionFilePath());
    }

    @Override
    protected String getEncryptedPrivateKey() {
        try {
            byte[] keyBytes = Files.readAllBytes(getEncryptionFilePath());
            return new String(keyBytes, CHARSET_CRYPTO);
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }

    @Override
    protected void persistPrivateKey(@Nonnull String privateKeyEncrypted) {
        try {
            Path outputDir = getKeyDir();
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }

            // Persist
            Files.write(getEncryptionFilePath(),
                privateKeyEncrypted.getBytes(CHARSET_CRYPTO));
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }

    public Path getKeyDir() {
        return keyDir;
    }

    private Path getEncryptionFilePath() {
        return encryptionFilePath;
    }
}
