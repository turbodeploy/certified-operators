package com.vmturbo.auth.api.authorization.keyprovider;

import java.util.Optional;

import com.google.common.io.BaseEncoding;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.common.api.crypto.IEncryptionKeyProvider;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.Lock;

/**
 * Tests for {@link EncryptionKeyProvider}.
 */
public class EncryptionKeyProviderTest {

    /**
     * A mock key value store, to store the internal, per-component encryption keys.
     */
    private final KeyValueStore keyValueStoreMock = Mockito.mock(KeyValueStore.class);

    /**
     * A mock master key reader, to simulate reading the master encryption key from an external source.
     */
    private final MasterKeyReader masterKeyReaderMock = Mockito.mock(MasterKeyReader.class);

    /**
     * The class under test.
     */
    private final IEncryptionKeyProvider encryptionKeyProvider =
        new EncryptionKeyProvider(keyValueStoreMock, masterKeyReaderMock);

    /**
     * Consul distributed lock.
     */
    private final Lock lockMock = Mockito.mock(Lock.class);

    /**
     * A rule for expecting exceptions in tests.
     */
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    /**
     * Test using the main interface method, to get an existing encryption key.
     * @throws InterruptedException if the thread is interrupted.
     */
    @Test
    public void testGetExistingEncryptionKey() throws InterruptedException {
        // Prepare
        // Generate an authentic encryption key
        final byte[] rawEncryptionKeyBytes = CryptoFacility.getRandomBytes(32);
        final String base64EncodedEncryptionKey = BaseEncoding.base64().encode(rawEncryptionKeyBytes);
        // Generate a master key used to encrypt the encryption key
        final String masterKey = BaseEncoding.base64().encode(CryptoFacility.getRandomBytes(32));
        // Use the masterKey to encrypt the encryption key
        byte[] encryptedEncryptionKey = CryptoFacility.encrypt(masterKey, rawEncryptionKeyBytes);
        String encodedEncryptedEncryptionKey = BaseEncoding.base64().encode(encryptedEncryptionKey);
        // Provide a master key to decrypt the key
        Mockito.when(masterKeyReaderMock.getPrimaryMasterKey()).thenReturn(Optional.of(masterKey));
        // Signal that the key already exists
        Mockito.when(keyValueStoreMock.containsKey(Matchers.anyString())).thenReturn(true);
        // Retrieve the encoded, encrypted key from the mock store
        Mockito.when(keyValueStoreMock.get(Matchers.anyString()))
            .thenReturn(Optional.of(encodedEncryptedEncryptionKey));
        // setup distributed lock
        Mockito.when(keyValueStoreMock.lock(Mockito.any(), Mockito.any())).thenReturn(lockMock);
        Mockito.when(lockMock.lock(true)).thenReturn(true);

        // Act
        String retrievedKey = encryptionKeyProvider.getEncryptionKey();

        // Verify
        // This verifies that the key provider was able to decrypt the encrypted key using the master key
        Assert.assertEquals(base64EncodedEncryptionKey, retrievedKey);
    }

    /**
     * Test using the main interface method, to generate a new encryption key.
     * @throws InterruptedException if the thread is interrupted.
     */
    @Test
    public void testGetNewEncryptionKey() throws InterruptedException {
        // Prepare
        // Generate a master key used to encrypt the encryption key
        final String masterKey = BaseEncoding.base64().encode(CryptoFacility.getRandomBytes(32));
        // Provide a master key to encrypt/decrypt the key
        Mockito.when(masterKeyReaderMock.getPrimaryMasterKey()).thenReturn(Optional.of(masterKey));
        // Signal that the key does not yet exist
        Mockito.when(keyValueStoreMock.containsKey(Matchers.anyString())).thenReturn(false);
        Mockito.when(keyValueStoreMock.lock(Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
                Lock.class));

        // setup distributed lock
        Mockito.when(keyValueStoreMock.lock(Mockito.any(), Mockito.any())).thenReturn(lockMock);
        Mockito.when(lockMock.lock(true)).thenReturn(true);

        // Act
        String retrievedKey = encryptionKeyProvider.getEncryptionKey();

        // Verify
        Assert.assertNotNull(retrievedKey);
    }

    /**
     * Test the error case where no master key has been provided.
     */
    @Test
    public void testMasterKeyNotProvided() {
        // Prepare
        // Generate an authentic encryption key
        final String encryptionKey = BaseEncoding.base64().encode(CryptoFacility.getRandomBytes(32));
        // Provide no master key to encrypt/decrypt the key
        Mockito.when(masterKeyReaderMock.getPrimaryMasterKey()).thenReturn(Optional.empty());
        // Signal that the key already exists
        Mockito.when(keyValueStoreMock.containsKey(Matchers.anyString())).thenReturn(true);
        // Retrieve the key from the mock store
        Mockito.when(keyValueStoreMock.get(Matchers.anyString())).thenReturn(Optional.of(encryptionKey));

        // Act
        expectedException.expect(SecurityException.class);
        String retrievedKey = encryptionKeyProvider.getEncryptionKey();
    }


}
