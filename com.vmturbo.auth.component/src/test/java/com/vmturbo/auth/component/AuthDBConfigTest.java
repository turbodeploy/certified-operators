package com.vmturbo.auth.component;

import static com.vmturbo.auth.component.AuthDBConfig.CONSUL_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Test {@link AuthDBConfig}.
 */
public class AuthDBConfigTest {

    private static final String TESTPASS = "testpass";

    /**
     * Rule.
     */
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private AuthDBConfig config;

    private KeyValueStore keyValueStore;

    /**
     * Before every test.
     *
     * @throws Exception if any.
     */
    @Before
    public void setUp() throws Exception {
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());
        AuthKVConfig authKVConfig = mock(AuthKVConfig.class);
        keyValueStore = mock(KeyValueStore.class);
        when(authKVConfig.authKeyValueStore()).thenReturn(keyValueStore);
        config = new AuthDBConfig(authKVConfig);
    }

    /**
     * Test get decrypted password when the password is actually plan text.
     * Verify it will go through the password migration route.
     * a. encrypt the password.
     * b. persist the encrypted password.
     */
    @Test
    public void testGetDecryptPasswordInPlainText() {
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valCaptor = ArgumentCaptor.forClass(String.class);
        assertEquals(TESTPASS, config.getDecryptPassword(TESTPASS));
        verify(keyValueStore).put(keyCaptor.capture(), valCaptor.capture());
        assertEquals(CONSUL_KEY, keyCaptor.getValue());
        assertEquals(TESTPASS, CryptoFacility.decrypt(valCaptor.getValue()));
    }

    /**
     * Test get decrypted password when the password is cipher text.
     * Positive path.
     */
    @Test
    public void testGetDecryptPasswordInCipherText() {
        assertEquals(TESTPASS, config.getDecryptPassword(CryptoFacility.encrypt(TESTPASS)));
    }
}