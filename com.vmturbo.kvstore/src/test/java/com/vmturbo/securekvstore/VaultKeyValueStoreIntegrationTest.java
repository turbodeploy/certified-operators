package com.vmturbo.securekvstore;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponseSupport;

/**
 * Integration tests for HashiCorp Vault.
 * Vault setup:
 * 1. unseal Vault.
 * 2. enable secret engine v1 on path "kv"
 * Required updates for this test:
 * Values for VAULT_HOST, VAULT_PORT, PROTOCOL_SCHEME, ACCESS_TOKEN, and NAMESPACE.
 */
@Ignore("Please setup Vault and update the values before running.")
public class VaultKeyValueStoreIntegrationTest {

    // You must update the following values to match your vault environment before running the test.
    // START
    private static final String VAULT_HOST = "localhost";
    private static final int VAULT_PORT = 8200;
    private static final String PROTOCOL_SCHEME = "http";
    private static final String ACCESS_TOKEN = "root";
    private static final String NAMESPACE = "kv";
    // END

    private static final String NON_EXIST = "non-exist";
    private static final String DEFAULT_VALUE = "default";
    private static final String KEY = "test";
    private static final String VALUE = "val";
    private static final String SPECIAL_CHARS = "/\\*$@";
    private static final String COMPONENT_TYPE = "api-1";
    private static final long RETRY_INTERVAL = 600;
    private VaultTemplate vaultTemplate;
    private VaultKeyValueStore vaultKeyValueStore;
    private VaultResponseSupport<ObjectWrapper> responseSupport;

    @Before
    public void setup() {
        final VaultEndpoint vaultEndpoint = new VaultEndpoint();
        vaultEndpoint.setHost(VAULT_HOST);
        vaultEndpoint.setPort(VAULT_PORT);
        vaultEndpoint.setScheme(PROTOCOL_SCHEME);
        vaultTemplate = new VaultTemplate(vaultEndpoint, new TokenAuthentication(ACCESS_TOKEN));
        vaultKeyValueStore =
                new VaultKeyValueStore(vaultTemplate, NAMESPACE, COMPONENT_TYPE, RETRY_INTERVAL,
                        TimeUnit.MILLISECONDS);
        responseSupport = new VaultResponseSupport<>();
        ObjectWrapper objectWrapper = new ObjectWrapper(KEY, VALUE);
        responseSupport.setData(objectWrapper);
    }

    /**
     * Test that the put function calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPut() throws Exception {
        vaultKeyValueStore.put(KEY, VALUE);
        assertEquals(VALUE, vaultKeyValueStore.get(KEY, DEFAULT_VALUE));
        vaultKeyValueStore.removeKey(KEY);
    }

    /**
     * Test put empty "" value.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPutEmptyValue() throws Exception {
        vaultKeyValueStore.put(KEY, "");
        assertEquals("", vaultKeyValueStore.get(KEY, DEFAULT_VALUE));
        vaultKeyValueStore.removeKey(KEY);
    }

    @Test
    public void testPutKeyValueWithSpecialChars() throws Exception {
        vaultKeyValueStore.put(SPECIAL_CHARS, SPECIAL_CHARS);
        assertEquals(SPECIAL_CHARS, vaultKeyValueStore.get(SPECIAL_CHARS, DEFAULT_VALUE));
        vaultKeyValueStore.removeKey(SPECIAL_CHARS);
    }

    /**
     * Test that the get function decodes the value and returns it.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGet() throws Exception {
        Optional<String> optional = vaultKeyValueStore.get(NON_EXIST);
        assertFalse(optional.isPresent());
    }

    /**
     * Test that the get function with non existing key.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetDefaultWithNonExistingKey() throws Exception {
        assertEquals(DEFAULT_VALUE, vaultKeyValueStore.get(NON_EXIST, DEFAULT_VALUE));
    }

    /**
     * Test that a key with whitespace is handled properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testWhitespace() throws Exception {
        vaultKeyValueStore.put("test/test test", VALUE);
        assertEquals(VALUE, vaultKeyValueStore.get("test/test test", DEFAULT_VALUE));
        vaultKeyValueStore.removeKey("test/test test");
    }

    /**
     * Test that the store reports a key when it is present.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWithKey() throws Exception {
        vaultKeyValueStore.put(KEY, VALUE);
        assertTrue(vaultKeyValueStore.containsKey(KEY));
        vaultKeyValueStore.removeKey(KEY);
    }

    /**
     * Test that the store reports no key when it is absent.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWithoutKey() throws Exception {
        assertFalse(vaultKeyValueStore.containsKey(KEY));
    }

    /**
     * Test that remove calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRemoveKeyWithPrefix() throws Exception {
        setupPrefixTree();
        vaultKeyValueStore.removeKeysWithPrefix(KEY);
        assertFalse(vaultKeyValueStore.containsKey(KEY));
        assertFalse(vaultKeyValueStore.containsKey(KEY + "/subKey"));
        assertFalse(vaultKeyValueStore.containsKey(KEY + "/subKey1"));
    }

    /**
     * Test that removeKey calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRemoveKey() throws Exception {
        vaultKeyValueStore.put(KEY, VALUE);
        assertTrue(vaultKeyValueStore.containsKey(KEY));
        vaultKeyValueStore.removeKey(KEY);
        assertFalse(vaultKeyValueStore.containsKey(KEY));
    }

    /**
     * Basic test for the get by prefix function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetByPrefix() throws Exception {
        setupPrefixTree();
        Map<String, String> ret = vaultKeyValueStore.getByPrefix(KEY);
        assertEquals(2, ret.size());
        assertEquals(VALUE, ret.get("subKey"));
        assertEquals(VALUE, ret.get("subKey1"));
        vaultKeyValueStore.removeKeysWithPrefix(KEY);
        assertFalse(vaultKeyValueStore.containsKey(KEY));
        assertFalse(vaultKeyValueStore.containsKey(KEY + "/subKey"));
        assertFalse(vaultKeyValueStore.containsKey(KEY + "/subKey1"));
    }

    /**
     * Test that when an exception occurs the key value store retries until success.
     * Note:
     * 1. Stop vault.
     * 2. Start this test, and check the retry messages.
     * 3. Start and unseal the vault.
     * 4. This test should complete successfully.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testServiceDown() throws Exception {
        vaultKeyValueStore.put(KEY, VALUE);
        assertEquals(VALUE, vaultKeyValueStore.get(KEY, DEFAULT_VALUE));
        vaultKeyValueStore.removeKey(KEY);
    }

    private void setupPrefixTree() {
        vaultKeyValueStore.put(KEY, VALUE);
        vaultKeyValueStore.put(KEY + "/subKey", VALUE);
        vaultKeyValueStore.put(KEY + "/subKey1", VALUE);
        assertTrue(vaultKeyValueStore.containsKey(KEY));
        assertTrue(vaultKeyValueStore.containsKey(KEY + "/subKey"));
        assertTrue(vaultKeyValueStore.containsKey(KEY + "/subKey1"));
    }
}
