package com.vmturbo.securekvstore;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.vault.VaultException;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponse;
import org.springframework.vault.support.VaultResponseSupport;
import org.springframework.web.client.ResourceAccessException;

/**
 * Tests for key value store back by Vault.
 */
public class VaultKeyValueStoreTest {

    private static final String KEY = "test";
    private static final String VALUE = "val";
    private static final String KV_API_1_TEST = "kv/api-1/test";
    private static final String KV = "kv";
    private static final String COMPONENT_TYPE = "api-1";
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private final long retryIntervalMillis = 600;
    private VaultTemplate vaultTemplate;
    private VaultKeyValueStore vaultKeyValueStore;
    private VaultResponseSupport<ObjectWrapper> responseSupport;

    @Before
    public void setup() {
        vaultTemplate = Mockito.mock(VaultTemplate.class);
        vaultKeyValueStore =
                new VaultKeyValueStore(vaultTemplate, KV, COMPONENT_TYPE, retryIntervalMillis,
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
        Mockito.verify(vaultTemplate).write(KV_API_1_TEST, new ObjectWrapper(KEY, VALUE));
    }

    /**
     * Test that the put function calls the right underlying function when value is empty "".
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPutEmptyValue() throws Exception {
        vaultKeyValueStore.put(KEY, "");
        Mockito.verify(vaultTemplate).write(anyString(), eq(new ObjectWrapper(KEY, "")));
    }

    /**
     * Test that the get function decodes the value and returns it.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGet() throws Exception {

        Mockito.when(vaultTemplate.read(KV_API_1_TEST, ObjectWrapper.class))
                .thenReturn(responseSupport);
        Optional<String> ret = vaultKeyValueStore.get(KEY);
        Assert.assertEquals(VALUE, ret.get());
    }

    /**
     * Test get with default value provided.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetDefault() throws Exception {
        Mockito.when(
                vaultTemplate.read(vaultKeyValueStore.fullComponentKey(KEY), ObjectWrapper.class))
                .thenReturn(responseSupport);
        Assert.assertEquals(VALUE, vaultKeyValueStore.get(KEY, "default"));
    }

    /**
     * Test get non-existed key with default value provided
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetDefaultWithNonExistingKey() throws Exception {
        Mockito.when(
                vaultTemplate.read(vaultKeyValueStore.fullComponentKey(KEY), ObjectWrapper.class))
                .thenReturn(new VaultResponseSupport<>());
        Assert.assertEquals("default", vaultKeyValueStore.get(KEY, "default"));
    }

    /**
     * Test that a key with whitespace is handled properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testWhitespace() throws Exception {
        vaultKeyValueStore.put("test/test test", VALUE);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(vaultTemplate).write(keyCaptor.capture(), Mockito.any());
        // Check that the key maps to a URI successfully.
        // If no exception gets thrown we're good!
        URI.create(keyCaptor.getValue());
    }

    /**
     * Test that getting a key with empty value.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetEmptyValue() throws Exception {
        Optional<String> ret = setupMissingKey();
        assertFalse(ret.isPresent());
    }

    private Optional<String> setupMissingKey() {
        VaultResponseSupport<ObjectWrapper> emptyResponse = new VaultResponseSupport<>();
        ObjectWrapper objectWrapper = new ObjectWrapper();
        emptyResponse.setData(objectWrapper);
        Mockito.when(
                vaultTemplate.read(vaultKeyValueStore.fullComponentKey(KEY), ObjectWrapper.class))
                .thenReturn(emptyResponse);
        return vaultKeyValueStore.get(KEY);
    }

    private Optional<String> setupNullResponse() {
        VaultResponseSupport<ObjectWrapper> emptyResponse = new VaultResponseSupport<>();
        Mockito.when(
                vaultTemplate.read(vaultKeyValueStore.fullComponentKey(KEY), ObjectWrapper.class))
                .thenReturn(emptyResponse);
        return vaultKeyValueStore.get(KEY);
    }

    /**
     * Test that getting a non-existing key returns an empty optional.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetWitNonExistingKey() throws Exception {
        Optional<String> ret = vaultKeyValueStore.get(KEY);
        assertFalse(ret.isPresent());
    }

    /**
     * Test that the store reports a key when it is present
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWithKey() throws Exception {
        Mockito.when(
                vaultTemplate.read(vaultKeyValueStore.fullComponentKey(KEY), ObjectWrapper.class))
                .thenReturn(responseSupport);

        assertTrue(vaultKeyValueStore.containsKey(KEY));
    }

    /**
     * Test that the store reports no key when it is absent
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWithoutKey() throws Exception {
        setupMissingKey();
        assertFalse(vaultKeyValueStore.containsKey(KEY));
    }

    /**
     * Test that the store reports no key when the store returns a null response.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWhenNull() throws Exception {
        setupNullResponse();
        Assert.assertFalse(vaultKeyValueStore.containsKey(KEY));
    }

    /**
     * Test that remove calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRemove() throws Exception {
        vaultKeyValueStore.removeKeysWithPrefix(KEY);
        Mockito.verify(vaultTemplate).delete(vaultKeyValueStore.fullComponentKey(KEY));
    }

    /**
     * Test that removeKey calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRemoveKey() throws Exception {
        vaultKeyValueStore.removeKey(KEY);
        Mockito.verify(vaultTemplate).delete(vaultKeyValueStore.fullComponentKey(KEY));
    }

    /**
     * Basic test for the get by prefix function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetByPrefix() throws Exception {
        List<String> respKeys = Lists.newArrayList();
        IntStream.range(0, 5).forEach(num -> {
            final String key = String.valueOf(num);
            respKeys.add(key);
            VaultResponseSupport<ObjectWrapper> responseSupport = new VaultResponseSupport<>();
            ObjectWrapper objectWrapper = new ObjectWrapper(KEY, VALUE + key);
            responseSupport.setData(objectWrapper);
            Mockito.when(vaultTemplate.read(vaultKeyValueStore.fullComponentKey(KEY + "/" + key),
                    ObjectWrapper.class)).thenReturn(responseSupport);
        });
        // key
        Mockito.when(vaultTemplate.list(vaultKeyValueStore.fullComponentKey(KEY)))
                .thenReturn(respKeys);

        Map<String, String> ret = vaultKeyValueStore.getByPrefix(KEY);
        IntStream.range(0, 5).forEach(num -> {
            String key = String.valueOf(num);
            assertTrue(ret.containsKey(key));
            assertEquals(VALUE + num, ret.get(key));
        });
    }

    /**
     * Test that when an exception occurs the key value store retries until
     * success.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetServiceDown() throws Exception {
        Mockito.when(
                vaultTemplate.read(vaultKeyValueStore.fullComponentKey(KEY), ObjectWrapper.class))
                .thenThrow(new VaultException("down"))
                .thenReturn(responseSupport);
        Assert.assertEquals(VALUE, vaultKeyValueStore.get(KEY).get());
    }

    /**
     * Test that when an exception occurs the key value store retries until
     * success.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPutServiceDown() throws Exception {
        VaultResponse response = new VaultResponse();
        Mockito.when(vaultTemplate.write(vaultKeyValueStore.fullComponentKey(KEY), VALUE))
                .thenThrow(new ResourceAccessException("down"))
                .thenThrow(new VaultException("down again"))
                .thenReturn(response);
        vaultKeyValueStore.put(KEY, VALUE);
    }

    /**
     * Test constructor - positive path.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testValidConstructor() throws Exception {
        new VaultKeyValueStore(vaultTemplate, KV, COMPONENT_TYPE, 501, TimeUnit.MILLISECONDS);
    }

    /**
     * Test constructor - negative path, name space is empty.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSpacesOnlyNamespace() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new VaultKeyValueStore(vaultTemplate, "  ", COMPONENT_TYPE, 501, TimeUnit.MILLISECONDS);
    }

    /**
     * Test constructor - negative path, retry interval value is less then 500 mills.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testIllegalRetryInterval() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new VaultKeyValueStore(vaultTemplate, KV, COMPONENT_TYPE, 499, TimeUnit.MILLISECONDS);
    }

    /**
     * Test constructor - negative path, namespace has slash.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testNamespaceSlashesPath() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new VaultKeyValueStore(vaultTemplate, "me/you", COMPONENT_TYPE, 501, TimeUnit.MILLISECONDS);
    }

    /**
     * Test constructor - negative path, component type has slash.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testComponentTypeSlashesPath() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new VaultKeyValueStore(vaultTemplate, "you", "api/1", 501, TimeUnit.MILLISECONDS);
    }

    /**
     * Test constructor - negative path, namespace has back slash.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testNamespaceBackSlashPath() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new VaultKeyValueStore(vaultTemplate, "me\\you", COMPONENT_TYPE, 501, TimeUnit.MILLISECONDS);
    }

    /**
     * Test when working thread is interrupted, put operation will stop.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testInterruptPutExitsThread() throws Exception {
        Mockito.when(vaultTemplate.write(Mockito.any(), Mockito.any()))
                .thenThrow(new VaultException("down"));

        Thread putThread = new Thread(() -> vaultKeyValueStore.put(KEY, VALUE));
        putThread.start();

        Thread.sleep(retryIntervalMillis);
        putThread.interrupt();
        putThread.join(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
    }

    /**
     * Test when working thread is interrupted, delete operation will stop.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testInterruptRemoveExitsThread() throws Exception {
        Mockito.doThrow(new VaultException("down")).when(vaultTemplate).delete(any());

        Thread removeThread = new Thread(() -> vaultKeyValueStore.removeKeysWithPrefix(KEY));
        removeThread.start();

        Thread.sleep(retryIntervalMillis);
        removeThread.interrupt();
        removeThread.join(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
    }

    /**
     * Test when working thread is interrupted, get operation will stop.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testInterruptGetExitsThread() throws Exception {
        Mockito.when(vaultTemplate.read(any())).thenThrow(new VaultException("down"));

        Thread putThread = new Thread(() -> vaultKeyValueStore.get(KEY));
        putThread.start();

        Thread.sleep(retryIntervalMillis);
        putThread.interrupt();
        putThread.join(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
    }
}
