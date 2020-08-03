package com.vmturbo.kvstore;

import static com.vmturbo.kvstore.ConsulKeyValueStore.emptyResponse;
import static org.mockito.Matchers.any;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.ecwid.consul.ConsulException;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.ecwid.consul.v1.kv.model.GetValue;

import org.apache.http.conn.ConnectTimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests for consul key value store.
 */
public class ConsulKeyValueStoreTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private final long timeout = 1000L;
    private KeyValueClient keyValueClient;
    private ConsulKeyValueStore consulKeyValueStore;
    private static final String USER_IN_CHINESE = "用户";

    @Before
    public void setup() {
        keyValueClient = Mockito.mock(KeyValueClient.class);
        consulKeyValueStore =
                new ConsulKeyValueStore(keyValueClient, "test", timeout, TimeUnit.MILLISECONDS);
        Mockito.when(keyValueClient.setKVValue(any(), any())).thenReturn(emptyResponse);
        Mockito.when(keyValueClient.deleteKVValues(any())).thenReturn(emptyResponse);
        Mockito.when(keyValueClient.deleteKVValue(any())).thenReturn(emptyResponse);
    }

    /**
     * Test that the decode function works properly.
     */
    @Test
    public void testDecodeBase64() {
        byte[] base64String = Base64.getEncoder().encode("hello world".getBytes());
        Assert.assertEquals("hello world",
                ConsulKeyValueStore.decodeBase64(new String(base64String)));
    }

    /**
     * Test that the decode function works with non-ASCII characters.
     */
    @Test
    public void testDecodeBase64Globalization() {
       byte[] base64String = Base64.getEncoder().encode(USER_IN_CHINESE.getBytes());
        Assert.assertEquals(USER_IN_CHINESE, ConsulKeyValueStore.decodeBase64(new String(base64String,
                StandardCharsets.UTF_8)));
    }

    /**
     * Test that the put function calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPut() throws Exception {
        consulKeyValueStore.put("test", "val");
        Mockito.verify(keyValueClient).setKVValue("test/test", "val");
    }

    /**
     * Test that the get function decodes the value and returns it.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGet() throws Exception {
        GetValue respVal = new GetValue();
        respVal.setKey("test/test");
        respVal.setValue(new String(Base64.getEncoder().encode("val".getBytes())));
        Mockito.when(keyValueClient.getKVValue(Mockito.eq("test/test")))
                .thenReturn(new Response<>(respVal, 0L, false, 0L));
        Optional<String> ret = consulKeyValueStore.get("test");
        Assert.assertEquals("val", ret.get());
    }

    /**
     * Test that a key with whitespace is handled properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testWhitespace() throws Exception {
        consulKeyValueStore.put("test/test test", "val");
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(keyValueClient).setKVValue(keyCaptor.capture(), any());
        // Check that the key maps to a URI successfully.
        // If no exception gets thrown we're good!
        URI.create(keyCaptor.getValue());
    }

    /**
     * Test that getting a non-existing key returns an empty optional.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetNotExisting() throws Exception {
        Mockito.when(keyValueClient.getKVValue(Mockito.eq("test/test")))
                .thenReturn(new Response<>(null, 0L, false, 0L));
        Optional<String> ret = consulKeyValueStore.get("test");
        Assert.assertFalse(ret.isPresent());
    }

    /**
     * Test that the store reports a key when it is present
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWithKey() throws Exception {
        Mockito.when(keyValueClient.getKVKeysOnly(Mockito.eq("test/test")))
                .thenReturn(new Response<>(Arrays.asList("foo", "bar"), 0L, false, 0L));

        Assert.assertTrue(consulKeyValueStore.containsKey("test"));
    }

    /**
     * Test that the store reports no key when it is absent
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWithoutKey() throws Exception {
        Mockito.when(keyValueClient.getKVKeysOnly(Mockito.eq("test/test")))
                .thenReturn(new Response<>(Collections.emptyList(), 0L, false, 0L));

        Assert.assertFalse(consulKeyValueStore.containsKey("test"));
    }

    /**
     * Test that the store reports no key when the store returns a null response.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testContainsKeyWhenNull() throws Exception {
        Mockito.when(keyValueClient.getKVKeysOnly(Mockito.eq("test/test")))
                .thenReturn(new Response<>(null, 0L, false, 0L));

        Assert.assertFalse(consulKeyValueStore.containsKey("test"));
    }

    /**
     * Test that remove calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRemove() throws Exception {
        consulKeyValueStore.removeKeysWithPrefix("test");
        Mockito.verify(keyValueClient).deleteKVValues("test/test");
    }

    /**
     * Test that removeKey calls the right underlying function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRemoveKey() throws Exception {
        consulKeyValueStore.removeKey("test");
        Mockito.verify(keyValueClient).deleteKVValue("test/test");
    }

    /**
     * Basic test for the get by prefix function.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetByPrefix() throws Exception {
        List<GetValue> respVals = new ArrayList<>();
        IntStream.range(0, 5).forEach(num -> {
            GetValue val = new GetValue();
            val.setKey("test/test" + num);
            val.setValue(new String(Base64.getEncoder().encode(("val" + num).getBytes())));
            respVals.add(val);
        });
        Mockito.when(keyValueClient.getKVValues(Mockito.eq("test/test")))
                .thenReturn(new Response<>(respVals, 0L, false, 0L));
        Map<String, String> ret = consulKeyValueStore.getByPrefix("test");
        IntStream.range(0, 5).forEach(num -> {
            String key = "test" + num;
            Assert.assertTrue(ret.containsKey(key));
            Assert.assertEquals("val" + num, ret.get(key));
        });
    }

    /**
     * Test that when an exception occurs the key value store retries until success.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetServiceDown() throws Exception {
        GetValue respVal = new GetValue();
        respVal.setKey("test/test");
        respVal.setValue(new String(Base64.getEncoder().encode("val".getBytes())));

        Mockito.when(keyValueClient.getKVValue(any()))
                .thenThrow(new ConsulException(new ConnectException()))
                .thenReturn(new Response<>(respVal, 0L, false, 0L));
        Assert.assertEquals("val", consulKeyValueStore.get("test").get());
    }

    /**
     * Test that when an exception occurs the key value store retries until timeout which is wrapped
     * in {@link KeyValueStoreOperationException}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = KeyValueStoreOperationException.class)
    public void testPutServiceDown() throws Exception {
        Mockito.when(keyValueClient.setKVValue(any(), any()))
                .thenThrow(new ConsulException(new ConnectException()))
                .thenThrow(new ConsulException(new SocketTimeoutException()))
                .thenThrow(new ConsulException(new ConnectTimeoutException()))
                .thenReturn(new Response<>(true, 0L, false, 0L));
        consulKeyValueStore.put("test", "val");
    }

    @Test
    public void testSpacesOnlyNamespace() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new ConsulKeyValueStore(keyValueClient, "  ", 10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testIllegalRetryInterval() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new ConsulKeyValueStore(keyValueClient, "  ", 0, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSlashesNamespace() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new ConsulKeyValueStore(keyValueClient, "me/you", 10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSlashesBackslash() throws Exception {
        exception.expect(IllegalArgumentException.class);
        new ConsulKeyValueStore(keyValueClient, "me\\you", 10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testInterruptPutExitsThread() throws Exception {
        Mockito.when(keyValueClient.setKVValue(any(), any()))
                .thenThrow(new ConsulException(new ConnectException()));

        Thread putThread = new Thread(() -> consulKeyValueStore.put("test", "val"));
        putThread.start();

        Thread.sleep(timeout);
        putThread.interrupt();
        putThread.join(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS));
    }
}
