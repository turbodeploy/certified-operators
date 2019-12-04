package com.vmturbo.utilprobe.component.service;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Test for {@link ConsulManagementService}.
 */
public class ConsulManagementServiceTest {

    private KeyValueStore store;

    private static final String PROBE_KV_STORE_PREFIX = "probes/";
    private static final String KEY1 = "Key1";
    private static final String KEY2 = "Key2";
    private static final String TYPE1 = "Type1";
    private static final String TYPE2 = "Type2";
    private static final String CATEGORY1 = "Category1";
    private static final String CATEGORY2 = "Category2";

    /**
     * Setting up store.
     *
     * @throws InvalidProtocolBufferException
     *      exception while converting {@link ProbeInfo} to JSON.
     */
    @Before
    public void setUp() throws InvalidProtocolBufferException {
        store = mock(KeyValueStore.class);
        Map<String, String> probes = new HashMap<>();
        ProbeInfo info1 = ProbeInfo
                .newBuilder()
                .setProbeCategory(CATEGORY1)
                .setProbeType(TYPE1)
                .build();
        ProbeInfo info2 = ProbeInfo
                .newBuilder()
                .setProbeCategory(CATEGORY2)
                .setProbeType(TYPE2)
                .build();
        probes.put(KEY1, JsonFormat.printer().print(info1));
        probes.put(KEY2, JsonFormat.printer().print(info2));
        when(store.getByPrefix(PROBE_KV_STORE_PREFIX)).thenReturn(probes);
        when(store.get(KEY1)).thenReturn(Optional.of(info1.toString()));
        when(store.get(KEY2)).thenReturn(Optional.of(info2.toString()));
        doNothing().when(store).removeKey(anyString());
    }

    /**
     * Deleting config test.
     */
    @Test
    public void testDeleteConfig() {
        ConsulManagementService service = new ConsulManagementService(store);
        service.deleteConfig(CATEGORY1, TYPE1);
        verify(store).removeKey(KEY1);
        service.deleteConfig(CATEGORY1, TYPE2);
        verify(store, atMost(1)).removeKey(anyString());
    }
}