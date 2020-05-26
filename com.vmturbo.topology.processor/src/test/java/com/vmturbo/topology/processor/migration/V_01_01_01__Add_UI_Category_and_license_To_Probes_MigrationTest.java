package com.vmturbo.topology.processor.migration;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Test probe migration.
 */
public class V_01_01_01__Add_UI_Category_and_license_To_Probes_MigrationTest {

    private static final String PROBE_ID = "73432431951280";
    private static final Map<String, String> PROBES =
            ImmutableMap.of(ProbeStore.PROBE_KV_STORE_PREFIX + PROBE_ID,
                    "{\"probeType\": \"AWS\",\"probeCategory\": \"Cloud Management\","
                            + "\"supplyChainDefinitionSet\": []}");

    private V_01_01_00__Add_UI_Category_and_license_To_Probes_Migration migration;
    private final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);

    /**
     * Set up method.
     */
    @Before
    public void setUp() {
        Mockito.when(kvStore.getByPrefix(ProbeStore.PROBE_KV_STORE_PREFIX))
                .thenReturn(PROBES);
        migration = new V_01_01_00__Add_UI_Category_and_license_To_Probes_Migration(kvStore);
    }

    /**
     * Check that migration process is completed successfully.
     *
     * @throws JsonProcessingException in case of problems with parsing
     */
    @Test
    public void testProbeMigration() throws JsonProcessingException {
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

        MigrationProgressInfo progressInfo = migration.doStartMigration();

        Assert.assertEquals(MigrationStatus.SUCCEEDED, progressInfo.getStatus());
        verify(kvStore, times(1)).put(keyCaptor.capture(),
                valueCaptor.capture());
        Assert.assertEquals(keyCaptor.getValue(), ProbeStore.PROBE_KV_STORE_PREFIX + PROBE_ID);

        Map<String, Object> probeProperties =
                new ObjectMapper().readValue(valueCaptor.getValue(), new TypeReference<Map<String, Object>>() {});
        Assert.assertEquals("Public Cloud", probeProperties.get("uiProbeCategory"));
        Assert.assertEquals("public_cloud", probeProperties.get("license"));
    }
}
