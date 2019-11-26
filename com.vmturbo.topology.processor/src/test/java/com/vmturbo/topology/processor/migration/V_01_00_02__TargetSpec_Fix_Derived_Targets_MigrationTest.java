package com.vmturbo.topology.processor.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.StringReader;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test that V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration migrates the saved targets
 * correctly.
 */
public class V_01_00_02__TargetSpec_Fix_Derived_Targets_MigrationTest {
    private static final String TARGET_INFO = "targetInfo";

    private static final String TARGET_SPEC = "spec";

    private static final String DERIVED_TARGETS = "derivedTargetIds";

    private static final String PARENT_ID = "parentId";

    private static final String TARGET1_ID = "73223381526944";

    private static final String TARGET2_ID = "73223381044480";

    private static final Map<String, String> TARGET_ID_TO_SAVED_TARGET_STRING =
        ImmutableMap.of(TargetStore.TARGET_KV_STORE_PREFIX + TARGET1_ID,
            "{\"secretFields\":[\"AAAAAQAAACD/mUOc/tHxCj3yweRAdhvODP89viW"
            + "Tn1mTJLHQ3kY1xwAAABAzd9psp2iDAJqWn8f/3EfdAAAAGBUoeOOFY5GnZ7i1ITGvP4w6sR9KfP14AQ"
            + "\\u003d\\u003d\"]}{\"targetInfo\":\"{\\n  \\\"id\\\": \\\"73223381526944\\\",\\n  "
            + "\\\"spec\\\": {\\n    \\\"probeId\\\": \\\"73219641736400\\\",\\n    "
            + "\\\"accountValue\\\": [{\\n      \\\"key\\\": \\\"password\\\",\\n      "
            + "\\\"stringValue\\\": \\\"AAAAAQAAACADzjY6LlFCXdgBSPIJrsS9w3Oyrn07wqp5xS+Ak88CtwA"
            + "AABBFQ+N8AA1kucTWK2Hilf5qAAAAIIoO7+QmO5oKllHbjDBG5t5qrIcb00R4sM1IqDjLx1bB\\\"\\n"
            + "    }, {\\n      \\\"key\\\": \\\"address\\\",\\n      \\\"stringValue\\\": "
            + "\\\"vsphere-dc17.eng.vmturbo.com\\\"\\n    }, {\\n      \\\"key\\\": "
            + "\\\"username\\\",\\n      \\\"stringValue\\\": \\\"corp\\\\\\\\ron.even\\\"\\n"
            + "    }],\\n    \\\"parentId\\\": \\\"73223381044480\\\",\\n    \\\"isHidden\\\":"
            + " true,\\n    \\\"readOnly\\\": false\\n  },\\n  "
            + "\\\"displayName\\\": \\\"vsphere-dc17.eng.vmturbo.com\\\"\\n}\"}",
            TargetStore.TARGET_KV_STORE_PREFIX + TARGET2_ID,
            "{\"secretFields\":[\"AAAAAQAAACBBTRjAd98C+VTj1OYSHdeG/t2dPyva0IMoPcc5BM3q5QAAABCs"
                + "OYrgxe2vnBl9nAv3DmLdAAAAGJn/Bwdk6A8DbibnPdd15dLNl9EVVIeYaw\\u003d\\u003d\"]}"
                + "{\"targetInfo\":\"{\\n  \\\"id\\\": \\\"73223381044480\\\",\\n  \\\"spec\\\": "
                + "{\\n    \\\"probeId\\\": \\\"73219641867392\\\",\\n    \\\"accountValue\\\": "
                + "[{\\n      \\\"key\\\": \\\"username\\\",\\n      \\\"stringValue\\\": \\\""
                + "corp\\\\\\\\ron.even\\\"\\n    }, {\\n      \\\"key\\\": \\\"password\\\","
                + "\\n      \\\"stringValue\\\": \\\"AAAAAQAAACB8Y4V1r5iDodp6W1I/qNe3CEzMAKTNkDP"
                + "YwZaKtYpJowAAABDuxf+ZjxFDV6JQ9nfHKxzhAAAAIM0na9nELbpsul6iyYeBx7NvXB15g/qkWJMx"
                + "R+kbWDrw\\\"\\n    }, {\\n      \\\"key\\\": \\\"isStorageBrowsingEnabled\\\","
                + "\\n      \\\"stringValue\\\": \\\"true\\\"\\n    }, {\\n      \\\"key\\\": \\\""
                + "address\\\",\\n      \\\"stringValue\\\": \\\"vsphere-dc17.eng.vmturbo.com\\\""
                + "\\n    }],\\n    \\\"isHidden\\\": false,\\n    \\\"readOnly\\\": false,\\n    "
                + "\\\"derivedTargetIds\\\": [\\\"73223381526944\\\"]\\n  },\\n  \\\""
                + "displayName\\\": \\\"vsphere-dc17.eng.vmturbo.com\\\"\\n}\"}");
    private V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration migration;
    private final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);

    /**
     * Setup the migration and the kvStore mock.
     */
    @Before
    public void setup() {
        migration = new V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration(kvStore);
        Mockito.when(kvStore.getByPrefix(TargetStore.TARGET_KV_STORE_PREFIX))
            .thenReturn(TARGET_ID_TO_SAVED_TARGET_STRING);
        migration = new V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration(kvStore);
    }

    /**
     * Test that the migration gets rid of parentId field in the saved target in the kvStore and
     * that other fields of the target remain the same.
     */
    @Test
    public void testMigrationOfParentTargetId() {
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        MigrationProgressInfo progress = migration.startMigration();
        verify(kvStore, times(2)).put(keyCaptor.capture(),
            valueCaptor.capture());
        assertThat(keyCaptor.getAllValues(),
            containsInAnyOrder(TargetStore.TARGET_KV_STORE_PREFIX + TARGET1_ID,
                TargetStore.TARGET_KV_STORE_PREFIX + TARGET2_ID));
        assertEquals(MigrationStatus.SUCCEEDED, progress.getStatus());
        for (int i = 0; i < keyCaptor.getAllValues().size(); i++) {
            verifyConvertedString(keyCaptor.getAllValues().get(i),
                valueCaptor.getAllValues().get(i));
        }
    }

    private void verifyConvertedString(@Nonnull String key, @Nonnull String migratedString) {
        String originalString = TARGET_ID_TO_SAVED_TARGET_STRING.get(key);
        assertNotNull(originalString);
        JsonReader originalReader = new JsonReader(new StringReader(originalString));
        JsonObject secretFieldsOriginal = new JsonParser().parse(originalReader).getAsJsonObject();
        JsonObject targetJsonOriginal = new JsonParser().parse(originalReader).getAsJsonObject();
        JsonPrimitive targetInfoOriginal = targetJsonOriginal.getAsJsonPrimitive(TARGET_INFO);
        JsonReader reader2Original = new JsonReader(new StringReader(targetInfoOriginal.getAsString()));
        JsonObject targetInfoObjectOriginal = new JsonParser().parse(reader2Original).getAsJsonObject();
        JsonObject specOriginal = targetInfoObjectOriginal.getAsJsonObject(TARGET_SPEC);
        JsonReader migratedReader = new JsonReader(new StringReader(migratedString));
        JsonObject secretFieldsMigrated = new JsonParser().parse(migratedReader).getAsJsonObject();
        JsonObject targetJsonMigrated = new JsonParser().parse(migratedReader).getAsJsonObject();
        JsonPrimitive targetInfoMigrated = targetJsonMigrated.getAsJsonPrimitive(TARGET_INFO);
        JsonReader reader2Migrated = new JsonReader(new StringReader(targetInfoMigrated.getAsString()));
        JsonObject targetInfoObjectMigrated = new JsonParser().parse(reader2Migrated).getAsJsonObject();
        JsonObject specMigrated = targetInfoObjectMigrated.getAsJsonObject(TARGET_SPEC);
        assertEquals(secretFieldsOriginal, secretFieldsMigrated);
        assertTrue(targetInfoObjectOriginal.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(TARGET_SPEC))
            .allMatch(entry ->
                entry.getValue().equals(targetInfoObjectMigrated.get(entry.getKey()))));
        assertTrue(specOriginal.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(PARENT_ID)
                && !entry.getKey().equals(DERIVED_TARGETS))
            .allMatch(entry -> entry.getValue().equals(specMigrated.get(entry.getKey()))));
        assertNull(specMigrated.get(PARENT_ID));
        if (specOriginal.get(DERIVED_TARGETS) == null) {
            assertNull(specMigrated.get(DERIVED_TARGETS));
        } else {
            assertEquals(
                Sets.newHashSet(specOriginal.get(DERIVED_TARGETS).getAsJsonArray().iterator()),
                Sets.newHashSet(specMigrated.get(DERIVED_TARGETS).getAsJsonArray().iterator()));
        }
    }
}
