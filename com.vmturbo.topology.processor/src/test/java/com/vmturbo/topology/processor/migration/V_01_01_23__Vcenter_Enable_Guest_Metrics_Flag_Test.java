package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.ACCOUNT_VALUE;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.ACCOUNT_VALUE_KEY;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.ACCOUNT_VALUE_VALUE;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.GUEST_METRICS;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.PROBE_ID;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.TARGET_ID;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.TARGET_INFO;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.TARGET_NAME;
import static com.vmturbo.topology.processor.migration.V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag.TARGET_SPEC;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountTypeTest.FakeKeyValueStore;

/**
 * Test the migration of enabling guest metrics for vcenter targets.
 */
public class V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag_Test {

    private static final String VCENTER_PROBE_ID = "74747115995552";
    private static final String NON_VC_PROBE_ID = "74747115995551";

    private static final JsonObject VCENTER_ENABLED = new JsonObject();
    private static final JsonObject VCENTER_NOT_ENABLED = new JsonObject();
    private static final JsonObject VCENTER_MISSING = new JsonObject();
    private static final JsonObject NON_VCENTER = new JsonObject();

    private static final Set<SDKProbeType> VCENTER_ONLY = ImmutableSet.of(SDKProbeType.VCENTER);

    static {
        VCENTER_ENABLED.addProperty(TARGET_ID, "1");
        VCENTER_ENABLED.addProperty(TARGET_NAME, "vcenterEnabled");
        VCENTER_ENABLED.add(TARGET_SPEC, createSpec(VCENTER_PROBE_ID, createGuestMetricsAcountValue(true)));

        VCENTER_NOT_ENABLED.addProperty(TARGET_ID, "2");
        VCENTER_NOT_ENABLED.addProperty(TARGET_NAME, "vcenterNotEnabled");
        VCENTER_NOT_ENABLED.add(TARGET_SPEC, createSpec(VCENTER_PROBE_ID, createGuestMetricsAcountValue(false)));

        NON_VCENTER.addProperty(TARGET_ID, "3");
        NON_VCENTER.addProperty(TARGET_NAME, "nonVcenter");
        NON_VCENTER.add(TARGET_SPEC, createSpec(NON_VC_PROBE_ID, new JsonArray()));

        VCENTER_MISSING.addProperty(TARGET_ID, "4");
        VCENTER_MISSING.addProperty(TARGET_NAME, "missingGuestMetricsVcenter");
        VCENTER_MISSING.add(TARGET_SPEC, createSpec(VCENTER_PROBE_ID, new JsonArray()));
    }

    private KeyValueStore keyValueStore;

    /**
     * Create KV store before each test case.
     */
    @Before
    public void setup() {
        keyValueStore = Mockito.spy(setupKvStore());
        Mockito.reset(keyValueStore);
    }

    /**
     * Test that only vcenter probe is migrated.
     */
    @Test
    public void testShouldUpdateProbe() {
        final V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag migration =
                new V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag(
                        new FakeKeyValueStore(Collections.emptyMap()), VCENTER_ONLY);
        assertTrue(migration.shouldUpdateProbe("", SDKProbeType.VCENTER.toString(), null));
        assertFalse(migration.shouldUpdateProbe("", "", null));
    }

    /**
     * There should be no changes to a vCenter target that already has the guest metrics already
     * enabled.
     */
    @Test
    public void testNoChangeForGuestMetricsEnabledVCenter() {
        insertTargetKey(keyValueStore, VCENTER_ENABLED);
        final V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag migration =
                new V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag(keyValueStore, VCENTER_ONLY);

        migration.doStartMigration();

        Mockito.verify(keyValueStore, Mockito.times(1)).put(Mockito.anyString(),
                Mockito.anyString());
        checkGuestMetricEnabled(keyValueStore, VCENTER_ENABLED.get(TARGET_ID).getAsString());
    }

    /**
     * There should be no changes to a non vCenter target.
     */
    @Test
    public void testNoChangeForNonVCenter() {
        insertTargetKey(keyValueStore, NON_VCENTER);
        final V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag migration =
                new V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag(keyValueStore, VCENTER_ONLY);

        migration.doStartMigration();

        Mockito.verify(keyValueStore, Mockito.times(1)).put(Mockito.anyString(),
                Mockito.anyString());
    }

    /**
     * Guest metrics flag should be set to true when a vCenter target has it disabled.
     */
    @Test
    public void testChangeForGuestMetricsDisabledVCenter() {
        insertTargetKey(keyValueStore, VCENTER_NOT_ENABLED);
        final V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag migration =
                new V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag(keyValueStore, VCENTER_ONLY);

        migration.doStartMigration();

        Mockito.verify(keyValueStore, Mockito.times(2)).put(Mockito.anyString(),
                Mockito.anyString());
        checkGuestMetricEnabled(keyValueStore, VCENTER_NOT_ENABLED.get(TARGET_ID).getAsString());
    }

    /**
     * Guest metrics flag should be set to true when a vCenter target is missing the account value entirely.
     */
    @Test
    public void testChangeForGuestMetricsMissingVCenter() {
        insertTargetKey(keyValueStore, VCENTER_MISSING);
        final V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag migration =
                new V_01_01_23__Vcenter_Enable_Guest_Metrics_Flag(keyValueStore, VCENTER_ONLY);

        migration.doStartMigration();

        Mockito.verify(keyValueStore, Mockito.times(2)).put(Mockito.anyString(),
                Mockito.anyString());
        checkGuestMetricEnabled(keyValueStore, VCENTER_MISSING.get(TARGET_ID).getAsString());
    }

    private static void checkGuestMetricEnabled(@Nonnull KeyValueStore keyValueStore,
            @Nonnull String targetId) {
        final Optional<String> result = keyValueStore.get("targets/" + targetId);
        assertTrue(result.isPresent());
        assertTrue(result.get().contains(createGuestMetricsAcountValue(true).toString().replaceAll("\"", "\\\\\"")));
    }

    private static FakeKeyValueStore setupKvStore() {
        final FakeKeyValueStore keyValueStore = new FakeKeyValueStore(new HashMap<>());
        insertProbeKey(keyValueStore, VCENTER_PROBE_ID, SDKProbeType.VCENTER.getProbeType());
        insertProbeKey(keyValueStore, NON_VC_PROBE_ID, SDKProbeType.VCD.getProbeType());
        return keyValueStore;
    }

    private static void insertProbeKey(@Nonnull KeyValueStore kvStore, @Nonnull String probeId,
            @Nonnull String probeType) {
        kvStore.put("probes/" + probeId, String.format("{\"probeType\": \"%s\"}", probeType));
    }

    private static void insertTargetKey(@Nonnull KeyValueStore kvStore,
            @Nonnull JsonObject targetInfo) {
        final JsonObject targetInfoWrapper = new JsonObject();
        targetInfoWrapper.addProperty(TARGET_INFO, targetInfo.toString());
        kvStore.put("targets/" + targetInfo.get(TARGET_ID).getAsString(), "{}" + targetInfoWrapper);
    }

    private static JsonArray createGuestMetricsAcountValue(boolean enabled) {
        final JsonArray aV = new JsonArray();
        final JsonObject guestM = new JsonObject();
        guestM.addProperty(ACCOUNT_VALUE_KEY, GUEST_METRICS);
        guestM.addProperty(ACCOUNT_VALUE_VALUE, String.valueOf(enabled));
        aV.add(guestM);
        return aV;
    }

    private static JsonObject createSpec(@Nonnull String probeId, @Nonnull JsonArray accountValue) {
        final JsonObject spec = new JsonObject();
        spec.addProperty(PROBE_ID, probeId);
        spec.add(ACCOUNT_VALUE, accountValue);
        return spec;
    }
}
