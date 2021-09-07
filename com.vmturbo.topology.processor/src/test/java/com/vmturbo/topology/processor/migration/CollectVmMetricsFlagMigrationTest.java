package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_01_05__Target_Common_Proxy_SettingsTest.createAccountDefEntry;

import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test 'Virtual Machine metrics' flag migration.
 */
public class CollectVmMetricsFlagMigrationTest {

    private static final String ADDRESS = "address";
    private static final String PORT = "8080";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    private static final String PROPERTY_NAME = "collectVmMetrics";
    private static final String PROPERTY_DISPLAY_NAME = "Collect Virtual Machine Metrics";
    private static final String PROPERTY_DESCRIPTION =
                    "Overwrite Hypervisor or Cloud Provider Virtual Machine metrics "
                    + "with data from the target";
    private static final Boolean PROPERTY_DEFAULT = Boolean.FALSE;
    private static final String REGEX = "(true|false)";
    private static final String PROBE_TYPE = "probeType";

    /**
     * Verify that new account value and definition {@code collectVmMetrics} are added into targets
     * and probe info.
     *
     * @throws Exception in case of any exception.
     */
    @Test
    public void testMigration() throws Exception {
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        long probeId = 100L;
        MediationMessage.ProbeInfo probeInfo = createProbeInfo();
        Mockito.when(probeStore.getProbe(probeId)).thenReturn(Optional.of(probeInfo));
        Target target = createTargetInfo(probeStore, probeId);
        Mockito.when(targetStore.getProbeTargets(probeId)).thenReturn(
                        Collections.singletonList(target));
        Mockito.when(probeStore.getProbeInfoForType(Mockito.anyString())).thenAnswer(invocation -> {
            final Object requestedType = invocation.getArguments()[0];
            if (requestedType.equals(PROBE_TYPE)) {
                return Optional.of(probeInfo);
            }
            return Optional.empty();
        });
        Mockito.when(probeStore.getProbeIdForType(Mockito.anyString())).thenAnswer(invocation -> {
            final Object requestedType = invocation.getArguments()[0];
            if (requestedType.equals(PROBE_TYPE)) {
                return Optional.of(probeId);
            }
            return Optional.empty();
        });

        // Run migration
        new TestMigration(targetStore, probeStore).doStartMigration();

        ArgumentCaptor<ProbeInfo> probeInfoCaptor = ArgumentCaptor.forClass(ProbeInfo.class);
        ArgumentCaptor<TopologyProcessorDTO.TargetSpec> targetSpecCaptor
                        = ArgumentCaptor.forClass(TopologyProcessorDTO.TargetSpec.class);

        Mockito.verify(probeStore).updateProbeInfo(probeInfoCaptor.capture());
        Mockito.verify(targetStore).restoreTarget(Mockito.anyLong(), targetSpecCaptor.capture());

        // Migrated Probe Info
        ProbeInfo migratedProbeInfo = probeInfoCaptor.getValue();

        // Check that existing values are not changed.
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList()
                             .stream()
                             .filter(def -> def.getCustomDefinition().getName().equals("address"))
                             .findFirst().orElse(null));
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList()
                             .stream()
                             .filter(def -> def.getCustomDefinition().getName().equals("port"))
                             .findFirst().orElse(null));
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList()
                             .stream()
                             .filter(def -> def.getCustomDefinition().getName().equals("username"))
                             .findFirst().orElse(null));
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList()
                             .stream()
                             .filter(def -> def.getCustomDefinition().getName().equals("password"))
                             .findFirst().orElse(null));

        // Check the new account definition 'Collect VM Metrics'
        final Discovery.AccountDefEntry collectVmMetric =
                    migratedProbeInfo.getAccountDefinitionList()
                        .stream()
                        .filter(def -> def.getCustomDefinition().getName().equals(PROPERTY_NAME))
                        .findFirst()
                        .orElse(null);
        Assert.assertNotNull(collectVmMetric);
        final Discovery.CustomAccountDefEntry definition = collectVmMetric.getCustomDefinition();
        Assert.assertEquals(PROPERTY_DISPLAY_NAME, definition.getDisplayName());
        Assert.assertEquals(PROPERTY_DESCRIPTION, definition.getDescription());
        Assert.assertEquals(REGEX, definition.getVerificationRegex());


        // Migrated Target values
        TopologyProcessorDTO.TargetSpec migratedTarget = targetSpecCaptor.getValue();

        // Check that existing values are not changed.
        Assert.assertEquals(ADDRESS, migratedTarget.getAccountValueList().stream()
                        .filter(val -> val.getKey().equals("address"))
                        .map(AccountValue::getStringValue)
                        .findFirst().orElse(null));
        Assert.assertEquals(PORT, migratedTarget.getAccountValueList().stream()
                        .filter(val -> val.getKey().equals("port"))
                        .map(AccountValue::getStringValue)
                        .findFirst().orElse(null));
        Assert.assertEquals(USERNAME, migratedTarget.getAccountValueList().stream()
                        .filter(val -> val.getKey().equals("username"))
                        .map(AccountValue::getStringValue)
                        .findFirst().orElse(null));
        Assert.assertEquals(PASSWORD, migratedTarget.getAccountValueList().stream()
                        .filter(val -> val.getKey().equals("password"))
                        .map(AccountValue::getStringValue)
                        .findFirst().orElse(null));

        // Check the new account value 'Collect VM Metrics'
        Assert.assertEquals(PROPERTY_DEFAULT.toString(), migratedTarget.getAccountValueList()
                        .stream()
                        .filter(val -> val.getKey().equals(PROPERTY_NAME))
                        .map(AccountValue::getStringValue)
                        .findFirst().orElse(null));
    }

    private ProbeInfo createProbeInfo() {
        return ProbeInfo.newBuilder().setProbeType(PROBE_TYPE)
                        .setProbeCategory("APPLICATIONS_AND_DATABASES")
                        .addTargetIdentifierField("address").addTargetIdentifierField("port")
                        .addAccountDefinition(createAccountDefEntry(true, "address", "Address"))
                        .addAccountDefinition(createAccountDefEntry(true, "port", "Port"))
                        .addAccountDefinition(createAccountDefEntry(true, "username", "Username"))
                        .addAccountDefinition(createAccountDefEntry(true, "password", "Password"))
                        .build();
    }

    private Target createTargetInfo(ProbeStore probeStore, long probeId) throws Exception  {
        TargetSpec spec = TargetSpec.newBuilder().setProbeId(probeId)
                        .addAccountValue(AccountValue.newBuilder().setKey("address")
                                             .setStringValue(ADDRESS)
                                             .build())
                        .addAccountValue(AccountValue.newBuilder().setKey("port")
                                             .setStringValue(PORT)
                                             .build())
                        .addAccountValue(AccountValue.newBuilder().setKey("username")
                                             .setStringValue(USERNAME)
                                             .build())
                        .addAccountValue(AccountValue.newBuilder().setKey("password")
                                             .setStringValue(PASSWORD)
                                             .build())
                        .build();
        return new Target(0L, probeStore, spec, true, true);
    }

    private static class TestMigration extends AbstractCollectVmMetricFlagMigration {

        /**
         * Constructor.
         * @param targetStore target store.
         * @param probeStore  probe store.
         */
        protected TestMigration(TargetStore targetStore, ProbeStore probeStore) {
            super(targetStore, probeStore, PROBE_TYPE);
        }
    }
}
