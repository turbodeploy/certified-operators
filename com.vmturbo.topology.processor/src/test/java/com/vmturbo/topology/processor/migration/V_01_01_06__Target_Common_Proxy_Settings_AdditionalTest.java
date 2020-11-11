package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_01_05__Target_Common_Proxy_SettingsTest.createAccountDefEntry;

import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test class for {@link V_01_01_06__Target_Common_Proxy_Settings_Additional}.
 */
public class V_01_01_06__Target_Common_Proxy_Settings_AdditionalTest {

    private static final String ADDRESS = "10.10.168.200";
    private static final String PORT = "9091";
    private static final String PROXY_HOST = "some-proxy.com";
    private static final String PROXY_PORT = "8183";

    /**
     * Test the migration of the Intersight probe and target.
     *
     * @throws Exception in case of any exception.
     */
    @Test
    public void testIntersightMigrationServer() throws Exception {
        testIntersightMigrationCommon(SDKProbeType.INTERSIGHT);
    }

    /**
     * Test the migration of the Intersight UCS probe and target.
     *
     * @throws Exception in case of any exception.
     */
    @Test
    public void testIntersightMigrationUcs() throws Exception {
        testIntersightMigrationCommon(SDKProbeType.INTERSIGHT_UCS);
    }

    /**
     * Test the migration of the Intersight Hyperflex probe and target.
     *
     * @throws Exception in case of any exception.
     */
    @Test
    public void testIntersightMigrationHx() throws Exception {
        testIntersightMigrationCommon(SDKProbeType.INTERSIGHT_HX);
    }

    private MediationMessage.ProbeInfo createIntersightProbeInfo(String type, String category) {
        return MediationMessage.ProbeInfo.newBuilder().setProbeType(type).setProbeCategory(category)
                .addTargetIdentifierField("address").addTargetIdentifierField("port")
                .addAccountDefinition(createAccountDefEntry(true, "address", "Address"))
                .addAccountDefinition(createAccountDefEntry(true, "port", "Port"))
                .addAccountDefinition(createAccountDefEntry(false, "clientId", "Client ID"))
                .addAccountDefinition(createAccountDefEntry(false, "clientSecret", "Client secret"))
                .addAccountDefinition(createAccountDefEntry(false, "moid", "Moid"))
                .addAccountDefinition(createAccountDefEntry(false, "connectionId", "Connection ID"))
                .addAccountDefinition(createAccountDefEntry(false, "deviceIPAddress", "Device IP Address"))
                .addAccountDefinition(createAccountDefEntry(false, "proxy", "Proxy Host"))
                .addAccountDefinition(createAccountDefEntry(false, "proxyPort", "Proxy Port"))
                .build();
    }

    private Target createIntersightTarget(ProbeStore probeStore, long probeId) throws Exception  {
        TopologyProcessorDTO.TargetSpec spec = TopologyProcessorDTO.TargetSpec.newBuilder()
                .setProbeId(probeId)
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("address")
                        .setStringValue(ADDRESS)
                        .build())
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("port")
                        .setStringValue(PORT)
                        .build())
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("proxy")
                        .setStringValue(PROXY_HOST)
                        .build())
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("proxyPort")
                        .setStringValue(PROXY_PORT)
                        .build())
                .build();
        return new Target(0L, probeStore, spec, true);
    }

    private void testIntersightMigrationCommon(SDKProbeType type) throws Exception {
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        long probeId = 100L;
        MediationMessage.ProbeInfo probeInfo = createIntersightProbeInfo(type.getProbeType(), "HYPERCONVERGED");
        Mockito.when(probeStore.getProbe(probeId)).thenReturn(Optional.of(probeInfo));
        Target target = createIntersightTarget(probeStore, probeId);
        Mockito.when(targetStore.getProbeTargets(probeId)).thenReturn(Collections.singletonList(target));
        Mockito.when(probeStore.getProbeInfoForType(Mockito.anyString())).thenAnswer(invocation -> {
            final Object requestedType = invocation.getArguments()[0];
            if (requestedType.equals(type.getProbeType())) {
                return Optional.of(probeInfo);
            }
            return Optional.empty();
        });
        Mockito.when(probeStore.getProbeIdForType(Mockito.anyString())).thenAnswer(invocation -> {
            final Object requestedType = invocation.getArguments()[0];
            if (requestedType.equals(type.getProbeType())) {
                return Optional.of(probeId);
            }
            return Optional.empty();
        });
        new V_01_01_06__Target_Common_Proxy_Settings_Additional(targetStore, probeStore, Mockito.mock(GroupScopeResolver.class))
                .doStartMigration();

        ArgumentCaptor<MediationMessage.ProbeInfo> probeInfoCaptor = ArgumentCaptor.forClass(MediationMessage.ProbeInfo.class);
        ArgumentCaptor<TopologyProcessorDTO.TargetSpec> targetSpecCaptor = ArgumentCaptor.forClass(TopologyProcessorDTO.TargetSpec.class);

        Mockito.verify(probeStore).updateProbeInfo(probeInfoCaptor.capture());
        Mockito.verify(targetStore).restoreTarget(Mockito.anyLong(), targetSpecCaptor.capture());

        // Migrated Probe Info
        MediationMessage.ProbeInfo migratedProbeInfo = probeInfoCaptor.getValue();

        // 'proxyPort' is not changed
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList()
                .stream().filter(def -> def.getCustomDefinition().getName().equals("proxyPort"))
                .findFirst().orElse(null));

        // 'port' is not changed
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList()
                .stream().filter(def -> def.getCustomDefinition().getName().equals("port"))
                .findFirst().orElse(null));

        // 'proxy' is removed
        Assert.assertNull(migratedProbeInfo.getAccountDefinitionList()
                .stream().filter(def -> def.getCustomDefinition().getName().equals("proxy"))
                .findFirst().orElse(null));

        // 'proxyHost' is added
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList()
                .stream().filter(def -> def.getCustomDefinition().getName().equals("proxyHost"))
                .findFirst().orElse(null));

        // Migrated Target values
        TopologyProcessorDTO.TargetSpec migratedTarget = targetSpecCaptor.getValue();

        // 'proxyPort' is not changed
        Assert.assertEquals(PROXY_PORT, migratedTarget.getAccountValueList().stream()
                .filter(val -> val.getKey().equals("proxyPort")).map(val -> val.getStringValue())
                .findFirst().orElse(null));

        // 'port' is not changed
        Assert.assertEquals(PORT, migratedTarget.getAccountValueList().stream()
                .filter(val -> val.getKey().equals("port")).map(val -> val.getStringValue())
                .findFirst().orElse(null));

        // 'proxy' is renamed to 'proxyHost'
        Assert.assertEquals(PROXY_HOST, migratedTarget.getAccountValueList().stream()
                .filter(val -> val.getKey().equals("proxyHost")).map(val -> val.getStringValue())
                .findFirst().orElse(null));

        // 'proxy' does not exist
        Assert.assertNull(migratedTarget.getAccountValueList().stream()
                .filter(val -> val.getKey().equals("proxy")).map(val -> val.getStringValue())
                .findFirst().orElse(null));

    }
}
