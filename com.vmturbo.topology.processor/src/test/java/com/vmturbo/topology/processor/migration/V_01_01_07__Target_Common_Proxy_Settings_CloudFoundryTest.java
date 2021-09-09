package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_01_05__Target_Common_Proxy_SettingsTest.createAccountDefEntry;

import java.time.Clock;
import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test class for {@link V_01_01_07__Target_Common_Proxy_Settings_CloudFoundry}.
 */
public class V_01_01_07__Target_Common_Proxy_Settings_CloudFoundryTest {

    private static final String ADDRESS = "10.10.168.200";
    private static final String PORT = "9091";
    private static final String PROXY_HOST = "some-proxy.com";
    private static final String PROXY_PORT = "8183";

    private ProbeInfo createCloudFoundryProbeInfo() {
        return ProbeInfo.newBuilder().setProbeType(SDKProbeType.CLOUD_FOUNDRY.getProbeType()).setProbeCategory("Cloud Native")
                .addTargetIdentifierField("address")
                .addAccountDefinition(createAccountDefEntry(true, "address", "Address"))
                .addAccountDefinition(createAccountDefEntry(false, "proxy", "Proxy Host"))
                .addAccountDefinition(createAccountDefEntry(false, "proxyPort", "Proxy Port"))
                .build();
    }

    private Target createCloudFoundryTarget(ProbeStore probeStore, long probeId) throws Exception  {
        TargetSpec spec = TargetSpec.newBuilder()
                .setProbeId(probeId)
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("address")
                        .setStringValue(ADDRESS)
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
        return new Target(0L, probeStore, spec, true, true, Clock.systemUTC());
    }

    /**
     * Test the migration of the Cloud Foundry probe and target.
     *
     * @throws Exception in case of any exception.
     */
    @Test
    public void testCloudFoundryMigration() throws Exception {
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        long probeId = 100L;
        ProbeInfo probeInfo = createCloudFoundryProbeInfo( );
        Mockito.when(probeStore.getProbe(probeId)).thenReturn(Optional.of(probeInfo));
        Target target = createCloudFoundryTarget(probeStore, probeId);
        Mockito.when(targetStore.getProbeTargets(probeId)).thenReturn(Collections.singletonList(target));
        Mockito.when(probeStore.getProbeInfoForType(Mockito.anyString())).thenAnswer(invocation -> {
            final Object requestedType = invocation.getArguments()[0];
            if (requestedType.equals(SDKProbeType.CLOUD_FOUNDRY.getProbeType())) {
                return Optional.of(probeInfo);
            }
            return Optional.empty();
        });
        Mockito.when(probeStore.getProbeIdForType(Mockito.anyString())).thenAnswer(invocation -> {
            final Object requestedType = invocation.getArguments()[0];
            if (requestedType.equals(SDKProbeType.CLOUD_FOUNDRY.getProbeType())) {
                return Optional.of(probeId);
            }
            return Optional.empty();
        });
        new V_01_01_07__Target_Common_Proxy_Settings_CloudFoundry(targetStore, probeStore,
                Mockito.mock(GroupScopeResolver.class)).doStartMigration();

        ArgumentCaptor<ProbeInfo> probeInfoCaptor = ArgumentCaptor.forClass(ProbeInfo.class);
        ArgumentCaptor<TargetSpec> targetSpecCaptor = ArgumentCaptor.forClass(TargetSpec.class);

        Mockito.verify(probeStore).updateProbeInfo(probeInfoCaptor.capture());
        Mockito.verify(targetStore).restoreTarget(Mockito.anyLong(), targetSpecCaptor.capture());

        // Migrated Probe Info
        ProbeInfo migratedProbeInfo = probeInfoCaptor.getValue();

        // Check that 'proxy' is migrated
        Assert.assertNotNull(migratedProbeInfo.getAccountDefinitionList().stream()
                .filter(def -> def.getCustomDefinition().getName().equals("proxyHost")).findFirst().orElse(null));
        Assert.assertNull(migratedProbeInfo.getAccountDefinitionList()
                .stream().filter(def -> def.getCustomDefinition().getName().equals("proxy")).findFirst().orElse(null));

        // Check all other fields are saved
        Assert.assertEquals(1, migratedProbeInfo.getAccountDefinitionList().stream()
                .filter(def -> def.getCustomDefinition().getName().equals("address"))
                .count());

        // Migrated Target values
        TargetSpec migratedTarget = targetSpecCaptor.getValue();

        // Check that 'proxy' is migrated
        Assert.assertNull(migratedTarget.getAccountValueList().stream()
                .filter(val -> val.getKey().equals("proxy")).findFirst().orElse(null));
        Assert.assertEquals(PROXY_HOST, migratedTarget.getAccountValueList().stream()
                .filter(val -> val.getKey().equals("proxyHost")).map(v -> v.getStringValue()).findFirst().orElse(null));
    }
}
