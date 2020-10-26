package com.vmturbo.topology.processor.migration;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test class for {@link V_01_01_05__Target_Common_Proxy_Settings}.
 */
public class V_01_01_05__Target_Common_Proxy_SettingsTest {

    /**
     * Tests renaming of defined fields: in a probe info and in a target values.
     *
     * @throws Exception in case of any exception.
     */
    @Test
    public void testMigration() throws Exception {
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        long probeId = 73685959586368L;
        ProbeInfo awsProbeInfo = createDefaultAwsProbeInfo();

        Mockito.when(probeStore.getProbe(probeId)).thenReturn(Optional.of(awsProbeInfo));
        Target awsTargetNoRenamingFields = createAwsTargetNoRenamingFields(probeStore, probeId);
        Mockito.when(targetStore.getProbeTargets(probeId))
                .thenReturn(Collections.singletonList(awsTargetNoRenamingFields));

        Mockito.when(probeStore.getProbeInfoForType(Mockito.anyString())).thenAnswer(invocation -> {
            if (invocation.getArguments()[0].equals(SDKProbeType.AWS.getProbeType())) {
                return Optional.of(awsProbeInfo);
            }
            return Optional.empty();
        });
        Mockito.when(probeStore.getProbeIdForType(Mockito.anyString())).thenAnswer(invocation -> {
            if (invocation.getArguments()[0].equals(SDKProbeType.AWS.getProbeType())) {
                return Optional.of(probeId);
            }
            return Optional.empty();
        });

        new V_01_01_05__Target_Common_Proxy_Settings(targetStore, probeStore, Mockito.mock(GroupScopeResolver.class))
                .doStartMigration();

        ArgumentCaptor<ProbeInfo> probeInfoCaptor = ArgumentCaptor.forClass(ProbeInfo.class);
        ArgumentCaptor<TargetSpec> targetSpecCaptor = ArgumentCaptor.forClass(TargetSpec.class);
        Mockito.verify(probeStore).updateProbeInfo(probeInfoCaptor.capture());
        Mockito.verify(targetStore).restoreTarget(Mockito.anyLong(), targetSpecCaptor.capture());

        // Verify
        validateMigratedProbeInfo(probeInfoCaptor.getValue());
        validateMigratedTargetSpec(targetSpecCaptor.getValue());

    }

    private void validateMigratedTargetSpec(TargetSpec spec) {
        Assert.assertEquals(5, spec.getAccountValueList().size());
        Stream.of("proxyHost", "proxyPort", "proxyUsername", "secureProxy")
                .forEach(name -> Assert.assertEquals(1, spec.getAccountValueList()
                        .stream()
                        .filter(val -> val.getKey().equals(name))
                        .count()));
    }

    private void validateMigratedProbeInfo(ProbeInfo info) {
        Assert.assertEquals(5, info.getAccountDefinitionList().size());
        Stream.of("proxyHost", "proxyPort", "proxyUsername", "secureProxy")
                .forEach(name -> Assert.assertEquals(1, info.getAccountDefinitionList()
                        .stream()
                        .filter(def -> def.getCustomDefinition().getName().equals(name))
                        .count()));
    }

    private ProbeInfo createDefaultAwsProbeInfo() {
        return ProbeInfo.newBuilder()
                .setProbeType("AWS")
                .setProbeCategory("Cloud Management")
                .addTargetIdentifierField("address")
                .addAccountDefinition(Discovery.AccountDefEntry.newBuilder()
                        .setMandatory(false)
                        .setCustomDefinition(Discovery.CustomAccountDefEntry.newBuilder()
                                .setName("proxy")
                                .setDisplayName("Proxy Host")
                                .setDescription("Proxy Host")
                                .build())
                        .build())
                .addAccountDefinition(Discovery.AccountDefEntry.newBuilder()
                        .setMandatory(false)
                        .setCustomDefinition(Discovery.CustomAccountDefEntry.newBuilder()
                                .setName("port")
                                .setDisplayName("Proxy Port")
                                .setDescription("Proxy Port")
                                .build())
                        .build())
                .addAccountDefinition(Discovery.AccountDefEntry.newBuilder()
                        .setMandatory(false)
                        .setCustomDefinition(Discovery.CustomAccountDefEntry.newBuilder()
                                .setName("proxyUser")
                                .setDisplayName("Proxy User")
                                .setDescription("Proxy User")
                                .build())
                        .build())
                .addAccountDefinition(Discovery.AccountDefEntry.newBuilder()
                        .setMandatory(true)
                        .setIsTargetDisplayName(true)
                        .setCustomDefinition(Discovery.CustomAccountDefEntry.newBuilder()
                                .setName("address")
                                .setDisplayName("Custom Target Name")
                                .setDescription("Enter a unique target name.")
                                .build())
                        .build())
                .build();
    }

    private Target createAwsTargetNoRenamingFields(ProbeStore probeStore, long probeId) throws Exception {
        TargetSpec spec = TargetSpec.newBuilder()
                .setProbeId(probeId)
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("address")
                        .setStringValue("vmturboamp.aws.amazon.com")
                        .build())
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("proxy")
                        .setStringValue("10.10.168.168")
                        .build())
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("port")
                        .setStringValue("8081")
                        .build())
                .addAccountValue(TopologyProcessorDTO.AccountValue.newBuilder()
                        .setKey("proxyUser")
                        .setStringValue("turbouser")
                        .build())
                .build();
        return new Target(0L, probeStore, spec, true);
    }
}
