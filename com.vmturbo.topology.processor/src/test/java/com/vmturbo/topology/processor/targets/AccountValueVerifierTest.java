package com.vmturbo.topology.processor.targets;

import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Unit test for {@link AccountValueVerifier}.
 */
public class AccountValueVerifierTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String DEPENDENT_KEY = "dependentKey";
    private final CustomAccountDefEntry customAccountDefEntry = CustomAccountDefEntry.newBuilder()
            .setIsSecret(true)
            .setName(PredefinedAccountDefinition.Password.getValueKey())
            .setDisplayName("Password")
            .setDescription("sampleDescription")
            .build();
    private final CustomAccountDefEntry accountDefEntry = CustomAccountDefEntry.newBuilder()
            .setName(DEPENDENT_KEY)
            .setDisplayName("sample displayName")
            .setDescription("sample description")
            .build();

    /**
     * Test cover all the predefined values verifications. In case of some verification errors, we'd
     * get {@link InvalidTargetException}. If there is some verifiers are missing, we'll get
     * {@link NullPointerException} instead.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testPredefinedVerifiers() throws Exception {
        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        final ProbeInfo.Builder probeInfo = ProbeInfo.newBuilder(Probes.emptyProbe);
        for (PredefinedAccountDefinition pad : PredefinedAccountDefinition.values()) {
            probeInfo.addAccountDefinition(
                    AccountDefEntry.newBuilder().setPredefinedDefinition(pad.name()));
        }
        Mockito.when(probeStore.getProbe(Mockito.anyLong()))
                .thenReturn(Optional.of(probeInfo.build()));
        final TargetSpec spec = TargetSpec.newBuilder().setProbeId(0).build();
        expectedException.expect(InvalidTargetException.class);
        AccountValueVerifier.validate(spec, probeStore);
    }

    /**
     * Test to validate secret fields is PRESENT with matching dependent key.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testValidatingSecretKeysWhenSecretFieldPresent() throws Exception {
        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        final ProbeInfo.Builder probeInfo = ProbeInfo.newBuilder().setProbeType(SDKProbeType.AWS.getProbeType());
        probeInfo.addAccountDefinition(AccountDefEntry.newBuilder().setCustomDefinition(accountDefEntry));
        probeInfo.setProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setDependencyKey(DEPENDENT_KEY)
                        .setDependencyValue("false")
                        .setCustomDefinition(customAccountDefEntry)
                        .build());
        Mockito.when(probeStore.getProbe(Mockito.anyLong()))
                .thenReturn(Optional.of(probeInfo.build()));
        final TargetSpec spec = TargetSpec.newBuilder().setProbeId(0)
                .addAccountValue(AccountValue.newBuilder().setKey(PredefinedAccountDefinition.Password.getValueKey())
                        .setStringValue("test").build())
                .addAccountValue(AccountValue.newBuilder()
                        .setKey(DEPENDENT_KEY)
                        .setStringValue("false").build()).build();
        AccountValueVerifier.validate(spec, probeStore);
        //no exception show the validation was successful.
    }

    /**
     * Test to validate secret fields can be absent if dependent key values are a mismatch.
     * eg : DEPENDENT_KEY is true for the mandatory field but input for DEPENDENT_KEY is actually false;
     * so we do not need to validate the mandatory secret field.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testValidatingSecretKeysWhenDependentKeyMismatch() throws Exception {
        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        final ProbeInfo.Builder probeInfo = ProbeInfo.newBuilder().setProbeType(SDKProbeType.AWS.getProbeType());
        probeInfo.addAccountDefinition(AccountDefEntry.newBuilder().setCustomDefinition(accountDefEntry));
        probeInfo.setProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setDependencyKey(DEPENDENT_KEY)
                        .setDependencyValue("false")
                        .setCustomDefinition(customAccountDefEntry)
                        .build());
        Mockito.when(probeStore.getProbe(Mockito.anyLong()))
                .thenReturn(Optional.of(probeInfo.build()));
        final TargetSpec spec = TargetSpec.newBuilder().setProbeId(0)
                .addAccountValue(AccountValue.newBuilder()
                        .setKey(DEPENDENT_KEY)
                        .setStringValue("true").build()).build();
        AccountValueVerifier.validate(spec, probeStore);
        //no exception show the validation was successful.
    }

    /**
     *  Test to validate absence of secret fields causes exception if dependent key values are a match.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testValidatingSecretFieldAbsent() throws Exception {
        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        final ProbeInfo.Builder probeInfo = ProbeInfo.newBuilder().setProbeType(SDKProbeType.AWS.getProbeType());
        probeInfo.addAccountDefinition(AccountDefEntry.newBuilder().setCustomDefinition(accountDefEntry));
        probeInfo.setProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setDependencyKey(DEPENDENT_KEY)
                        .setDependencyValue("true")
                        .setCustomDefinition(customAccountDefEntry)
                        .build());
        Mockito.when(probeStore.getProbe(Mockito.anyLong()))
                .thenReturn(Optional.of(probeInfo.build()));
        final TargetSpec spec = TargetSpec.newBuilder().setProbeId(0)
                .addAccountValue(AccountValue.newBuilder()
                        .setKey(DEPENDENT_KEY)
                        .setStringValue("true").build()).build();
        expectedException.expect(InvalidTargetException.class);
        AccountValueVerifier.validate(spec, probeStore);
    }
}
