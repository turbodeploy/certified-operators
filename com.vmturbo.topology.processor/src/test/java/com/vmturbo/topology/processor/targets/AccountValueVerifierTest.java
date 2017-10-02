package com.vmturbo.topology.processor.targets;

import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Unit test for {@link AccountValueVerifier}.
 */
public class AccountValueVerifierTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
}
