package com.vmturbo.topology.processor.communication.it;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.mediation.common.tests.util.ProbeConfig;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Integration tests to check, whether probes are actually registered within topology processor.
 */
public class ProbeRegistrationTestIT extends AbstractIntegrationTest {

    /**
     * Test for registration of a single SDK probe component.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testSimpleRegistration() throws Exception {
        final SdkProbe probe = new SdkProbe(ProbeConfig.Empty, "empty-probe");
        final SdkContainer container = startSdkComponent(probe);
        container.awaitRegistered();
    }

    /**
     * Tests for registration of different probes with different types. They all should be
     * registered as a result.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testSeveralContainersRegistration() throws Exception {
        final SdkProbe probe1 = new SdkProbe(ProbeConfig.Empty, "first-probe");
        final SdkProbe probe2 = new SdkProbe(ProbeConfig.Failing, "second-probe");
        final SdkContainer container1 = startSdkComponent(probe1);
        final SdkContainer container2 = startSdkComponent(probe2);
        container1.awaitRegistered();
        container2.awaitRegistered();
        verifyAllRegistered(Arrays.asList(probe1, probe2));
    }

    /**
     * Tests several similar probe with the different type. All the probes are expected to be
     * registered.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testSameProbeDifferentTypes() throws Exception {
        final SdkProbe probe1 = new SdkProbe(ProbeConfig.Empty, "first-probe");
        final SdkProbe probe2 = new SdkProbe(ProbeConfig.Empty, "second-probe");
        final SdkContainer container1 = startSdkComponent(probe1);
        final SdkContainer container2 = startSdkComponent(probe2);
        container1.awaitRegistered();
        container2.awaitRegistered();
        verifyAllRegistered(Arrays.asList(probe1, probe2));
    }

    /**
     * Tests several identical probes with identical type. They all should be registered as one
     * probe.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testSameProbeSameTypes() throws Exception {
        final SdkProbe probe1 = new SdkProbe(ProbeConfig.Empty, "spst");
        final SdkProbe probe2 = new SdkProbe(ProbeConfig.Empty, "spst");
        final SdkContainer container1 = startSdkComponent(probe1);
        final SdkContainer container2 = startSdkComponent(probe2);
        container1.awaitRegistered();
        container2.awaitRegistered();
        verifyAllRegistered(Arrays.asList(probe1));
    }

    private void verifyAllRegistered(Collection<SdkProbe> expectedProbes) {
        final Set<String> probeTypes =
                        new HashSet<String>(Collections2.transform(expectedProbes,
                                        new Function<SdkProbe, String>() {
                                            @Override
                                            public String apply(SdkProbe input) {
                                                return input.getType();
                                            }
                                        }));
        final Set<String> registeredTypes =
                        new HashSet<String>(Collections2.transform(getRemoteMediation()
                                        .getConnectedProbes(), new Function<ProbeInfo, String>() {
                            public String apply(ProbeInfo input) {
                                return input.getProbeType();
                            }
                        }));
        Assert.assertEquals(probeTypes, registeredTypes);
    }
}
