package com.vmturbo.topology.processor.probes;

import static com.vmturbo.topology.processor.probes.ProbeVersionFactory.PROBE_VERSION_INCOMPATIBLE_MESSAGE;
import static com.vmturbo.topology.processor.probes.ProbeVersionFactory.PROBE_VERSION_NEWER_MESSAGE;
import static com.vmturbo.topology.processor.probes.ProbeVersionFactory.PROBE_VERSION_ONE_BEHIND_MESSAGE;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * {@link ProbeVersionToHealthDeducerTest} has a set of test cases for the "deduceProbeHealth()"
 * method in the {@link ProbeVersionFactory} class.
 */
@RunWith(value = Parameterized.class)
public class ProbeVersionToHealthDeducerTest {
    private final String probeVersion;
    private final String platformVersion;
    private final HealthState expectedHealthState;
    private final String expectedMessage;

    /**
     * Test cases.
     *
     * @return the collection of test cases.
     */
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "", "", HealthState.MAJOR, "Probe has invalid version (no major version): " },
                { "8.3.2", "", HealthState.MAJOR, "Platform has invalid version (no major version): " },
                { "8.3.2", "8.3.2", HealthState.NORMAL, "" },
                { "8.3.2.1", "8.3.2", HealthState.NORMAL, "" },
                { "8.3.1", "8.3.2", HealthState.MINOR, PROBE_VERSION_ONE_BEHIND_MESSAGE },
                { "8.2.5", "8.3.2", HealthState.MAJOR, PROBE_VERSION_INCOMPATIBLE_MESSAGE },
                { "8.0.1", "8.3.2", HealthState.MAJOR, PROBE_VERSION_INCOMPATIBLE_MESSAGE },
                { "8.3.3-SNAPSHOT", "8.3.2", HealthState.MINOR, PROBE_VERSION_NEWER_MESSAGE },
                { "test-version", "8.3.2", HealthState.MAJOR, "Probe has invalid version (no major version): test-version" },
                { "8.3.9", "8.4.0", HealthState.MAJOR, PROBE_VERSION_INCOMPATIBLE_MESSAGE },
        });
    }

    /**
     * Construct a test using the following input parameters.
     *
     * @param probeVersion the version of the probe registration
     * @param platformVersion the version of the platform
     * @param expectedHealthState the expected health state
     * @param expectedMessage the expected message associated with the health state
     */
    public ProbeVersionToHealthDeducerTest(final String probeVersion,
            final String platformVersion,
            final HealthState expectedHealthState,
            final String expectedMessage) {
        this.probeVersion = probeVersion;
        this.platformVersion = platformVersion;
        this.expectedHealthState = expectedHealthState;
        this.expectedMessage = expectedMessage;
    }

    /**
     * Test the probe version to health deducer factory method.
     */
    @Test
    public void testDeduceProbeHealthFromVersion() {
        final Pair<HealthState, String> health = ProbeVersionFactory.deduceProbeHealth(probeVersion, platformVersion);
        Assert.assertEquals(expectedHealthState, health.getFirst());
        Assert.assertEquals(expectedMessage, health.getSecond());
    }
}
