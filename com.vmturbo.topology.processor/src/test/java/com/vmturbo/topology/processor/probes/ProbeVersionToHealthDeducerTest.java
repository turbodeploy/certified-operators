package com.vmturbo.topology.processor.probes;

import java.util.Arrays;
import java.util.Collection;

import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.topology.processor.api.ProbeInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.probes.ProbeVersionFactory.ProbeVersionErrorMessage;
import com.vmturbo.topology.processor.probes.ProbeVersionFactory.ServerVersionErrorMessage;

import common.HealthCheck.HealthState;

/**
 * {@link ProbeVersionToHealthDeducerTest} has a set of test cases for the "deduceProbeHealth()"
 * method in the {@link ProbeVersionFactory} class.
 */
@RunWith(value = Parameterized.class)
public class ProbeVersionToHealthDeducerTest {
    private final String probeVersion;
    private final String serverVersion;
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
                { "", "", HealthState.MAJOR, ServerVersionErrorMessage.MISSING.getMessage("") },
                { "", "test-version", HealthState.MAJOR, ServerVersionErrorMessage.CUSTOM.getMessage("test-version") },
                { "", "8.3.2", HealthState.MAJOR, ProbeVersionErrorMessage.MISSING.getMessage("", "8.3.2") },
                { "8.3.2", "8.3.2", HealthState.NORMAL, "" },
                { "8.3.2.1", "8.3.2", HealthState.NORMAL, "" },
                { "8.3.2-SNAPSHOT", "8.3.2", HealthState.NORMAL, "" },
                { "8.3.2", "8.3.2-SNAPSHOT", HealthState.NORMAL, "" },
                { "8.3.1", "8.3.2", HealthState.MAJOR, ProbeVersionErrorMessage.OLDER.getMessage("8.3.1", "8.3.2") },
                { "8.2.5", "8.3.2", HealthState.MAJOR, ProbeVersionErrorMessage.OLDER.getMessage("8.2.5", "8.3.2") },
                { "8.0.1", "8.3.2", HealthState.MAJOR, ProbeVersionErrorMessage.OLDER.getMessage("8.0.1", "8.3.2") },
                { "8.3.3-SNAPSHOT", "8.3.2", HealthState.MINOR, ProbeVersionErrorMessage.NEWER.getMessage("8.3.3-SNAPSHOT", "8.3.2") },
                { "test-version", "8.3.2", HealthState.MINOR, ProbeVersionErrorMessage.CUSTOM.getMessage("test-version", "8.3.2") },
                { "8.3.9", "8.4.0", HealthState.MAJOR, ProbeVersionErrorMessage.OLDER.getMessage("8.3.9", "8.4.0") },
                { "8.7.3", "8.7.5", HealthState.MAJOR, ProbeVersionErrorMessage.OLDER.getMessage("8.7.3", "8.7.5") + " " + ProbeVersionFactory.CloudNativeAdditionalErrorMessage.CPU_THROTTLING_BREAKING_CHANGE_MESSAGE.getMessage()},
                { "8.7.3", "8.7.4", HealthState.MAJOR, ProbeVersionErrorMessage.OLDER.getMessage("8.7.3", "8.7.4") },
                { "8.7.5", "8.7.6", HealthState.MAJOR, ProbeVersionErrorMessage.OLDER.getMessage("8.7.5", "8.7.6") },
        });
    }

    /**
     * Construct a test using the following input parameters.
     *
     * @param probeVersion the version of the probe registration
     * @param serverVersion the version of the server
     * @param expectedHealthState the expected health state
     * @param expectedMessage the expected message associated with the health state
     */
    public ProbeVersionToHealthDeducerTest(final String probeVersion,
            final String serverVersion,
            final HealthState expectedHealthState,
            final String expectedMessage) {
        this.probeVersion = probeVersion;
        this.serverVersion = serverVersion;
        this.expectedHealthState = expectedHealthState;
        this.expectedMessage = expectedMessage;
    }

    /**
     * Test the probe version to health deducer factory method.
     */
    @Test
    public void testDeduceProbeHealthFromVersion() {
        final Pair<HealthState, String> health = ProbeVersionFactory.deduceProbeHealth(probeVersion, serverVersion,
            MediationMessage.ProbeInfo.newBuilder().setProbeType("").setProbeCategory("").setProbeTargetInfo(
                MediationMessage.ProbeTargetInfo.newBuilder().addInputValues(
                    Discovery.AccountValue.newBuilder().setKey("image").setStringValue("kubeturbo").build()).build()).build());
        Assert.assertEquals(expectedHealthState, health.getFirst());
        Assert.assertEquals(expectedMessage, health.getSecond());
    }
}
