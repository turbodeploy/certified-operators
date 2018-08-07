package com.vmturbo.topology.processor.group.settings;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class ResizeIncrementAdjustorTest {

    @Test
    public void testRoundToProbeIncrementEqual() {
        float userRequestedIncrement = 1024f;
        float probeProvidedIncrement = 1024f;
        assertThat(ResizeIncrementAdjustor.roundToProbeIncrement(userRequestedIncrement, probeProvidedIncrement),
                is(userRequestedIncrement));
    }

    @Test
    public void testRoundToProbeIncrementEqualFloat() {
        float userRequestedIncrement = 1024.3f;
        float probeProvidedIncrement = 1024.3f;
        assertThat(ResizeIncrementAdjustor.roundToProbeIncrement(userRequestedIncrement, probeProvidedIncrement),
                is(userRequestedIncrement));
    }

    /**
     * Make sure that in the case where we would normally round down to zero, we round up to
     * 1 * probeProvidedIncrement. This is because if we round down to 0 there won't be any resizes.
     */
    @Test
    public void testRoundToProbeIncrementNoZeroIncrement() {
        float userRequestedIncrement = 1;
        float probeProvidedIncrement = 1024;
        assertThat(ResizeIncrementAdjustor.roundToProbeIncrement(userRequestedIncrement, probeProvidedIncrement),
                is(probeProvidedIncrement));
    }

    @Test
    public void testRoundToProbeIncrementRoundsDown() {
        float userRequestedIncrement = 1280;
        float probeProvidedIncrement = 1024;
        assertThat(ResizeIncrementAdjustor.roundToProbeIncrement(userRequestedIncrement, probeProvidedIncrement),
                is(probeProvidedIncrement));
    }

    @Test
    public void testRoundToProbeIncrementRoundsDownToNearestMultiple() {
        float userRequestedIncrement = 1024 * 5 + 900;
        float probeProvidedIncrement = 1024;
        assertThat(ResizeIncrementAdjustor.roundToProbeIncrement(userRequestedIncrement, probeProvidedIncrement),
                is(1024.0f * 5));
    }
}
