package com.vmturbo.topology.processor.group.settings;

import com.google.common.base.Preconditions;

/**
 * A utility class to help with managing resize increments during settings application.
 *
 * Extracted into it's own class mainly for ease of testing.
 */
class ResizeIncrementAdjustor {

    private ResizeIncrementAdjustor() {}

    /**
     * Round the resize increment requested by the user to the nearest multiple of the resize
     * increment allowed by the probe.
     *
     * The reason we need to do this is because if the user specifies an increment that's different
     * from the probe-allowed increment, the market will recomend resize actions that aren't
     * executable by the probe.
     *
     * We round DOWN in all cases, EXCEPT when rounding down would result in an increment of 0.
     * This is because a resize increment of 0 would effectively disable resizes.
     *
     * @param userRequestedIncrement The resize increment requested by the user.
     * @param probeProvidedIncrement The resize increment allowed by the probe.
     * @return The resize increment to use for the commodity in the topology.
     */
    public static float roundToProbeIncrement(final double userRequestedIncrement,
                                              final float probeProvidedIncrement) {
        if (probeProvidedIncrement <= 0) {
            return (float)userRequestedIncrement;
        }
        // First, find the amount of "probe increments" that the user-requested increment can
        // be divided into. Round down, because that will give a more "conservative" adjusted
        // increment.
        final float userIncrementInUnitsOfProbeIncrement =
                // Casting to float shouldn't lose accuracy, because floor rounds to integer.
                (float)Math.floor(userRequestedIncrement / probeProvidedIncrement);
        if (userIncrementInUnitsOfProbeIncrement == 0) {
            // If the user increment < probe increment, return the probe increment.
            // This is the one case where we "round up".
            return probeProvidedIncrement;
        } else {
            return userIncrementInUnitsOfProbeIncrement * probeProvidedIncrement;
        }
    }
}
