package com.vmturbo.topology.processor.discoverydumper;

import javax.annotation.Nonnull;

/**
 * Interface to provide information about SDK target dumping configuration.
 *
 * <p>This interface was adapted from the same-named interface in Classic.</p>
 */
public interface TargetDumpingSettings {
    /**
     * Returns the number of dumps to store for the specified target. This means, that number of
     * dumps, held by mediation server should not be target than this value.
     *
     * @param targetId target identifier
     * @return maximum number of dumps to store (default is 0)
     */
    int getDumpsToHold(@Nonnull String targetId);

    /**
     * Refresh dump settings from their authoritatve source.
     */
    void refreshSettings();
}
