package com.vmturbo.stitching.cpucapacity;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Provide access to the capacity performance analysis library 'com.turbonomic.cpucapacity' which
 * uses the SPEC benchmarks to more accurately give performance capacity for specific CPU models.
 **/
public interface CpuCapacityStore {
    /**
     * Return an Optional containing the scalingFactor for the given 'cpuModel' string if found,
     * or Optional.empty() if not found.
     *
     * @param cpuModel the string representing the model of a given CPU, e.g. "AMD A10 PRO-7800B"
     * @return an Optional containing the scalingFactor for this CPU model, relative to a baseline;
     * or Optional.empty() if the scalingFactor lookup fails.
     */
    Optional<Double> getScalingFactor(@Nonnull String cpuModel);
}
