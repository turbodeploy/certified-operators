package com.vmturbo.market.cloudvmscaling.entities;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * The output to the Stable marriage algorithm.
 */
public class SMAOutput {

    /**
     * List of SMA output contexts.
     */
    public final List<SMAOutputContext> outputContexts;

    /**
     * Constructor for SMAOutput.
     *
     * @param contexts list of SMAOutputContext partitioned based on context.
     */
    public SMAOutput(@Nonnull final List<SMAOutputContext> contexts) {
        this.outputContexts = Objects.requireNonNull(contexts, "contexts is null!");
    }

    /**
     * getter method for  outputContexts.
     *
     * @return outputContexts
     */
    @Nonnull
    public List<SMAOutputContext> getContexts() {
        return outputContexts;
    }

    /**
     * converts the SMAOutput to string.
     *
     * @return SMAOutput converted to string.
     */
    @Override
    public String toString() {
        return "SMAOutput{" +
                "outputContexts=" + outputContexts.size() +
                '}';
    }
}
