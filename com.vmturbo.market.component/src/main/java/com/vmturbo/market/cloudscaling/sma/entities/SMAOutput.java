package com.vmturbo.market.cloudscaling.sma.entities;

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

    @Nonnull
    public List<SMAOutputContext> getContexts() {
        return outputContexts;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SMAOutput outputContext size=").append(outputContexts.size());
        for (int i = 0; i < outputContexts.size(); i++) {
            buffer.append("\n  ").append(outputContexts.get(i));
        }
        return buffer.toString();
    }
}
