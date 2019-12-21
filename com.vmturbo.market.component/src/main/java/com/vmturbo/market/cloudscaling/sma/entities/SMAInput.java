package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * The input to the Stable marriage algorithm.
 */
public class SMAInput {

    /**
     * List of input contexts.
     */
    public final List<SMAInputContext> inputContexts;

    /**
     * Constructor for SMAInput.
     * @param contexts the input to SMA partioned into contexts.
     *                 Each element in list corresponds to a context.
     */
    public SMAInput(@Nonnull final List<SMAInputContext> contexts) {
        this.inputContexts = Objects.requireNonNull(contexts, "contexts is null!");
    }

    @Nonnull
    public List<SMAInputContext> getContexts() {
        return inputContexts;
    }

    @Override
    public String toString() {
        return "SMAInput{" +
                "inputContexts=" + inputContexts.size() +
                '}';
    }
}
