package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.dataflow.qual.Pure;

/**
 * The different states a given trader can be in.
 *
 * <p>
 *  The states should make sense for the market and not just for our application domain of data
 *  center optimization.
 * </p>
 *
 * <p>
 *  Clients of this class should use its public methods to test specific properties of the states
 *  instead of comparing against a specific trader state or switching over all states.
 * </p>
 */
public enum TraderState {
    // Enumerators
    ACTIVE,
    INACTIVE,
    // only VMs can be IDLE
    IDLE;

    // Methods

    /**
     * Tests whether {@code this} trader state is an active one.
     *
     * <p>
     *  An active {@link Trader} participates in a number of markets (depending on its commodities)
     *  as a buyer or seller. An inactive one does not. An inactive trader retains its attributes'
     *  values and participation in the economy and may resume its trading operations in the future.
     * </p>
     */
    @Pure
    public boolean isActive(@ReadOnly TraderState this) {
        return this == TraderState.ACTIVE;
    }

} // end TraderState enumeration
