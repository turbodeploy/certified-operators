package com.vmturbo.platform.analysis.actions;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Economy;

/**
 * Base Class for Reconfigure action.
 */
public abstract class ReconfigureBase extends ActionImpl {

    /**
     * Reconfigure.
     *
     * @param economy - Economy.
     */
    public ReconfigureBase(@NonNull Economy economy) {
        super(economy);
    }
}