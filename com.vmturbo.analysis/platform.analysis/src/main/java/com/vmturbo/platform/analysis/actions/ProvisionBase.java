package com.vmturbo.platform.analysis.actions;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * Base class of both types of provision actions by market
 * with common methods and fields
 * @author nitya
 *
 */
public abstract class ProvisionBase extends ActionImpl {

    // Trader which is cloned to provision new trader
    private final @NonNull Trader modelSeller_;
    // New Trader created after provision action is taken
    private @Nullable Trader provisionedSeller_;
    // id of action
    private long oid_;

    /**
     * Constructor to populate basic fields of provision action
     * @param economy
     * @param modelSeller
     */
    public ProvisionBase(@NonNull Economy economy, @NonNull Trader modelSeller) {
        super(economy);
        modelSeller_ = modelSeller;
    }

    /**
     * Set a trader that is result of taking a provision action
     *
     * @param provisionedSeller
     */
    public void setProvisionedSeller(Trader provisionedSeller) {
        this.provisionedSeller_ = provisionedSeller;
    }

    /**
     * Returns the seller that was added as a result of taking {@code this} action.
     *
     * <p>
     *  It will be {@code null} before the action is taken and/or after it is rolled back.
     * </p>
     */
    @Pure
    public @Nullable Trader getProvisionedSeller(@ReadOnly ProvisionBase this) {
        return provisionedSeller_;
    }

    /**
     * Returns the seller that is used as a model in taking {@code this} action.
     */
    @Pure
    public @NonNull Trader getModelSeller(@ReadOnly ProvisionBase this) {
        return modelSeller_;
    }

}
