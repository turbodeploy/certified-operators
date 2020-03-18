package com.vmturbo.platform.analysis.actions;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * The implementation for Action interface. This class is the superclass for any kind of
 * {@link Action}. It keeps the common implementation for methods defined in {@link Action}.
 * It also keeps track of the state of an action instance whether it is being taken or not.
 */
public abstract class ActionImpl implements Action {
    // Fields
    private final @NonNull Economy economy_; // whether we can avoid this field is under investigation.

    // a flag to indicate if the action is taken or not
    private boolean actionTaken = false;

    // measure of importance of an action
    private double importance_ = 0;

    private boolean executable_ = true;

    // Extract some Action that isn't normally extracted from the provision analysis round of
    // main market.
    private boolean extractAction_ = false;

    private @NonNull List<Action> subsequentActions_ = new LinkedList<>();

    // Constructors

    /**
     * Constructs a new ActionImpl object. It's not intended to be used independently, but rather as
     * the base object of concrete actions.
     *
     * @param economy The economy of {@code this} action.
     */
    ActionImpl(@NonNull Economy economy) {
        economy_ = economy;
    }

    // Methods

    @Pure
    @Override
    public @NonNull Economy getEconomy(@ReadOnly ActionImpl this) {
        return economy_;
    }

    @Override
    public ActionType getType() {
        return ActionType.UNKNOWN;
    }

    @Override
    public boolean setExecutable(boolean executable) {
        return executable_ = executable;
    }

    @Override
    public boolean isExecutable() {
        return executable_;
    }

    /**
     * @return the importance of this action
     * @see ActionImpl#setImportance
     */
    public double getImportance() {
        return importance_;
    }

    /**
     * @param importance of an action
     * @return {@code this}
     * @see #getImportance()
     */
    public @NonNull Action setImportance(double importance) {
        if (importance < -Double.MAX_VALUE) {
            importance_ = -Double.MAX_VALUE;
        } else if (importance < Double.MAX_VALUE) {
            importance_ = importance;
        } else {
            importance_ = Double.MAX_VALUE;
        }
        return this;
    }

    /**
     * A getter for the flag regarding whether the action is taken or not.
     */
    public boolean isActionTaken() {
        return actionTaken;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull Trader getActionTarget() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Before taking any action, the state for the action should be checked.
     * If it is taken, throws an {@link IllegalStateException}.
     */
    @Override
    public @NonNull Action take() {
        if (actionTaken) {
            throw new IllegalStateException("The action should not be taken twice!");
        }
        actionTaken = true;
        return this;
    }

    /**
     * Before rolling back any action, the state for the action should be checked.
     * If it is not taken, throws an {@link IllegalStateException}.
     */
    @Override
    public @NonNull Action rollback() {
        if (!actionTaken) {
            throw new IllegalStateException("The action should not rollback without being taken!");
        }
        actionTaken = false;
        return this;
    }

    @Override
    public @NonNull String debugDescription(
                    @NonNull Function<@NonNull Trader, @NonNull String> uuid,
                    @NonNull Function<@NonNull Trader, @NonNull String> name,
                    @NonNull IntFunction<@NonNull String> commodityType,
                    @NonNull IntFunction<@NonNull String> traderType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                    @NonNull Function<@NonNull Trader, @NonNull String> name,
                    @NonNull IntFunction<@NonNull String> commodityType,
                    @NonNull IntFunction<@NonNull String> traderType) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * A getter for the flag regarding whether the action that isn't normally extracted from the
     * provision round of analysis of the main market should be extracted.
     *
     * @return Whether the item should be extracted.
     */
    @Override
    public boolean isExtractAction() {
        return extractAction_;
    }

    /**
     * Set the flag regarding whether the action that isn't normally extracted from the
     * provision round of analysis of the main market to true.
     */
    @Override
    public void enableExtractAction() {
        extractAction_ = true;
    }

    /**
     * Returns the actions that were triggered after taking {@code this} action
     * @return a list of actions followed by {@code this}
     */
    @Pure
    public @NonNull List<Action> getSubsequentActions() {
        return subsequentActions_;
    }
}
