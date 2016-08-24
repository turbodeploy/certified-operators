package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Trader;

/**
 * The implementation for Action interface. This class is the superclass for any kind of
 * {@link Action}. It keeps the common implementation for methods defined in {@link Action}.
 * It also keeps track of the state of an action instance whether it is being taken or not.
 */
public class ActionImpl implements Action {

    // a flag to indicate if the action is taken or not
    private boolean actionTaken = false;

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


}
