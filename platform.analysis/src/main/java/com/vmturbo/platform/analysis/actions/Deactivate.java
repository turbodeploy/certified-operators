package com.vmturbo.platform.analysis.actions;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * An action to deactivate an active {@link Trader trader}.
 */
public class Deactivate extends StateChangeBase implements Action { // inheritance for code reuse

    // Constructors

    /**
     * Constructs a new Deactivate action with the specified target.
     *
     * @param target The trader that will be deactivated as a result of taking {@code this} action.
     * @param sourceMarket The market that benefits from deactivating target.
     */
    public Deactivate(@NonNull Trader target, @NonNull Market sourceMarket) {
        super(target,sourceMarket);
    }

    // Methods

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
            // TODO (Vaptistis): do I also need to send the market or basket?
            .append("<action type=\"deactivate\" target=\"").append(oid.apply(getTarget()))
            .append("\" />").toString();
    }

    @Override
    public @NonNull Deactivate take() {
        getTarget().changeState(TraderState.INACTIVE);
        return this;
    }

    @Override
    public @NonNull Deactivate rollback() {
        getTarget().changeState(TraderState.ACTIVE);
        return this;
    }

    // TODO: update description and reason when we create the corresponding matrix.
    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Deactivate ");
        appendTrader(sb, getTarget(), uuid, name);
        sb.append(".");

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder()
            .append("Because of insufficient demand for ").append(getSourceMarket().getBasket())
            .append(".").toString(); // TODO: print basket in human-readable form.
    }
} // end Deactivate class
