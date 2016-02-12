package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.topology.OID;

/**
 * The root of the action hierarchy.
 */
// TODO (Vaptistis): Do we need actions to be applicable to other economies than those used to
// generate them?
public interface Action {
    // Methods

    /**
     * Returns a String representation of {@code this} action suitable for transmission to the
     * "all-the-rest" server.
     *
     * @param oid A function that maps {@link Trader traders} to {@link OID OIDs}.
     * @return A String representation of {@code this} action where all object references have been
     *         substituted with {@link OID OIDs}.
     */
    // TODO: hoist most work for serializing the action here. Subclasses may not even need to know
    // if the format is JSON or XML e.g..
    @Pure
    @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid);

    /**
     * Takes {@code this} action on a specified {@link Economy}.
     *
     * <p>
     *  An action can be taken only once, unless it is rolled back in which case it can be taken
     *  again.
     * </p>
     */
    void take();

    /**
     * Rolls back {@code this} action on a specified {@link Economy}.
     *
     * <p>
     *  An action can only be rolled back after it is taken and only once for each time it is taken.
     * </p>
     */
    void rollback();

    /**
     * Returns a human-readable description of {@code this} action for debugging purposes.
     *
     * <p>
     *  The returned message may or may not be similar to what the user will see.
     * <p>
     *
     * @param uuid A function from trader to its UUID.
     * @param name A function from trader to its human-readable name.
     * @param commodityType A function from numeric to human-readable commodity type.
     * @param traderType A function from numeric to human-readable trader type.
     * @return A human-readable description of {@code this} action.
     */
    @Pure
    @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                     @NonNull Function<@NonNull Trader, @NonNull String> name,
                                     @NonNull IntFunction<@NonNull String> commodityType,
                                     @NonNull IntFunction<@NonNull String> traderType);
    /**
     * Returns a human-readable reason for {@code this} action for debugging purposes.
     *
     * <p>
     *  The returned message may or may not be similar to what the user will see.
     * <p>
     *
     * @param uuid A function from trader to its UUID.
     * @param name A function from trader to its human-readable name.
     * @param commodityType A function from numeric to human-readable commodity type.
     * @param traderType A function from numeric to human-readable trader type.
     * @return A human-readable reason for {@code this} action.
     */
    @Pure
    @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                @NonNull Function<@NonNull Trader, @NonNull String> name,
                                @NonNull IntFunction<@NonNull String> commodityType,
                                @NonNull IntFunction<@NonNull String> traderType);

} // end Action interface
