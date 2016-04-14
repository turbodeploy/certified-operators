package com.vmturbo.platform.analysis.ede;

import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A collection of attributes capturing decisions of the {@link Ede Economic Decision Engine} for
 * a {@link Trader}
 */
public class StateItem {

    // Fields

    // the Trader associated with the StateItem
    private final Trader trader_;

    // times capturing decisions for the Trader
    private long cloneOnlyAfterThisTime_;
    private long suspendOnlyAfterThisTime_;
    private long moveFromOnlyAfterThisTime_;
    private long moveToOnlyAfterThisTime_;

    // Constructors

    /**
     * Constructs a State Item and associates it with a Trader
     *
     * @param correspondingTrader - the Trader to which the State Item corresponds
     */
    public StateItem(Trader correspondingTrader) {
        trader_ = correspondingTrader;
    }

    // Methods

    public Trader getTrader() {
        return trader_;
    }

    public long getCloneOnlyAfterThisTime() {
        return cloneOnlyAfterThisTime_;
    }

    public StateItem setCloneOnlyAfterThisTime(long time) {
        cloneOnlyAfterThisTime_ = time;
        return this;
    }

    public long getSuspendOnlyAfterThisTime() {
        return suspendOnlyAfterThisTime_;
    }

    public StateItem setSuspendOnlyAfterThisTime(long time) {
        suspendOnlyAfterThisTime_ = time;
        return this;
    }

    public long getMoveFromOnlyAfterThisTime() {
        return moveFromOnlyAfterThisTime_;
    }

    public StateItem setMoveFromOnlyAfterThisTime(long time) {
        moveFromOnlyAfterThisTime_ = time;
        return this;
    }

    public long getMoveToOnlyAfterThisTime() {
        return moveToOnlyAfterThisTime_;
    }

    public StateItem setMoveToOnlyAfterThisTime(long time) {
        moveToOnlyAfterThisTime_ = time;
        return this;
    }

}
