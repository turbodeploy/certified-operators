package com.vmturbo.platform.analysis.economy;

/**
 * A class holding spend, budget and the list of traders which share the budget.
 * @author weiduan
 *
 */
public class BalanceAccount {

    private double spent_;
    private double budget_;
    private long id_;

    /**
     * Id corresponding to the price offering that this Balance Account is associated with. The
     * provider costs may be dependent on the priceId of the Balance Account.
     */
    private long priceId_;

    public BalanceAccount(double spent, double budget, long id, long priceId) {
        spent_ = spent;
        budget_ = budget;
        id_ = id;
        priceId_ = priceId;
    }

    public void setSpent(double spent) {
        spent_ = spent;
    }

    public void setBudget(double budget) {
        budget_ = budget;
    }

    public double getSpent() {
        return spent_;
    }

    public double getBudget() {
        return budget_;
    }

    public long getId() {
        return id_;
    }

    public long getPriceId() {
        return priceId_;
    }
}
