package com.vmturbo.platform.analysis.economy;

import java.util.Objects;

/**
 * A class representing the context which includes the balance account and region.
 */
public class Context {

    private long regionId;
    private BalanceAccount balanceAccount_;

    /**
     * Constructor for the Context.
     *
     * @param regionIdentity The regionId associated with the context
     * @param balanceAccount The balance account associated with the context
     */
    public Context(long regionIdentity, BalanceAccount balanceAccount) {
        regionId = regionIdentity;
        balanceAccount_ = balanceAccount;
    }

    public long getRegionId() {
        return regionId;
    }

    public BalanceAccount getBalanceAccount() {
        return balanceAccount_;
    }

    /**
     * Static class representing a balance account.
     */
    public static class BalanceAccount {

        private double spent_;
        private double budget_;
        private long id_;

        /**
         * Id corresponding to the price offering that this Balance Account is associated with. The
         * provider costs may be dependent on the priceId of the Balance Account.
         */
        private long priceId_;

        /**
         * Constructor for the Balance Account.
         *
         * @param spent the spent
         * @param budget the budget
         * @param id the id of the business account
         * @param priceId the price id associated with the business account
         */
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

    @Override
    public int hashCode() {
        return Objects.hash(regionId, balanceAccount_);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof Context)) {
            return false;
        }

        Context otherContext = (Context)other;

        return this.getBalanceAccount() == otherContext.getBalanceAccount() && this.getRegionId() == otherContext.getRegionId();
    }
}
