package com.vmturbo.platform.analysis.economy;

import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A class representing the context which includes the balance account and region.
 */
public class Context {

    private long regionId_;
    private long zoneId_;
    private BalanceAccount balanceAccount_;
    private long totalRequestedCoupons_;
    private long totalAllocatedCoupons_;

    /**
     * Constructor for the Context.
     *
     * @param regionId The regionId associated with the context
     * @param zoneId The zoneId associated with the context
     * @param balanceAccount The balance account associated with the context
     * @param totalRequestedCoupons
     * @param totalAllocatedCoupons
     */
    public Context(long regionId, long zoneId, BalanceAccount balanceAccount, long totalRequestedCoupons, long totalAllocatedCoupons) {
        regionId_ = regionId;
        zoneId_ = zoneId;
        balanceAccount_ = balanceAccount;
        totalRequestedCoupons_ = totalRequestedCoupons;
        totalAllocatedCoupons_ = totalAllocatedCoupons;
    }

    /**
     * Constructor for the Context.
     *
     * @param regionId The regionId associated with the context
     * @param zoneId The zoneId associated with the context
     * @param balanceAccount The balance account associated with the context
     */
    public Context(long regionId, long zoneId, BalanceAccount balanceAccount) {
        regionId_ = regionId;
        zoneId_ = zoneId;
        balanceAccount_ = balanceAccount;
        totalRequestedCoupons_ = 0;
        totalAllocatedCoupons_ = 0;
    }

    public long getRegionId() {
        return regionId_;
    }

    public long getZoneId() {
        return zoneId_;
    }

    public long getTotalRequestedCoupons() {
        return totalRequestedCoupons_;
    }

    public @Nonnull Context setTotalRequestedCoupons(long totalRequestedCoupons) {
        totalRequestedCoupons_ = totalRequestedCoupons;
        return this;
    }

    public long getTotalAllocatedCoupons() {
        return totalAllocatedCoupons_;
    }

    public @Nonnull Context setTotalAllocatedCoupons(long totalAllocatedCoupons) {
        totalAllocatedCoupons_ = totalAllocatedCoupons;
        return this;
    }

    public BalanceAccount getBalanceAccount() {
        return balanceAccount_;
    }

    public boolean equals(EconomyDTOs.Context other) {
        return this.getTotalRequestedCoupons() == other.getTotalRequestedCoupons() &&
                this.getTotalAllocatedCoupons() == other.getTotalAllocatedCoupons();
    }

    public boolean hasValidContext() {
        return this.getTotalRequestedCoupons() != 0;
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
        return Objects.hash(regionId_, balanceAccount_);
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

    @Override
    public String toString() {
        return String.format("[region id: %s, zone id: %s]", regionId_, zoneId_);
    }
}
