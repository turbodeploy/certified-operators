package com.vmturbo.platform.analysis.economy;

import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CoverageEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CoverageEntry.Builder;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

/**
 * A class representing the context which includes the balance account and region.
 */
public class Context {
    private long regionId_;
    private long zoneId_;
    private BalanceAccount balanceAccount_;
    private Map<Long, CoverageEntry> coverageEntryMap_;

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
        coverageEntryMap_ = new HashMap<>();
    }

    public Context(long providerId, long regionId, long zoneId, BalanceAccount balanceAccount,
                   final List<EconomyDTOs.CoverageEntry> familyBasedCoverageList) {
        this(regionId, zoneId, balanceAccount);  // XLTODO this is broken for multiple entries
        for (EconomyDTOs.CoverageEntry ce : familyBasedCoverageList) {
            CoverageEntry coverageEntry = new CoverageEntry(ce.getTotalAllocatedCoupons(),
                ce.getTotalRequestedCoupons());
            coverageEntryMap_.put(providerId, coverageEntry);
        }
    }

    public Context(final long providerId, final long regionId, final long zoneId,
                   final BalanceAccount balanceAccount, final double totalAllocatedCoupons,
                   final double totalRequestedCoupons) {
        this(regionId, zoneId, balanceAccount);
        coverageEntryMap_.put(providerId,
            new CoverageEntry(totalAllocatedCoupons, totalRequestedCoupons));
    }


    //    /**
//     * This version is used for testing only
//     * @param regionId
//     * @param zoneId
//     * @param balanceAccount
//     * @param totalAllocatedCoupons initial total allocated coupons
//     * @param totalRequestedCoupons initial total requested coupons
//     */
//    public Context(long providerId, long regionId, long zoneId, BalanceAccount balanceAccount,
//                   double totalAllocatedCoupons, double totalRequestedCoupons) {
//        this(regionId, zoneId, balanceAccount, new ArrayList<>());
//        List<CoverageEntry> coverageEntries = ;
//        coverageEntries.add(new CoverageEntry(totalAllocatedCoupons, totalRequestedCoupons));
//    }
//
    public long getRegionId() {
        return regionId_;
    }

    public long getZoneId() {
        return zoneId_;
    }

    public Optional<Double> getTotalRequestedCoupons(long providerId) {
        CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
        return coverageEntry != null
            ? Optional.of(coverageEntry.getTotalRequestedCoupons())
            : Optional.empty();
    }

    public Optional<Double> getTotalAllocatedCoupons(final long providerId) {
        final CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
        return coverageEntry != null
            ? Optional.of(coverageEntry.getTotalAllocatedCoupons())
            : Optional.empty();
    }

    public Context setTotalAllocatedCoupons(final long providerId, double numCoupons) {
        CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
        if (coverageEntry != null) {
            coverageEntry.setTotalAllocatedCoupons(numCoupons);
        }
        return this;
    }

    public Context setTotalRequestedCoupons(final long providerId, double numCoupons) {
        CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
        if (coverageEntry != null) {
            coverageEntry.setTotalRequestedCoupons(numCoupons);
        }
        return this;
    }

    public BalanceAccount getBalanceAccount() {
        return balanceAccount_;
    }

    Map<Long, Context.CoverageEntry> getCoverageEntryMap() {
        return this.coverageEntryMap_;
    }

    public boolean equals(EconomyDTOs.Context other) {
        for (EconomyDTOs.CoverageEntry otherCoverage : other.getFamilyBasedCoverageList()) {
            CoverageEntry thisCoverage = getCoverageEntryMap().get(otherCoverage.getProviderId());
            if (thisCoverage == null) {
                return false;
            }
            if (thisCoverage.getTotalRequestedCoupons() != otherCoverage.getTotalRequestedCoupons() ||
                thisCoverage.getTotalAllocatedCoupons() != otherCoverage.getTotalAllocatedCoupons()) {
                return false;
            }
        }
        return true;
    }

    public boolean hasValidContext() {
        return this.getCoverageEntryMap().values().stream()
            .anyMatch(ce -> ce.getTotalRequestedCoupons() != 0);
    }

    public void setCoverageEntryMap(final Map<Long, CoverageEntry> coverageMap) {
        this.coverageEntryMap_ = coverageMap;
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

    static class CoverageEntry {
        private double totalRequestedCoupons_;
        private double totalAllocatedCoupons_;

        public double getTotalRequestedCoupons() {
            return totalRequestedCoupons_;
        }

        public double getTotalAllocatedCoupons() {
            return totalAllocatedCoupons_;
        }

        public CoverageEntry(double totalAllocatedCoupons, double totalRequestedCoupons) {
            totalAllocatedCoupons_ = totalAllocatedCoupons;
            totalRequestedCoupons_ = totalRequestedCoupons;
        }

        public boolean equals(EconomyDTOs.CoverageEntry other) {
            return this.getTotalAllocatedCoupons() == other.getTotalAllocatedCoupons() &&
                   this.getTotalRequestedCoupons() == other.getTotalRequestedCoupons();
        }

        public CoverageEntry setTotalAllocatedCoupons(double amount) {
            totalAllocatedCoupons_ = amount;
            return this;
        }

        public CoverageEntry setTotalRequestedCoupons(double amount) {
            totalRequestedCoupons_ = amount;
            return this;
        }

        public CoverageEntry addTotalAllocatedCoupons(double amount) {
            totalAllocatedCoupons_ += amount;
            return this;
        }

        public CoverageEntry addTotalRequestedCoupons(double amount) {
            totalRequestedCoupons_ += amount;
            return this;
        }
    }
}
