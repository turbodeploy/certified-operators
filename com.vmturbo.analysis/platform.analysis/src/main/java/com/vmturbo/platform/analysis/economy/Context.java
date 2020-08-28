package com.vmturbo.platform.analysis.economy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;

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

    /**
     * Constructor for the Context.
     *
     * @param regionId The regionId associated with the context
     * @param zoneId The zoneId associated with the context
     * @param balanceAccount The balance account associated with the context
     * @param familyBasedCoverageList is a list of coverages of consumers belonging to a scalingGroup
     */
    public Context(long regionId, long zoneId, BalanceAccount balanceAccount,
                   final List<EconomyDTOs.CoverageEntry> familyBasedCoverageList) {
        this(regionId, zoneId, balanceAccount);
        for (EconomyDTOs.CoverageEntry ce : familyBasedCoverageList) {
            CoverageEntry coverageEntry = new CoverageEntry(ce.getTotalAllocatedCoupons(),
                ce.getTotalRequestedCoupons());
            coverageEntryMap_.put(ce.getProviderId(), coverageEntry);
        }
    }

    /**
     * Constructor for the Context.
     *
     * @param other is the Context to clone
     */
    public Context(Context other) {
        this(other.getRegionId(), other.getZoneId(), other.getBalanceAccount());
        for (Map.Entry<Long, CoverageEntry> ce : other.getCoverageEntryMap().entrySet()) {
            CoverageEntry coverageEntry = new CoverageEntry(ce.getValue().getTotalAllocatedCoupons(),
                    ce.getValue().getTotalRequestedCoupons());
            coverageEntryMap_.put(ce.getKey(), coverageEntry);
        }
    }

    /**
     * Constructor for the Context. The coverageMap is constructed for this consumer.
     *
     * @param providerId The provider that the consumer is placed on
     * @param regionId The regionId associated with the context
     * @param zoneId The zoneId associated with the context
     * @param balanceAccount The balance account associated with the context
     * @param totalAllocatedCoupons is the total number of allocated coupons for the consumer
     * @param totalRequestedCoupons is the total number of requested coupons for the consumer
     */
    public Context(final Long providerId, final long regionId, final long zoneId,
                   final BalanceAccount balanceAccount, final double totalAllocatedCoupons,
                   final double totalRequestedCoupons) {
        this(regionId, zoneId, balanceAccount);
        coverageEntryMap_.put(providerId,
            new CoverageEntry(totalAllocatedCoupons, totalRequestedCoupons));
    }

    public long getRegionId() {
        return regionId_;
    }

    public long getZoneId() {
        return zoneId_;
    }

    public Optional<Double> getTotalRequestedCoupons(Long providerId) {
        CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
        return coverageEntry != null
            ? Optional.of(coverageEntry.getTotalRequestedCoupons())
            : Optional.empty();
    }

    public Optional<Double> getTotalAllocatedCoupons(final Long providerId) {
        final CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
        return coverageEntry != null
            ? Optional.of(coverageEntry.getTotalAllocatedCoupons())
            : Optional.empty();
    }

    public Context setTotalAllocatedCoupons(final Long providerId, double numCoupons) {
        if (providerId != null) {
            CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
            if (coverageEntry == null) {
                coverageEntry = createEntryAndRegisterInContext(providerId);
            }
            coverageEntry.setTotalAllocatedCoupons(numCoupons);
        }
        return this;
    }

    public Context setTotalRequestedCoupons(final Long providerId, double numCoupons) {
        if (providerId != null) {
            CoverageEntry coverageEntry = coverageEntryMap_.get(providerId);
            if (coverageEntry == null) {
                coverageEntry = createEntryAndRegisterInContext(providerId);
            }
            coverageEntry.setTotalRequestedCoupons(numCoupons);
        }
        return this;
    }

    public CoverageEntry createEntryAndRegisterInContext(final Long providerId) {
        CoverageEntry coverageEntry = new CoverageEntry(0, 0);
        coverageEntryMap_.put(providerId, coverageEntry);
        return coverageEntry;
    }

    public BalanceAccount getBalanceAccount() {
        return balanceAccount_;
    }

    Map<Long, Context.CoverageEntry> getCoverageEntryMap() {
        return this.coverageEntryMap_;
    }

    public boolean isEqualCoverages(Optional<EconomyDTOs.Context> other) {
        if (!other.isPresent()) {
            return false;
        }
        EconomyDTOs.Context otherContext = other.get();
        for (EconomyDTOs.CoverageEntry otherCoverage : otherContext.getFamilyBasedCoverageList()) {
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

        private Long parentId_;

        /**
         * Constructor for the Balance Account.
         *
         * @param spent the spent
         * @param budget the budget
         * @param id the id of the business account
         * @param priceId the price id associated with the business account
         * @param parentId trader's parent account ID (e.g. billing family)
         */
        public BalanceAccount(double spent, double budget, long id, long priceId,
                @Nullable Long parentId) {
            spent_ = spent;
            budget_ = budget;
            id_ = id;
            priceId_ = priceId;
            parentId_ = parentId;
        }

        /**
         * Constructor for the Balance Account.
         *
         * @param spent the spent
         * @param budget the budget
         * @param id the id of the business account
         * @param priceId the price id associated with the business account
         */
        public BalanceAccount(double spent, double budget, long id, long priceId) {
            this(spent, budget, id, priceId, null);
        }

        /**
         * Constructor for the {@link BalanceAccount} with a given account id.
         * When priceId is not specified, defaults it to be the same as account id, as there is
         * one possible. Otherwise, use the other constructor.
         *
         * @param id the business account id.
         */
        public BalanceAccount(long id) {
            this(0, 0, id, id, null);
        }

        /**
         * Creates a new BalanceAccount with account id and the parent id.
         *
         * @param id Account id.
         * @param parentId Parent (e.g BillingFamily) id.
         */
        public BalanceAccount(long id, long parentId) {
            this(0, 0, id, id, parentId);
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

        /**
         * Get trader's parent account ID (e.g. billing family).
         *
         * @return ID of trader's parent account.
         */
        @Nullable
        public Long getParentId() {
            return parentId_;
        }

        /**
         * Sets parent id, e.g BillingFamily id in case of AWS.
         *
         * @param parentId ID of parent.
         */
        public void setParentId(Long parentId) {
            parentId_ = parentId;
        }

        /**
         * Checks if a non-null parent id had been previously set.
         *
         * @return Whether parent id was set.
         */
        public boolean hasParentId() {
            return parentId_ != null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(spent_, budget_, id_, priceId_, parentId_);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof BalanceAccount)) {
                return false;
            }
            // the balance account is considered to be equal if all the contents
            // are the same.
            BalanceAccount otherBalanceAccount = (BalanceAccount)other;
            return this.getSpent() == otherBalanceAccount.getSpent()
                    && this.getBudget() == otherBalanceAccount.getBudget()
                    && this.getId() == otherBalanceAccount.getId()
                    && this.getPriceId() == otherBalanceAccount.getPriceId()
                    && (this.getParentId() != null ? this.getParentId().equals(otherBalanceAccount.getParentId())
                    : otherBalanceAccount.getParentId() == null);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(regionId_) + balanceAccount_.hashCode();
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

        return this.getBalanceAccount().equals(otherContext.getBalanceAccount())
                && this.getRegionId() == otherContext.getRegionId();
    }

    @Override
    public String toString() {
        return String.format("[region id: %s, zone id: %s, price id: %s, account id: %s]",
                regionId_, zoneId_, balanceAccount_.priceId_, balanceAccount_.id_);
    }

    /**
     * Static class representing a coverage. The coverage for a consumer captures the totalRequestedCoupons and the
     * totalAllocatedCoupons for a consumer from a provider.
     *
     */
    public static class CoverageEntry {
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
