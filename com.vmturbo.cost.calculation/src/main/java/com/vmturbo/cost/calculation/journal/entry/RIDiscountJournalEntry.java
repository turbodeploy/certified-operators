package com.vmturbo.cost.calculation.journal.entry;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostJournal.RateExtractor;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for RI discounted rates.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class RIDiscountJournalEntry<E> implements QualifiedJournalEntry<E> {
    /**
     * The data about the reserved instance.
     */
    private final ReservedInstanceData riData;

    /**
     * The number of coupons covered by this RI.
     */
    private final TraxNumber riBoughtPercentage;

    private final CostCategory targetCostCategory;

    private final CostSource costSource;

    private final boolean isBuyRI;

    /**
     * Constructor for the RI discount journal entry.
     *
     * @param riData The reserved instance object specific data.
     * @param riBoughtPercentage The ri bought percentage.
     * @param targetCostCategory The target cost category.
     * @param costSource The cost source
     * @param isBuyRI true if the the discount is coming from a buy RI action.
     */
    public RIDiscountJournalEntry(
            @Nonnull final ReservedInstanceData riData,
            @Nonnull final TraxNumber riBoughtPercentage,
            @Nonnull final CostCategory targetCostCategory,
            @Nonnull final CostSource costSource,
            final boolean isBuyRI) {
        this.riData = riData;
        this.riBoughtPercentage = riBoughtPercentage;
        this.targetCostCategory = targetCostCategory;
        this.costSource = costSource;
        this.isBuyRI = isBuyRI;
    }

    @Override
    public TraxNumber calculateHourlyCost(
            @Nonnull final EntityInfoExtractor<E> infoExtractor,
            @Nonnull final DiscountApplicator<E> discountApplicator,
            @Nonnull final RateExtractor rateExtractor) {
        // OnDemand rate is already price-adjusted (discounted), so don't discount again.
        TraxNumber onDemandRate = rateExtractor.lookupCostWithFilter(targetCostCategory,
                cs -> cs == CostSource.ON_DEMAND_RATE);
        return Trax.trax(riBoughtPercentage.dividedBy(1).compute().times(-1).getValue())
                .times(onDemandRate)
                .compute(String.format("RI discounted %s cost (BuyRI=%s)",
                        targetCostCategory.getDescriptorForType().getFullName(), isBuyRI));
    }

    @Nonnull
    @Override
    public Optional<CostSource> getCostSource() {
        return Optional.of(costSource);
    }

    @Nonnull
    @Override
    public CostCategory getCostCategory() {
        return targetCostCategory;
    }

    @Nonnull
    public ReservedInstanceData getRiData() {
        return riData;
    }

    @Nonnull
    public TraxNumber getRiBoughtPercentage() {
        return riBoughtPercentage;
    }

    public boolean isBuyRI() {
        return isBuyRI;
    }

    @Override
    public int compareTo(final Object o) {
        // Make sure RIDiscountJournalEntries always come after OnDemandJournalEntries in order
        // to properly calculate the RI discount based on the on-demand cost recorded in the cost
        // journal
        if (o instanceof OnDemandJournalEntry) {
            return Integer.MAX_VALUE;
        } else {
            return QualifiedJournalEntry.super.compareTo(o);
        }
    }
}
