package com.vmturbo.cost.calculation.journalentry;

import static com.vmturbo.trax.Trax.trax;

import javax.annotation.Nonnull;

import java.util.Optional;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.CostJournal.RateExtractor;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for RI discounted rates.
 *
 * @param <ENTITY_CLASS_> See {@link QualifiedJournalEntry}
 */
public class RIDiscountJournalEntry<ENTITY_CLASS_> implements QualifiedJournalEntry<ENTITY_CLASS_> {
    /**
     * The data about the reserved instance.
     */
    private final ReservedInstanceData riData;

    /**
     * The number of coupons covered by this RI.
     */
    private final TraxNumber riBoughtPercentage;

    private TraxNumber onDemandRate = trax(0);

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
    public RIDiscountJournalEntry(@Nonnull final ReservedInstanceData riData,
                           final TraxNumber riBoughtPercentage,
                           @Nonnull final CostCategory targetCostCategory,
                           @Nonnull final CostSource costSource,
                           boolean isBuyRI) {
        this.riData = riData;
        this.riBoughtPercentage = riBoughtPercentage;
        this.targetCostCategory = targetCostCategory;
        this.costSource = costSource;
        this.isBuyRI = isBuyRI;
    }

    @Override
    public TraxNumber calculateHourlyCost(@Nonnull final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor, @Nonnull final DiscountApplicator<ENTITY_CLASS_> discountApplicator, @Nonnull final RateExtractor rateExtractor) {
        final long providerId =
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo().getTierId();
        final TraxNumber discountPercentage = discountApplicator.getDiscountPercentage(providerId);
        final TraxNumber fullPricePercentage = trax(1.0, "100%")
                .minus(discountPercentage)
                .compute("full price portion");
        onDemandRate = rateExtractor.lookupCostWithFilter(targetCostCategory, costSource1 -> costSource1.equals(CostSource.ON_DEMAND_RATE));
        onDemandRate = trax(riBoughtPercentage.dividedBy(1).compute().times(-1).getValue())
                .times(onDemandRate).compute();

        return onDemandRate.times(fullPricePercentage)
                .compute(String.format("RI discounted %s cost (BuyRI=%s)",
                        targetCostCategory.getDescriptorForType().getFullName(),
                        isBuyRI));
    }

    @Nonnull
    @Override
    public Optional<CostSource> getCostSource() {
        return Optional.ofNullable(costSource);
    }

    @Nonnull
    @Override
    public CostCategory getCostCategory() {
        return targetCostCategory;
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof OnDemandJournalEntry) {
            return Integer.MAX_VALUE;
        } else {
            return 0;
        }
    }
}
