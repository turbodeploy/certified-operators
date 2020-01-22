package com.vmturbo.cost.calculation.journalentry;

import static com.vmturbo.trax.Trax.trax;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.CostJournal.RateExtractor;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for payments covered by reserved instances. One entity may have
 * entries for several reserved instances, if multiple RI's are partially covering the entity.
 *
 * @param <ENTITY_CLASS_> See {@link QualifiedJournalEntry}.
 */
@Immutable
public class RIJournalEntry<ENTITY_CLASS_> implements QualifiedJournalEntry<ENTITY_CLASS_> {

    /**
     * The data about the reserved instance.
     */
    private final ReservedInstanceData riData;

    /**
     * The number of coupons covered by this RI.
     */
    private final TraxNumber couponsCovered;

    /**
     * The cost of the reserved instance for this entity.
     */
    private final TraxNumber hourlyCost;

    private final CostSource costSource;

    private final CostCategory costCategory;

    public RIJournalEntry(@Nonnull final ReservedInstanceData riData,
                   final TraxNumber couponsCovered,
                   @Nonnull final TraxNumber hourlyCost,
                   @Nonnull CostCategory costCategory,
                   @Nonnull CostSource costSource) {
        this.riData = riData;
        this.couponsCovered = couponsCovered;
        this.hourlyCost = hourlyCost;
        this.costCategory = costCategory;
        this.costSource = costSource;
    }

    @Override
    public TraxNumber calculateHourlyCost(@Nonnull final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor,
                                          @Nonnull final DiscountApplicator<ENTITY_CLASS_> discountApplicator, @Nonnull final RateExtractor rateExtractor) {
        // We still want to apply discounts to RI prices.
        // When looking up the discount for an RI we use the tier that the RI is for.
        //
        // It may be possible that the RI was bought by a different account than
        // the one that owns the VM. If that account has a different discount, it's not
        // clear which discount we should use. However, for consistency we choose to use
        // the same discount that we use for the entity. Realistically this shouldn't be
        // a problem, because the RI purchase and the entity the RI is applying to should
        // be under the same master account.
        final long providerId =
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo().getTierId();
        final TraxNumber discountPercentage = discountApplicator.getDiscountPercentage(providerId);
        final TraxNumber fullPricePercentage = trax(1.0, "100%")
                .minus(discountPercentage)
                .compute("full price portion");
        return hourlyCost.times(fullPricePercentage).compute("hourly discounted RI");
    }

    @Nonnull
    @Override
    public Optional<CostSource> getCostSource() {
        return Optional.ofNullable(costSource);
    }

    @Nonnull
    @Override
    public CostCategory getCostCategory() {
        return costCategory;
    }

    @Override
    public int compareTo(final Object o) {
        // The RI journal entry has a dependence on the on Demand journal entry and requires it
        // to calculate RI discounted costs.
        if (o instanceof OnDemandJournalEntry) {
            return Integer.MAX_VALUE;
        } else {
            return 0;
        }
    }

    public ReservedInstanceData getRiData() {
        return riData;
    }

    public TraxNumber getCouponsCovered() {
        return couponsCovered;
    }

    public TraxNumber getHourlyCost() {
        return hourlyCost;
    }
}
