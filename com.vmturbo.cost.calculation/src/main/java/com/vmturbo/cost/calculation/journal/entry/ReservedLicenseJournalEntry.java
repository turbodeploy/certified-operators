package com.vmturbo.cost.calculation.journal.entry;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostJournal.RateExtractor;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for reserved instance license rates.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
@Immutable
public class ReservedLicenseJournalEntry<E> implements QualifiedJournalEntry<E>, Comparable {
    private final Price price;
    private final Optional<CostSource> costSource;
    private final CostCategory targetCostCategory;
    private final ReservedInstanceData riData;
    private final TraxNumber riBoughtPercentage;

    /**
     * Constructor.
     *
     * @param price the reserved instance license price
     * @param riData the reserved instance data
     * @param riBoughtPercentage the percentage of the RI applied
     * @param costCategory the cost category to record the journal entry
     * @param costSource the cost source for the journal entry
     */
    public ReservedLicenseJournalEntry(final Price price, final ReservedInstanceData riData,
            final TraxNumber riBoughtPercentage, final CostCategory costCategory,
            final Optional<CostSource> costSource) {
        this.price = price;
        this.targetCostCategory = costCategory;
        this.riData = riData;
        this.riBoughtPercentage = riBoughtPercentage;
        this.costSource = costSource;
    }

    @Override
    public TraxNumber calculateHourlyCost(
            @Nonnull final EntityInfoExtractor<E> infoExtractor,
            @Nonnull final DiscountApplicator<E> discountApplicator,
            @Nonnull final RateExtractor rateExtractor) {
        final long providerId =
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo().getTierId();
        final TraxNumber discountPercentage = discountApplicator.getDiscountPercentage(providerId);
        final TraxNumber discountedPricePercentage = Trax.trax(1.0, "100%")
                .minus(discountPercentage)
                .compute("discounted price portion");
        final TraxNumber unitPrice =
                Trax.trax(price.getPriceAmount().getAmount() * riBoughtPercentage.getValue(),
                        "Unit Full Price.");
        return unitPrice.times(discountedPricePercentage).compute("discounted unit price");
    }

    @Nonnull
    @Override
    public Optional<CostSource> getCostSource() {
        return costSource;
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

    @Override
    public int compareTo(final Object o) {
        if (o instanceof OnDemandJournalEntry) {
            return Integer.MAX_VALUE;
        } else {
            return 0;
        }
    }
}
