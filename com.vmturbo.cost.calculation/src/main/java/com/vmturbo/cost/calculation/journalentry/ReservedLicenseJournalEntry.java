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
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for reserved instance license rates.
 *
 * @param <ENTITY_CLASS_> See {@link QualifiedJournalEntry}
 */
@Immutable
public class ReservedLicenseJournalEntry<ENTITY_CLASS_> implements QualifiedJournalEntry<ENTITY_CLASS_>, Comparable {
    private final Price price;
    private Optional<CostSource> costSource;
    private final CostCategory targetCostCategory;
    private ReservedInstanceData riData;
    private TraxNumber riBoughtPercentage;

    /**
     * Constructor for the Reserved Instance Journal Entry.
     *
     * @param price The Reserved Instance license price..
     * @param riData The reserved instance data.
     * @param riBoughtPercentage The percentage of the RI applied.
     * @param costCategory The cost category to record the journal entry for.
     * @param costSource Then cost souce for the Journal entry.
     */
    public ReservedLicenseJournalEntry(Price price, ReservedInstanceData riData, TraxNumber riBoughtPercentage,
                                CostCategory costCategory, Optional<CostSource> costSource) {
        this.price = price;
        this.targetCostCategory = costCategory;
        this.riData = riData;
        this.riBoughtPercentage = riBoughtPercentage;
        this.costSource = costSource;
    }

    @Override
    public TraxNumber calculateHourlyCost(@Nonnull final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor, @Nonnull final DiscountApplicator<ENTITY_CLASS_> discountApplicator, @Nonnull final RateExtractor rateExtractor) {
        final long providerId =
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo().getTierId();
        final TraxNumber discountPercentage = discountApplicator.getDiscountPercentage(providerId);
        final TraxNumber discountedPricePercentage = trax(1.0, "100%")
                .minus(discountPercentage)
                .compute("discounted price portion");
        final TraxNumber unitPrice = trax(price.getPriceAmount().getAmount() * riBoughtPercentage.getValue(), "Unit Full Price.");
        final TraxNumber discountedUnitPrice = unitPrice.times(discountedPricePercentage).compute("discounted unit price");
        return discountedUnitPrice;
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

    @Override
    public int compareTo(final Object o) {
        if (o instanceof OnDemandJournalEntry) {
            return Integer.MAX_VALUE;
        } else {
            return 0;
        }
    }
}
