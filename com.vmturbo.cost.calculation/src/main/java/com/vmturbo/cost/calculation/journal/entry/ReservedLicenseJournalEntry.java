package com.vmturbo.cost.calculation.journal.entry;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.builder.EqualsBuilder;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostItem;
import com.vmturbo.cost.calculation.journal.CostItem.CostSourceLink;
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
public class ReservedLicenseJournalEntry<E> implements QualifiedJournalEntry<E> {
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
    public Collection<CostItem> calculateHourlyCost(
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
        final TraxNumber discountedLicensePrice =
                unitPrice.times(discountedPricePercentage).compute("discounted unit price");

        return ImmutableList.of(CostItem.builder()
                .costSourceLink(CostSourceLink.of(costSource))
                .cost(discountedLicensePrice)
                .build());
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
    public int hashCode() {
        return Objects.hash(targetCostCategory, costSource, riData, riBoughtPercentage, price);
    }

    @Override
    public boolean equals(final Object obj) {

        if (obj == null || !(obj instanceof ReservedLicenseJournalEntry)) {
            return false;
        } else if (obj == this) {
            return true;
        } else {
            final ReservedLicenseJournalEntry other = (ReservedLicenseJournalEntry)obj;
            return new EqualsBuilder()
                    .append(targetCostCategory, other.targetCostCategory)
                    .append(costSource, other.costSource)
                    .append(riData, other.riData)
                    .append(riBoughtPercentage, other.riBoughtPercentage)
                    .append(price, other.price)
                    .build();
        }
    }
}
