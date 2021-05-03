package com.vmturbo.cost.calculation.journal.entry;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.builder.EqualsBuilder;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostItem;
import com.vmturbo.cost.calculation.journal.CostItem.CostSourceLink;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.journal.CostJournal.RateExtractor;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for RI discounted rates.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class RIDiscountJournalEntry<E> implements QualifiedJournalEntry<E> {

    private static final CostSourceFilter EXCLUDE_RI_DISCOUNTS_FILTER = (costSource) ->
            costSource != CostSource.RI_INVENTORY_DISCOUNT
                    && costSource != CostSource.BUY_RI_DISCOUNT;

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
    public Collection<CostItem> calculateHourlyCost(
            @Nonnull final EntityInfoExtractor<E> infoExtractor,
            @Nonnull final DiscountApplicator<E> discountApplicator,
            @Nonnull final RateExtractor rateExtractor) {
        // OnDemand rate is already price-adjusted (discounted), so don't discount again.
        final Collection<CostItem> costItems = rateExtractor.lookupCostWithFilter(targetCostCategory, EXCLUDE_RI_DISCOUNTS_FILTER);
        final TraxNumber riDiscount =  Trax.trax(riBoughtPercentage.dividedBy(1).compute().times(-1).getValue());

        return costItems.stream()
                .map(costItem -> {

                    final TraxNumber discount = riDiscount.times(costItem.cost())
                            .compute(String.format("RI discounted %s cost (BuyRI=%s, Cost Source Link=%s)",
                                    targetCostCategory.getDescriptorForType().getFullName(),
                                    isBuyRI, costItem.costSourceLink()));
                    return CostItem.builder()
                            .costSourceLink(CostSourceLink.of(costSource, Optional.of(costItem.costSourceLink())))
                            .cost(discount)
                            .build();
                }).collect(ImmutableList.toImmutableList());
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
    public int hashCode() {
        return Objects.hash(costSource, costSource, riData, isBuyRI, riBoughtPercentage);
    }

    @Override
    public boolean equals(final Object obj) {

        if (obj == null || !(obj instanceof RIDiscountJournalEntry)) {
            return false;
        } else if (obj == this) {
            return true;
        } else {
            final RIDiscountJournalEntry other = (RIDiscountJournalEntry)obj;
            return new EqualsBuilder()
                    .append(targetCostCategory, other.targetCostCategory)
                    .append(costSource, other.costSource)
                    .append(riData, other.riData)
                    .append(riBoughtPercentage, other.riBoughtPercentage)
                    .append(isBuyRI, other.isBuyRI)
                    .build();
        }
    }
}
