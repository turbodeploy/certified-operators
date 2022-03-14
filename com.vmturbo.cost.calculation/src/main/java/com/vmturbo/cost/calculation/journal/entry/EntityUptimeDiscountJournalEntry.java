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
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostItem;
import com.vmturbo.cost.calculation.journal.CostItem.CostSourceLink;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.journal.CostJournal.RateExtractor;
import com.vmturbo.trax.TraxNumber;

/**
 * Journal entry for discounts related to entity uptime.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
public class EntityUptimeDiscountJournalEntry<E> implements  QualifiedJournalEntry<E> {


    private final CostCategory targetCostCategory;

     /**
     * For VM on demand cost, the uptime discount multiplier is an inverse of the uptime percentage
      * As an example, if a VM is up for 80% of the time, the discount multiplier will be 0.2.
      * For IP cost for the same VM on the other hand would be discounted by 80 % since VMs are
      * charged for IP only when VMs are not using them.
     */
     private final TraxNumber entityUptimeDiscountMultiplier;



    /**
     * EntityUptime Journal entry constructor.
     * @param targetCostCategory Cost category.
     * @param entityUptimeDiscountMultiplier the discount multiplier to be applied on the price.
     */
    public EntityUptimeDiscountJournalEntry(@Nonnull final CostCategory targetCostCategory,
                                            @Nonnull final TraxNumber entityUptimeDiscountMultiplier) {
        this.targetCostCategory = targetCostCategory;
        this.entityUptimeDiscountMultiplier = entityUptimeDiscountMultiplier;
    }

    @Override
    public Collection<CostItem> calculateHourlyCost(@Nonnull final EntityInfoExtractor infoExtractor,
                                          @Nonnull final DiscountApplicator discountApplicator,
                                          @Nonnull final RateExtractor rateExtractor) {
        // Retrieve the sum total of prices for all sources.
        final Collection<CostItem> costItems = rateExtractor.lookupCostWithFilter(targetCostCategory, CostSourceFilter.INCLUDE_ALL);

        return costItems.stream()
                .map(costItem -> {
                    final TraxNumber discount = costItem.cost()
                            .times(-1).compute()
                            .times(entityUptimeDiscountMultiplier).compute();

                    return CostItem.builder()
                            .costSourceLink(CostSourceLink.of(
                                    CostSource.ENTITY_UPTIME_DISCOUNT,
                                    Optional.of(costItem.costSourceLink())))
                            .cost(discount)
                            .build();
                }).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public Optional<CostSource> getCostSource() {
        return Optional.of(CostSource.ENTITY_UPTIME_DISCOUNT);
    }

    @Nonnull
    @Override
    public CostCategory getCostCategory() {
        return targetCostCategory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetCostCategory, entityUptimeDiscountMultiplier);
    }

    @Override
    public boolean equals(final Object obj) {

        if (obj == null || !(obj instanceof EntityUptimeDiscountJournalEntry)) {
            return false;
        } else if (obj == this) {
            return true;
        } else {
            final EntityUptimeDiscountJournalEntry other = (EntityUptimeDiscountJournalEntry)obj;
            return new EqualsBuilder()
                    .append(targetCostCategory, other.targetCostCategory)
                    .append(entityUptimeDiscountMultiplier, other.entityUptimeDiscountMultiplier)
                    .build();
        }
    }
}
