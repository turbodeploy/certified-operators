package com.vmturbo.cost.calculation.journal.entry;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
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
     private final TraxNumber enityUptimeDiscountMultiplier;



    /**
     * EntityUptime Journal entry constructor.
     * @param targetCostCategory Cost category.
     * @param enityUptimeDiscountMultiplier the discount multiplier to be applied on the price.
     */
    public EntityUptimeDiscountJournalEntry(@Nonnull final CostCategory targetCostCategory,
                                            @Nonnull final TraxNumber enityUptimeDiscountMultiplier) {
        this.targetCostCategory = targetCostCategory;
        this.enityUptimeDiscountMultiplier = enityUptimeDiscountMultiplier;
    }

    @Override
    public TraxNumber calculateHourlyCost(@Nonnull final EntityInfoExtractor infoExtractor,
                                          @Nonnull final DiscountApplicator discountApplicator,
                                          @Nonnull final RateExtractor rateExtractor) {
        // Retrieve the sum total of prices for all sources.
        TraxNumber price = rateExtractor.lookupCostWithFilter(targetCostCategory, cs -> true);
        return price.times(-1).compute()
                .times(enityUptimeDiscountMultiplier).compute();
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
    public int compareTo(final Object o) {
        return Integer.MIN_VALUE;
    }
}
