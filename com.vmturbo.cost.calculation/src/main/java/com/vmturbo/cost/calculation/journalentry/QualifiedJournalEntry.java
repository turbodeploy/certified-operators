package com.vmturbo.cost.calculation.journalentry;

import javax.annotation.Nonnull;

import java.util.Optional;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CostJournal.RateExtractor;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.trax.TraxNumber;

/**
 * A single item contributing to the cost of an entity.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology. Extra _ at the
 *                       end of the name so that it doesn't hide the outer ENTITY_CLASS, even
 *                       though they will be the same type.
 */
public interface QualifiedJournalEntry<ENTITY_CLASS> extends Comparable {
    /**
     * Calculate the hourly cost of this entry.
     *
     * @param infoExtractor The {@link EntityInfoExtractor}, mainly for debugging purposes.
     * @param discountApplicator The {@link DiscountApplicator} containing the discount for
     *                           the entity whose journal this entry belongs to.
     * @param rateExtractor The functional interface to extract the rate for a particular cost source and category.
     *
     * @return The hourly cost.
     */
    TraxNumber calculateHourlyCost(@Nonnull EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                                   @Nonnull DiscountApplicator<ENTITY_CLASS> discountApplicator,
                                   @Nonnull RateExtractor rateExtractor);

    /**
     * Get the cost source associated with the journal entry.
     *
     * @return An optional field representing the cost source.
     */
    @Nonnull
    Optional<CostSource> getCostSource();

    /**
     * Get the cost category associated with the journal entry.
     *
     * @return The cost category.
     */
    @Nonnull
    CostCategory getCostCategory();
}
