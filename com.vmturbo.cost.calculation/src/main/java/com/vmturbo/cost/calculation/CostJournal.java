package com.vmturbo.cost.calculation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

/**
 * The {@link CostJournal} collects the elements that contribute to the cost of a particular entity.
 *
 * TODO (roman, Aug 17th 2018): Right now costs are calculated on a per-entity basis. It may make
 * more sense for the cost journal to be a global (to the topology), append-only object.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
@Immutable
public class CostJournal<ENTITY_CLASS> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The entity for which the cost is calculated.
     */
    private final ENTITY_CLASS entity;

    private final EntityInfoExtractor<ENTITY_CLASS> infoExtractor;

    private final DiscountApplicator<ENTITY_CLASS> discountApplicator;

    private final Map<CostCategory, List<JournalEntry<ENTITY_CLASS>>> costEntries;

    private final Map<CostCategory, Double> finalCostsByCategory = Collections.synchronizedMap(new HashMap<>());

    /**
     * We calculate the costs from the journal entries the first time costs are actually requested.
     * However, we want to avoid calculating multiple times, so we use an atomic boolean to make
     * sure the calculation is only ever done once.
     */
    private final AtomicBoolean calculationStarted = new AtomicBoolean(false);

    /**
     * These are the entities whose costs should be factored into the cost of this journal.
     */
    private final List<ENTITY_CLASS> childCostEntities;

    private final DependentCostLookup<ENTITY_CLASS> dependentCostLookup;

    private CostJournal(@Nonnull final ENTITY_CLASS entity,
                        @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                        @Nonnull final DiscountApplicator<ENTITY_CLASS> discountApplicator,
                        @Nonnull final Map<CostCategory, List<JournalEntry<ENTITY_CLASS>>> costEntries,
                        @Nonnull final List<ENTITY_CLASS> childCostEntities,
                        @Nonnull final DependentCostLookup<ENTITY_CLASS> dependentCostLookup) {
        this.entity = entity;
        this.infoExtractor = infoExtractor;
        this.discountApplicator = discountApplicator;
        this.costEntries = Collections.unmodifiableMap(costEntries);
        this.childCostEntities = childCostEntities;
        this.dependentCostLookup = dependentCostLookup;
    }

    private void calculateCosts() {
        if (calculationStarted.compareAndSet(false, true)) {
            // Note - this should "block" other operations on the map until the calculation is done.
            synchronized (finalCostsByCategory) {
                logger.trace("Summing price entries.");
                costEntries.forEach((category, priceEntries) -> {
                    double aggregateHourly = 0.0;
                    logger.trace("Aggregating hourly costs for category {}", category);
                    for (JournalEntry<ENTITY_CLASS> journalEntry : priceEntries) {
                        aggregateHourly += journalEntry.calculateHourlyCost(infoExtractor, discountApplicator)
                            // TODO (roman, Sept 22 2018): Handle currency conversion when aggregating
                            // hourly costs - or accept the desired currency as a parameter in this function,
                            // and convert all currencies to that.
                            .getAmount();
                    }
                    logger.trace("Aggregated hourly cost for category {} is {}", category, aggregateHourly);
                    finalCostsByCategory.put(category, aggregateHourly);
                });
                logger.trace("Inheriting costs...");
                // Go through the child cost entities, and add their costs to the
                // costs calculated so far.
                childCostEntities.forEach(childCostEntity -> {
                    final CostJournal<ENTITY_CLASS> journal =
                            dependentCostLookup.getCostJournal(childCostEntity);
                    if (journal == null) {
                        logger.error("Could not find costs for child entity {} (id: {})",
                            infoExtractor.getName(childCostEntity),
                            infoExtractor.getId(childCostEntity));
                    } else {
                        logger.trace(() -> "Inheriting costs from " + infoExtractor.getName(childCostEntity)
                                + " id: " + infoExtractor.getId(childCostEntity));
                        // Make sure the child journal's prices have already been calculated.
                        // Hope there are no circular dependencies, fingers crossed!
                        journal.calculateCosts();
                        journal.getCategories().forEach(category ->
                            finalCostsByCategory.compute(category, (k, curPrice) -> {
                                final double inheritedCost =
                                        journal.getHourlyCostForCategory(category);
                                final double newCost = inheritedCost + (curPrice == null ? 0 : curPrice);
                                logger.trace("Inheriting {} for category {}. New cost is: {}",
                                        inheritedCost, category, newCost);
                                return newCost;
                            }));
                    }
                });
                logger.trace("Finished inheriting cost.");
            }
        }
    }

    @Nonnull
    public ENTITY_CLASS getEntity() {
        return entity;
    }

    /**
     * Convert this entry to an {@link EntityCost} protobuf that can be sent over the wire.
     *
     * @return The {@link EntityCost} protobuf message representing this entry.
     */
    @Nonnull
    public EntityCost toEntityCostProto() {
        calculateCosts();
        final EntityCost.Builder costBuilder = EntityCost.newBuilder()
            .setAssociatedEntityId(infoExtractor.getId(entity))
            .setAssociatedEntityType(infoExtractor.getEntityType(entity))
            .setTotalAmount(CurrencyAmount.newBuilder()
                .setAmount(getTotalHourlyCost()));
        getCategories().forEach(category -> {
            costBuilder.addComponentCost(ComponentCost.newBuilder()
                .setCategory(category)
                .setAmount(CurrencyAmount.newBuilder()
                    .setAmount(getHourlyCostForCategory(category))));
        });
        return costBuilder.build();
    }

    /**
     * Get the aggregated hourly cost for a particular category.
     *
     * @param category The category.
     * @return The hourly cost for the category.
     */
    public double getHourlyCostForCategory(@Nonnull final CostCategory category) {
        calculateCosts();
        return finalCostsByCategory.getOrDefault(category, 0.0);
    }

    /**
     * Get the categories that this journal has prices for.
     *
     * @return The set of categories.
     */
    @Nonnull
    public Set<CostCategory> getCategories() {
        calculateCosts();
        return Collections.unmodifiableSet(finalCostsByCategory.keySet());
    }

    /**
     * Get the total (hourly) cost for the entity.
     *
     * @return The total (hourly) cost for the entity.
     */
    public double getTotalHourlyCost() {
        calculateCosts();
        return finalCostsByCategory.values().stream().mapToDouble(d -> d).sum();
    }

    /**
     * A factory method to create an empty cost journal.
     *
     * @param <ENTITY_CLASS> The entity class (see {@link CostJournal}).
     * @return An empty {@link CostJournal} that will return 0 for all costs.
     */
    public static <ENTITY_CLASS> CostJournal<ENTITY_CLASS> empty(
            @Nonnull final ENTITY_CLASS entity,
            @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor) {
        return new CostJournal<>(entity, infoExtractor,
                DiscountApplicator.noDiscount(),
                Collections.emptyMap(),
                Collections.emptyList(),
                e -> null);
    }

    /**
     * Create a builder used to construct a new {@link CostJournal}.
     *
     * @param <ENTITY_CLASS> The entity class (see {@link CostJournal}).
     * @return The {@link Builder}.
     */
    public static <ENTITY_CLASS> Builder<ENTITY_CLASS> newBuilder(
            @Nonnull final ENTITY_CLASS entity,
            @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
            @Nonnull final ENTITY_CLASS region,
            @Nonnull final DiscountApplicator<ENTITY_CLASS> discountApplicator,
            @Nonnull final DependentCostLookup<ENTITY_CLASS> dependentCostLookup) {
        return new Builder<>(entity, infoExtractor, region, discountApplicator, dependentCostLookup);
    }

    /**
     * A single item contributing to the cost of an entity.
     *
     * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
     *                      {@link TopologyEntityDTO} for the realtime topology. Extra _ at the
     *                       end of the name so that it doesn't hide the outer ENTITY_CLASS, even
     *                       though they will be the same type.
     */
    public interface JournalEntry<ENTITY_CLASS> {

        /**
         * Calculate the hourly cost of this entry.
         *
         * @param infoExtractor The {@link EntityInfoExtractor}, mainly for debugging purposes.
         * @param discountApplicator The {@link DiscountApplicator} containing the discount for
         *                           the entity whose journal this entry belongs to.
         * @return The hourly cost.
         */
        CurrencyAmount calculateHourlyCost(@Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                               @Nonnull final DiscountApplicator<ENTITY_CLASS> discountApplicator);
    }

    /**
     * A {@link JournalEntry} for payments covered by reserved instances. One entity may have
     * entries for several reserved instances, if multiple RI's are partially covering the entity.
     *
     * @param <ENTITY_CLASS_> See {@link JournalEntry}.
     */
    @Immutable
    public static class RIJournalEntry<ENTITY_CLASS_> implements JournalEntry<ENTITY_CLASS_> {

        /**
         * The data about the reserved instance.
         */
        private final ReservedInstanceData riData;

        /**
         * The cost of the reserved instance for this entity.
         */
        private final CurrencyAmount hourlyCost;

        RIJournalEntry(@Nonnull final ReservedInstanceData riData,
                       @Nonnull final CurrencyAmount hourlyCost) {
            this.riData = riData;
            this.hourlyCost = hourlyCost;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CurrencyAmount calculateHourlyCost(@Nonnull final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor,
                          @Nonnull final DiscountApplicator<ENTITY_CLASS_> discountApplicator) {
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
            final double discountPercentage = discountApplicator.getDiscountPercentage(providerId);
            return hourlyCost.toBuilder()
                .setAmount(hourlyCost.getAmount() * (1.0 - discountPercentage))
                .build();
        }
    }

    /**
     * A {@link JournalEntry} for on-demand payments to entities in the topology.
     *
     * @param <ENTITY_CLASS_> See {@link JournalEntry}
     */
    @Immutable
    public static class OnDemandJournalEntry<ENTITY_CLASS_> implements JournalEntry<ENTITY_CLASS_> {
        /**
         * The payee - i.e. the entity that's selling the item to the buyer.
         */
        private final ENTITY_CLASS_ payee;

        /**
         * The unit price at which the payee is selling whatever item the {@link JournalEntry}
         * represents.
         */
        private final Price price;

        /**
         * The number of units of the item that the buyer is buying from the payee. This can
         * be combined with the price to get the cost of the item to the buyer.
         *
         * The number can be
         */
        private final double unitsBought;

        OnDemandJournalEntry(@Nonnull final ENTITY_CLASS_ payee,
                             @Nonnull final Price price,
                             final double unitsBought) {
            Preconditions.checkArgument(unitsBought >= 0);
            this.payee = payee;
            this.price = price;
            this.unitsBought = unitsBought;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CurrencyAmount calculateHourlyCost(@Nonnull final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor,
                                          @Nonnull final DiscountApplicator<ENTITY_CLASS_> discountApplicator) {
            logger.trace("Calculating hourly cost for purchase from entity {} of type {}",
                    infoExtractor.getId(payee), infoExtractor.getEntityType(payee));
            final CurrencyAmount cost;
            switch (price.getUnit()) {
                case HOURS: {
                    final CurrencyAmount unitPrice = price.getPriceAmount();
                    final double discountPercentage = discountApplicator.getDiscountPercentage(payee);
                    final double discountedUnitPrice = unitPrice.getAmount() * (1.0 - discountPercentage);
                    logger.trace("Buying {} units at unit price {} with discount percentage {}",
                            unitsBought, unitPrice.getAmount(), discountPercentage);
                    cost = unitPrice.toBuilder()
                            .setAmount(unitsBought * discountedUnitPrice)
                            // Currency inherited from unit price.
                            .build();
                    break;
                }
                case MONTH:
                case MILLION_IOPS:
                case GB_MONTH: {
                    // In all of these cases, the key distinction is that the price is monthly,
                    // so to get the hourly price we need to divide.
                    final CurrencyAmount unitPrice = price.getPriceAmount();
                    final double discountPercentage = discountApplicator.getDiscountPercentage(payee);
                    final double discountedUnitPrice = unitPrice.getAmount() * (1.0 - discountPercentage);
                    logger.trace("Buying {} units at monthly unit price {} with discount percentage {}",
                            unitsBought, unitPrice.getAmount(), discountPercentage);
                    cost = unitPrice.toBuilder()
                            .setAmount((unitsBought * discountedUnitPrice) / CostProtoUtil.HOURS_IN_MONTH)
                            // Currency inherited from unit price.
                            .build();
                    break;
                }
                default:
                    logger.warn("Unsupported unit: {}", price.getUnit());
                    cost = CurrencyAmount.getDefaultInstance();
                    break;
            }
            logger.trace("Purchase from entity {} of type {} has cost: {}",
                    infoExtractor.getId(payee), infoExtractor.getEntityType(payee), cost);
            return cost;
        }
    }

    /**
     * Builder for the {@link CostJournal}.
     *
     * TODO (roman, Aug 16th 2018): The intention is to have the cost journal be
     * immutable once constructed, but in reality it  may be better to have the cost journal
     * be mutable (append-only), and track operations on it.
     *
     * @param <ENTITY_CLASS_> The class used to represent entities in the topology. For example,
     *                      {@link TopologyEntityDTO} for the realtime topology. Extra _ at the
     *                       end of the name so that it doesn't hide the outer ENTITY_CLASS, even
     *                       though they will be the same type.
     */
    public static class Builder<ENTITY_CLASS_> {

        /**
         * The entity the journal is for.
         */
        private final ENTITY_CLASS_ entity;

        private final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor;

        /**
         * The region the entity is in.
         */
        private final ENTITY_CLASS_ region;

        private final DiscountApplicator<ENTITY_CLASS_> discountApplicator;

        private final Map<CostCategory, List<JournalEntry<ENTITY_CLASS_>>> costEntries =
                new EnumMap<>(CostCategory.class);

        private final List<ENTITY_CLASS_> childCosts = new ArrayList<>();

        private final DependentCostLookup<ENTITY_CLASS_> dependentCostLookup;

        private Builder(@Nonnull final ENTITY_CLASS_ entity,
                        @Nonnull final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor,
                        @Nonnull final ENTITY_CLASS_ region,
                        @Nonnull final DiscountApplicator<ENTITY_CLASS_> discountApplicator,
                        @Nonnull final DependentCostLookup<ENTITY_CLASS_> dependentCostLookup) {
            this.entity = Objects.requireNonNull(entity);
            this.infoExtractor = Objects.requireNonNull(infoExtractor);
            this.region = Objects.requireNonNull(region);
            this.discountApplicator = Objects.requireNonNull(discountApplicator);
            this.dependentCostLookup = Objects.requireNonNull(dependentCostLookup);
        }

        /**
         * Get the entity the journal will be for.
         *
         * @return The entity reference.
         */
        @Nonnull
        public ENTITY_CLASS_ getEntity() {
            return entity;
        }

        /**
         * Record an on-demand cost for an entity.
         *
         * @param category The category for the cost.
         * @param payee The payee (mainly for debugging purposes).
         * @param price The price at which the entity is purchasing from the payee.
         * @param amount The amount bought at the price.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder<ENTITY_CLASS_> recordOnDemandCost(
                @Nonnull final CostCategory category,
                @Nonnull final ENTITY_CLASS_ payee,
                @Nonnull final Price price,
                final double amount) {
            if (logger.isTraceEnabled()) {
                logger.trace("On-demand {} purchase of {} from payee {} (id: {}) at price {}",
                        category, amount, infoExtractor.getName(payee), infoExtractor.getId(payee), price);
            }
            final List<JournalEntry<ENTITY_CLASS_>> prices = costEntries.computeIfAbsent(category, k -> new ArrayList<>());
            prices.add(new OnDemandJournalEntry<>(payee, price, amount));
            return this;
        }

        @Nonnull
        public Builder<ENTITY_CLASS_> inheritCost(@Nonnull final ENTITY_CLASS_ entity) {
            if (logger.isTraceEnabled()) {
                logger.trace("Inheriting costs of entity {} (id: {})",
                        infoExtractor.getName(entity), infoExtractor.getId(entity));
            }
            childCosts.add(entity);
            return this;
        }

        /**
         * Record a reserved instance cost for an entity. RI costs are for demands that are filled
         * by reserved instances, instead of providers in the topology.
         *
         * @param category The category for the cost.
         * @param payee Data about the RI the cost is going to.
         * @param hourlyCost The hourly cost for using the RI.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder<ENTITY_CLASS_> recordRiCost(
                @Nonnull final CostCategory category,
                @Nonnull final ReservedInstanceData payee,
                @Nonnull final CurrencyAmount hourlyCost) {
            if (logger.isTraceEnabled()) {
                logger.trace("RI {} coverage by {} at cost {}", category,
                        payee.getReservedInstanceBought().getId(), hourlyCost);
            }
            final List<JournalEntry<ENTITY_CLASS_>> prices = costEntries.computeIfAbsent(category, k -> new ArrayList<>());
            prices.add(new RIJournalEntry<>(payee, hourlyCost));
            return this;
        }


        @Nonnull
        public CostJournal<ENTITY_CLASS_> build() {
            return new CostJournal<>(entity,
                    infoExtractor,
                    discountApplicator,
                    costEntries,
                    childCosts,
                    dependentCostLookup);
        }
    }
}
