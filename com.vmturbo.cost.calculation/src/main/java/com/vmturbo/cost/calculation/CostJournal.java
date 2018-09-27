package com.vmturbo.cost.calculation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

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

    private final Map<CostCategory, List<JournalEntry<ENTITY_CLASS>>> onDemandPriceEntries;

    private final Map<CostCategory, Double> finalPricesByCategory;

    private CostJournal(@Nonnull final ENTITY_CLASS entity,
                        @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                        @Nonnull final DiscountApplicator<ENTITY_CLASS> discountApplicator,
                        @Nonnull final Map<CostCategory, List<JournalEntry<ENTITY_CLASS>>> priceEntries) {
        this.entity = entity;
        this.infoExtractor = infoExtractor;
        this.discountApplicator = discountApplicator;
        this.onDemandPriceEntries = Collections.unmodifiableMap(priceEntries);
        this.finalPricesByCategory = sumPriceEntries();
    }

    @Nonnull
    private Map<CostCategory, Double> sumPriceEntries() {
        final Map<CostCategory, Double> summedMap = new HashMap<>();
        onDemandPriceEntries.forEach((category, priceEntries) -> {
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
            summedMap.put(category, aggregateHourly);
        });
        return Collections.unmodifiableMap(summedMap);
    }

    @Nonnull
    public ENTITY_CLASS getEntity() {
        return entity;
    }

    /**
     * Get the aggregated hourly cost for a particular category.
     *
     * @param category The category.
     * @return The hourly cost for the category.
     */
    public double getHourlyCostForCategory(@Nonnull final CostCategory category) {
        return finalPricesByCategory.getOrDefault(category, 0.0);
    }

    /**
     * Get the categories that this journal has prices for.
     *
     * @return The set of categories.
     */
    @Nonnull
    public Set<CostCategory> getCategories() {
        return Collections.unmodifiableSet(finalPricesByCategory.keySet());
    }

    /**
     * Get the total (hourly) cost for the entity.
     *
     * @return The total (hourly) cost for the entity.
     */
    public double getTotalHourlyCost() {
        return finalPricesByCategory.values().stream().mapToDouble(d -> d).sum();
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
        return new CostJournal<>(entity, infoExtractor, DiscountApplicator.noDiscount(), Collections.emptyMap());
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
            @Nonnull final DiscountApplicator<ENTITY_CLASS> discountApplicator) {
        return new Builder<>(entity, infoExtractor, region, discountApplicator);
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
            if (price.getUnit() == Unit.HOURS) {
                final CurrencyAmount unitPrice = price.getPriceAmount();
                final double discountPercentage = discountApplicator.getDiscountPercentage(payee);
                final double discountedUnitPrice = unitPrice.getAmount() * (1.0 - discountPercentage);
                logger.trace("Buying {} units at unit price {} with discount percentage {}",
                        unitsBought, unitPrice.getAmount(), discountPercentage);
                cost = unitPrice.toBuilder()
                    .setAmount(unitsBought * discountedUnitPrice)
                    // Currency inherited from unit price.
                    .build();
            } else {
                logger.warn("Unsupported unit: {}", price.getUnit());
                cost = CurrencyAmount.getDefaultInstance();
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

        private final Map<CostCategory, List<JournalEntry<ENTITY_CLASS_>>> priceEntries =
                new EnumMap<>(CostCategory.class);

        private Builder(@Nonnull final ENTITY_CLASS_ entity,
                        @Nonnull final EntityInfoExtractor<ENTITY_CLASS_> infoExtractor,
                        @Nonnull final ENTITY_CLASS_ region,
                        @Nonnull final DiscountApplicator<ENTITY_CLASS_> discountApplicator) {
            this.entity = Objects.requireNonNull(entity);
            this.infoExtractor = Objects.requireNonNull(infoExtractor);
            this.region = Objects.requireNonNull(region);
            this.discountApplicator = Objects.requireNonNull(discountApplicator);
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
            final List<JournalEntry<ENTITY_CLASS_>> prices = priceEntries.computeIfAbsent(category, k -> new ArrayList<>());
            prices.add(new OnDemandJournalEntry<>(payee, price, amount));
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
            final List<JournalEntry<ENTITY_CLASS_>> prices = priceEntries.computeIfAbsent(category, k -> new ArrayList<>());
            prices.add(new RIJournalEntry<>(payee, hourlyCost));
            return this;
        }


        @Nonnull
        public CostJournal<ENTITY_CLASS_> build() {
            return new CostJournal<>(entity, infoExtractor, discountApplicator, priceEntries);
        }
    }
}
