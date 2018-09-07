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
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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

    private final Map<CostCategory, List<JournalEntry<ENTITY_CLASS>>> onDemandPriceEntries;

    private final Map<CostCategory, Double> finalPricesByCategory;

    private CostJournal(@Nonnull final ENTITY_CLASS entity,
                        @Nullable final Map<CostCategory, List<JournalEntry<ENTITY_CLASS>>> priceEntries) {
        this.entity = entity;
        this.onDemandPriceEntries = priceEntries == null ?
                Collections.emptyMap() : Collections.unmodifiableMap(priceEntries);
        this.finalPricesByCategory = sumPriceEntries();
    }

    @Nonnull
    private Map<CostCategory, Double> sumPriceEntries() {
        final Map<CostCategory, Double> summedMap = new HashMap<>();
        onDemandPriceEntries.forEach((category, priceEntries) -> {
            double aggregateHourly = 0.0;
            for (JournalEntry<ENTITY_CLASS> journalEntry : priceEntries) {
                if (journalEntry.getPrice().getUnit() == Unit.HOURS) {
                    CurrencyAmount currencyAmount = journalEntry.getPrice().getPriceAmount();
                    if (currencyAmount.getCurrency() == CurrencyAmount.getDefaultInstance().getCurrency()) {
                        final double unitPrice = currencyAmount.getAmount();
                        aggregateHourly += journalEntry.getAmountPurchased() * unitPrice;
                    } else {
                        logger.warn("Unsupported currency: {}", currencyAmount.getCurrency());
                    }
                } else {
                    logger.warn("Unsupported unit: {}", journalEntry.getPrice().getUnit());
                }
            }
            summedMap.put(category, aggregateHourly);
        });
        return Collections.unmodifiableMap(summedMap);
    }

    @Nonnull
    public ENTITY_CLASS getEntity() {
        return entity;
    }

    /**
     * Get the aggregated cost for a particular category.
     *
     * @param category The category.
     * @return The cost for the category.
     */
    public double getCostForCategory(@Nonnull final CostCategory category) {
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
     * Get the total cost for the entity.
     *
     * @return The total cost for the entity.
     */
    public double getTotalCost() {
        return finalPricesByCategory.values().stream().mapToDouble(d -> d).sum();
    }

    /**
     * A factory method to create an empty cost journal.
     *
     * @param <ENTITY_CLASS> The entity class (see {@link CostJournal}).
     * @return An empty {@link CostJournal} that will return 0 for all costs.
     */
    public static <ENTITY_CLASS> CostJournal<ENTITY_CLASS> empty(@Nonnull final ENTITY_CLASS entity) {
        return new CostJournal<>(entity, null);
    }

    /**
     * Create a builder used to construct a new {@link CostJournal}.
     *
     * @param <ENTITY_CLASS> The entity class (see {@link CostJournal}).
     * @return The {@link Builder}.
     */
    public static <ENTITY_CLASS> Builder<ENTITY_CLASS> newBuilder(@Nonnull final ENTITY_CLASS entity,
                                                                  @Nonnull final ENTITY_CLASS region) {
        return new Builder<>(entity, region);
    }

    /**
     * A single item contributing to the cost of an entity.
     *
     * @param <ENTITY_CLASS_> The class used to represent entities in the topology. For example,
     *                      {@link TopologyEntityDTO} for the realtime topology. Extra _ at the
     *                       end of the name so that it doesn't hide the outer ENTITY_CLASS, even
     *                       though they will be the same type.
     */
    @Immutable
    public static class JournalEntry<ENTITY_CLASS_> {
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
         * The amount of the item that the buyer is buying from the payee, in units. This can
         * be combined with the price to get the cost of the item to the buyer.
         */
        private final double amount;

        public JournalEntry(@Nonnull final ENTITY_CLASS_ payee,
                            @Nonnull final Price price,
                            final double amount) {
            this.payee = payee;
            this.price = price;
            this.amount = amount;
        }

        @Nonnull
        public ENTITY_CLASS_ getPayee() {
            return payee;
        }

        @Nonnull
        public Price getPrice() {
            return price;
        }

        public double getAmountPurchased() {
            return amount;
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


        /**
         * The region the entity is in.
         */
        private final ENTITY_CLASS_ region;

        private final Map<CostCategory, List<JournalEntry<ENTITY_CLASS_>>> priceEntries =
                new EnumMap<>(CostCategory.class);

        private Builder(@Nonnull final ENTITY_CLASS_ entity,
                        @Nonnull final ENTITY_CLASS_ region) {
            this.entity = Objects.requireNonNull(entity);
            this.region = Objects.requireNonNull(region);
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
            prices.add(new JournalEntry<>(payee, price, amount));
            return this;
        }

        @Nonnull
        public CostJournal<ENTITY_CLASS_> build() {
            return new CostJournal<>(entity, priceEntries);
        }
    }
}
