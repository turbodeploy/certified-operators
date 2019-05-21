package com.vmturbo.cost.calculation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.stringtemplate.v4.ST;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.asciithemes.a7.A7_Grids;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
     * Template for logging the cost journal.
     */
    private static final String JOURNAL_TEMPLATE =
            "\n================= NEW JOURNAL ====================\n" +
            "| Entity: <entityName>\n" +
            "| ID:     <entityId>\n" +
            "| Type:   <entityType>\n" +
            "================= FINAL COSTS ====================\n" +
            "<finalCosts>\n" +
            "================== DISCOUNT ======================\n" +
            "<discount>\n" +
            "================= DEPENDENCIES ===================\n" +
            "<dependencies>\n" +
            "================= RI COVERAGE ====================\n" +
            "<riEntries>\n" +
            "============== ON DEMAND ENTRIES =================\n" +
            "<onDemandEntries>\n" +
            "==================================================\n";


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
     * Get the journals this journal depends on. For example, a VM "inherits" the cost journals
     * of the volumes it's attached to.
     *
     * @return A stream of {@link CostJournal}s..
     */
    @Nonnull
    public Stream<CostJournal<ENTITY_CLASS>> getDependentJournals() {
        return childCostEntities.stream()
            .map(dependentCostLookup::getCostJournal)
            .filter(Objects::nonNull);
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
     * Get the aggregated hourly cost for all categories except the excluded categories.
     *
     * @param excludeCategories The categories to exclude.
     * @return The hourly cost for all categories except for the categories to exclude.
     */
    public double getTotalHourlyCostExcluding(@Nonnull final Set<CostCategory> excludeCategories) {
        calculateCosts();
        return finalCostsByCategory.entrySet().stream()
                .filter(e -> !excludeCategories.contains(e.getKey()))
                .map(Entry::getValue)
                .mapToDouble(d -> d)
                .sum();
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
     * Return whether this journal is empty or not. The cost of an empty journal is always 0.
     *
     * @return True if the journal is empty.
     */
    public boolean isEmpty() {
        // The journal is empty if there are no entries and no dependent costs.
        return costEntries.isEmpty() && childCostEntities.isEmpty();
    }

    @Override
    public String toString() {
        calculateCosts();
        final StringBuilder stringBuilder = new StringBuilder();
        final OnDemandEntryTabulator<ENTITY_CLASS> onDemandTabulator =
                new OnDemandEntryTabulator<>();
        final RIEntryTabulator<ENTITY_CLASS> riTabulator =
                new RIEntryTabulator<>();
        synchronized (finalCostsByCategory) {
            stringBuilder.append(new ST(JOURNAL_TEMPLATE)
                .add("entityName", infoExtractor.getName(entity))
                .add("entityId", infoExtractor.getId(entity))
                .add("entityType", infoExtractor.getEntityType(entity))
                .add("dependencies", childCostEntities.stream()
                    .map(entity -> {
                        final int type = infoExtractor.getEntityType(entity);
                        final StringBuilder descBuilder = new StringBuilder();
                        descBuilder.append("Name: ").append(infoExtractor.getName(entity))
                            .append(" Type: ").append((EntityType.forNumber(type) == null ? type : EntityType.forNumber(type)))
                            .append(" ID: ").append(infoExtractor.getId(entity));
                        CostJournal<ENTITY_CLASS> dependencyJournal = dependentCostLookup.getCostJournal(entity);
                        if (dependencyJournal != null) {
                            descBuilder.append("\n");
                            dependencyJournal.finalCostsByCategory.forEach((category, cost) -> descBuilder.append("    ").append(category).append(" : ").append(cost).append("\n"));
                        }
                        return descBuilder.toString();
                    })
                    .collect(Collectors.joining("\n")))
                .add("finalCosts", finalCostsByCategory.entrySet().stream()
                    .map(categoryEntry -> categoryEntry.getKey() + " : " + categoryEntry.getValue())
                    .collect(Collectors.joining("\n")))
                .add("discount", discountApplicator.toString())
                .add("riEntries", riTabulator.tabulateEntries(infoExtractor, costEntries))
                .add("onDemandEntries", onDemandTabulator.tabulateEntries(infoExtractor, costEntries))
                .render());
        }
        return stringBuilder.toString();
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
         * The number of coupons covered by this RI.
         */
        private final double couponsCovered;

        /**
         * The cost of the reserved instance for this entity.
         */
        private final CurrencyAmount hourlyCost;

        RIJournalEntry(@Nonnull final ReservedInstanceData riData,
                       final double couponsCovered,
                       @Nonnull final CurrencyAmount hourlyCost) {
            this.riData = riData;
            this.couponsCovered = couponsCovered;
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
            final CurrencyAmount unitPrice = price.getPriceAmount();
            final double discountPercentage = discountApplicator.getDiscountPercentage(payee);
            final double discountedUnitPrice = unitPrice.getAmount() * (1.0 - discountPercentage);
            final double totalPrice = unitsBought * discountedUnitPrice;
            logger.trace("Buying {} units at unit price {} with discount percentage {}",
                unitsBought, price.getUnit().name(), unitPrice.getAmount(), discountPercentage);
            switch (price.getUnit()) {
                case HOURS: {
                    cost = unitPrice.toBuilder()
                            .setAmount(totalPrice)
                            // Currency inherited from unit price.
                            .build();
                    break;
                }
                case DAYS: {
                    cost = unitPrice.toBuilder()
                        .setAmount(totalPrice / CostProtoUtil.HOURS_IN_DAY)
                        // Currency inherited from unit price.
                        .build();
                    break;
                }
                case MONTH:
                case MILLION_IOPS:
                case GB_MONTH: {
                    // In all of these cases, the key distinction is that the price is monthly,
                    // so to get the hourly price we need to divide.
                    cost = unitPrice.toBuilder()
                            .setAmount(totalPrice / CostProtoUtil.HOURS_IN_MONTH)
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
            Preconditions.checkArgument(category != CostCategory.RI_COMPUTE);
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
         * @param payee Data about the RI the cost is going to.
         * @param hourlyCost The hourly cost for using the RI.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder<ENTITY_CLASS_> recordRiCost(
                @Nonnull final ReservedInstanceData payee,
                final double couponsCovered,
                @Nonnull final CurrencyAmount hourlyCost) {
            if (logger.isTraceEnabled()) {
                logger.trace("RI {} coverage by {} at cost {}",
                        payee.getReservedInstanceBought().getId(), hourlyCost);
            }
            final List<JournalEntry<ENTITY_CLASS_>> prices =
                    costEntries.computeIfAbsent(CostCategory.RI_COMPUTE, k -> new ArrayList<>());
            prices.add(new RIJournalEntry<>(payee, couponsCovered, hourlyCost));
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

    /**
     * A helper class to tabulate a list of {@link JournalEntry}s in an ASCII table.
     * The logic of tabulation is closely tied to the entry, but we split it off for readability.
     *
     * @param <ENTITY_CLASS> See {@link JournalEntry}.
     */
    private abstract static class JournalEntryTabulator<ENTITY_CLASS> {

        private Class<? extends JournalEntry> entryClass;

        private JournalEntryTabulator(Class<? extends JournalEntry> entryClass) {
            this.entryClass = entryClass;
        }

        /**
         * Tabulate the entries of this tabulator's entry class into an ASCII table.
         *
         * @param infoExtractor The extractor to use to get information out of the entity.
         * @param entries The journal entries to tabulate. A single {@link JournalEntryTabulator}
         *               subclass will only tabulate a subset of the entries (matching the class).
         * @return The string of an ASCII table for the entry class of this tabulator.
         */
        @Nonnull
        String tabulateEntries(@Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                               @Nonnull final Map<CostCategory, List<JournalEntry<ENTITY_CLASS>>> entries) {
            final List<ColumnInfo> columnInfos = columnInfo();
            final CWC_LongestLine columnWidthCalculator = new CWC_LongestLine();
            columnInfos.forEach(colInfo -> columnWidthCalculator.add(colInfo.minWidth, colInfo.maxWidth));

            final AsciiTable table = new AsciiTable();
            table.getContext().setGrid(A7_Grids.minusBarPlusEquals());
            table.getRenderer().setCWC(columnWidthCalculator);
            table.addRule();
            table.addRow(Collections2.transform(columnInfos, colInfo -> colInfo.heading));
            boolean empty = true;
            for (Entry<CostCategory, List<JournalEntry<ENTITY_CLASS>>> categoryEntry : entries.entrySet()) {
                final List<JournalEntry<ENTITY_CLASS>> typeSpecificEntriesForCategory =
                        categoryEntry.getValue().stream()
                                .filter(journalEntry -> entryClass.isInstance(journalEntry))
                                .collect(Collectors.toList());
                empty = typeSpecificEntriesForCategory.isEmpty();
                if (!empty) {
                    table.addRule();
                    table.addRow(Stream.concat(
                            columnInfos.stream()
                                    .limit(columnInfos.size() - 1)
                                    .map(heading -> null),
                            Stream.of(categoryEntry.getKey().name())).collect(Collectors.toList()));
                    typeSpecificEntriesForCategory.forEach(entry -> {
                        table.addRule();
                        addToTable(entry, table, infoExtractor);
                    });
                }
            }

            if (empty) {
                return "None";
            }

            table.addRule();

            // Text alignment for the table must be set AFTER rows are added to it.
            table.setTextAlignment(TextAlignment.LEFT);
            return table.render();
        }

        /**
         * Add an entry of this tabulator's type to the ascii table being constructed.
         *
         * @param entry The entry to add. This entry will be of the proper subclass (although
         *              I haven't been able to get the generic parameters to work properly
         *              to guarantee it).
         * @param asciiTable The ascii table to add the row to.
         * @param infoExtractor The entity extractor to use.
         */
        abstract void addToTable(@Nonnull final JournalEntry<ENTITY_CLASS> entry,
                                 @Nonnull final AsciiTable asciiTable,
                                 @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor);

        /**
         * Return information about the columns for the table. Each tabulator will have
         * different columns (because we tabulate different information).
         *
         * @return The ordered list of {@link ColumnInfo}.
         */
        abstract List<ColumnInfo> columnInfo();
    }

    /**
     * Utility class to capture information about columns in the ASCII table for
     * {@link JournalEntryTabulator}s.
     */
    private static class ColumnInfo {
        private final String heading;
        private final int maxWidth;
        private final int minWidth;

        private ColumnInfo(final String heading, final int minWidth, final int maxWidth) {
            this.heading = heading;
            this.minWidth = minWidth;
            this.maxWidth = maxWidth;
        }
    }

    /**
     * A {@link JournalEntryTabulator} for {@link OnDemandJournalEntry}.
     *
     * @param <ENTITY_CLASS> See {@link JournalEntry}.
     */
    private static class OnDemandEntryTabulator<ENTITY_CLASS> extends JournalEntryTabulator<ENTITY_CLASS> {

        private static final List<ColumnInfo> COLUMN_INFOS = ImmutableList.<ColumnInfo>builder()
                .add(new ColumnInfo("Payee Name", 21, 31))
                .add(new ColumnInfo("Payee ID", 14, 18))
                .add(new ColumnInfo("Payee Type", 10, 20))
                .add(new ColumnInfo("Price Unit", 5, 12))
                .add(new ColumnInfo("End Range", 5, 10))
                .add(new ColumnInfo("Price", 10, 10))
                .add(new ColumnInfo("Amt Bought", 10, 10))
                .build();

        private OnDemandEntryTabulator() {
            super(OnDemandJournalEntry.class);
        }

        @Override
        public void addToTable(
                @Nonnull final JournalEntry<ENTITY_CLASS> entry,
                @Nonnull final AsciiTable asciiTable,
                @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor) {
            // We only care about on-demand journal entries, because this tabulator only
            // tabulates the on-demand entries.
            // This is an extra safeguard - the JournalEntryTabulator should already filter out
            // other types of entries.
            if (entry instanceof OnDemandJournalEntry) {
                OnDemandJournalEntry<ENTITY_CLASS> onDemandEntry = (OnDemandJournalEntry<ENTITY_CLASS>)entry;
                final int entityTypeInt = infoExtractor.getEntityType(onDemandEntry.payee);
                final EntityType entityType = EntityType.forNumber(entityTypeInt);

                asciiTable.addRow(
                    infoExtractor.getName(onDemandEntry.payee),
                    infoExtractor.getId(onDemandEntry.payee),
                    entityType == null ? Integer.toString(entityTypeInt) : entityType.name(),
                    onDemandEntry.price.getUnit().name(),
                    onDemandEntry.price.getEndRangeInUnits() == 0 ? "Inf" : Long.toString(onDemandEntry.price.getEndRangeInUnits()),
                    Double.toString(onDemandEntry.price.getPriceAmount().getAmount()),
                    Double.toString(onDemandEntry.unitsBought));
            }
        }

        @Nonnull
        @Override
        List<ColumnInfo> columnInfo() {
            return COLUMN_INFOS;
        }
    }

    /**
     * A {@link JournalEntryTabulator} for {@link RIJournalEntry}.
     *
     * @param <ENTITY_CLASS> See {@link JournalEntry}.
     */
    private static class RIEntryTabulator<ENTITY_CLASS> extends JournalEntryTabulator<ENTITY_CLASS> {

        private static final List<ColumnInfo> COLUMN_INFOS = ImmutableList.<ColumnInfo>builder()
                .add(new ColumnInfo("RI Tier ID", 14, 18))
                .add(new ColumnInfo("RI ID", 14, 18))
                .add(new ColumnInfo("RI Spec ID", 14, 18))
                .add(new ColumnInfo("Coupons", 10, 10))
                .add(new ColumnInfo("Cost", 10, 10))
                .build();

        private RIEntryTabulator() {
            super(RIJournalEntry.class);
        }

        @Override
        void addToTable(@Nonnull final JournalEntry<ENTITY_CLASS> entry,
                        @Nonnull final AsciiTable asciiTable,
                        @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor) {
            if (entry instanceof RIJournalEntry) {
                RIJournalEntry<ENTITY_CLASS> riEntry = (RIJournalEntry<ENTITY_CLASS>) entry;
                asciiTable.addRow(
                        // It would be nice to get the name of the tier here, if we can do it without
                        // too much overhead.
                        riEntry.riData.getReservedInstanceSpec().getReservedInstanceSpecInfo().getTierId(),
                        riEntry.riData.getReservedInstanceBought().getId(),
                        riEntry.riData.getReservedInstanceSpec().getId(),
                        riEntry.couponsCovered,
                        riEntry.hourlyCost.getAmount());
            }

        }

        @Override
        @Nonnull
        List<ColumnInfo> columnInfo() {
            return COLUMN_INFOS;
        }
    }
}
