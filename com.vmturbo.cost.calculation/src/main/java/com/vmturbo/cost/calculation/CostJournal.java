package com.vmturbo.cost.calculation;

import static com.vmturbo.trax.Trax.trax;
import static com.vmturbo.trax.Trax.traxConstant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.stringtemplate.v4.ST;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.asciithemes.a7.A7_Grids;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journalentry.OnDemandJournalEntry;
import com.vmturbo.cost.calculation.journalentry.QualifiedJournalEntry;
import com.vmturbo.cost.calculation.journalentry.RIDiscountJournalEntry;
import com.vmturbo.cost.calculation.journalentry.RIJournalEntry;
import com.vmturbo.cost.calculation.journalentry.ReservedLicenseJournalEntry;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.trax.TraxCollectors;
import com.vmturbo.trax.TraxNumber;

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

    private static final Map<CostCategory, TraxNumber> NO_COST_MAP = new EnumMap<>(CostCategory.class);
    static {
        for (CostCategory category : CostCategory.values()) {
            NO_COST_MAP.put(category, traxConstant(0, "No " + category.name() + " cost"));
        }
    }

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

    private final Map<CostCategory, SortedSet<QualifiedJournalEntry<ENTITY_CLASS>>> costEntries;

    private final Table<CostCategory, CostSource, TraxNumber> finalCostsByCategoryAndSource = HashBasedTable.create();

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
                        @Nonnull final Map<CostCategory, SortedSet<QualifiedJournalEntry<ENTITY_CLASS>>> costEntries,
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
            synchronized (finalCostsByCategoryAndSource) {
                logger.trace("Summing price entries.");
                costEntries.forEach((category, priceEntries) -> {
                    TraxNumber aggregateHourly = trax(0);
                    for (QualifiedJournalEntry<ENTITY_CLASS> entry: priceEntries) {
                        TraxNumber cost = entry.calculateHourlyCost(infoExtractor, discountApplicator, this::getHourlyCostFilterEntries);
                        CostSource costSource = entry.getCostSource().orElse(CostSource.UNCLASSIFIED);
                        if (finalCostsByCategoryAndSource.get(category, costSource) != null) {
                            TraxNumber existing = finalCostsByCategoryAndSource.get(category, costSource);
                            cost = cost.plus(existing).compute("Total cost for {}" + costSource);
                            finalCostsByCategoryAndSource.put(category, costSource, cost);
                        } else {
                            finalCostsByCategoryAndSource.put(category, costSource, cost);
                        }
                    }
                });
                logger.trace("Inheriting costs...");
                // Go through the child cost entities, and add their costs to the
                // costs calculated so far.
                childCostEntities.forEach(childCostEntity -> {
                    final CostJournal<ENTITY_CLASS> journal =
                        dependentCostLookup.getCostJournal(childCostEntity);
                    if (journal == null) {
                        logger.trace("Could not find costs for child entity {} (id: {}). It maybe" +
                                        "possible that the journal for the entity hasn't been created yet.",
                            infoExtractor.getName(childCostEntity),
                            infoExtractor.getId(childCostEntity));
                    } else {
                        logger.trace(() -> "Inheriting costs from " + infoExtractor.getName(childCostEntity)
                            + " id: " + infoExtractor.getId(childCostEntity));
                        // Make sure the child journal's prices have already been calculated.
                        // Hope there are no circular dependencies, fingers crossed!
                        journal.calculateCosts();
                        for (CostCategory category: journal.getCategories()) {
                            Map<CostSource, TraxNumber> costByCategoryMap = finalCostsByCategoryAndSource.row(category);
                            final Map<CostSource, TraxNumber> inheritedCostMap = journal.getHourlyCostForCategoryBySource(category);
                            for (CostSource costSource: inheritedCostMap.keySet()) {
                                final TraxNumber inheritedCost = inheritedCostMap.get(costSource);
                                TraxNumber currPriceNum = costByCategoryMap.getOrDefault(costSource, trax(0, "none"));
                                final TraxNumber newCost = inheritedCost.plus(currPriceNum)
                                        .compute("inherited hourly costs from " + infoExtractor.getName(childCostEntity));
                                logger.trace("Inheriting {} for category {}. New cost is: {}",
                                        inheritedCost, category, newCost);
                                finalCostsByCategoryAndSource.put(category, costSource, newCost);
                            }
                        }
                    }
                });
                logger.trace("Finished inheriting cost.");
            }
        }
    }

    /**
     * Interface to filter by cost source.
     */
    @FunctionalInterface
    public interface CostSourceFilter {

        /**
         * filter by cost source.
         *
         * @param costSource The cost source.
         *
         * @return true if cost source is of the same type.
         */
        boolean filter(@Nonnull CostSource costSource);

        /**
         * A cost source filter that only filters {@link CostSource#BUY_RI_DISCOUNT}.
         */
        CostSourceFilter EXCLUDE_BUY_RI_DISCOUNT_FILTER =
                (costSource) -> costSource != CostSource.BUY_RI_DISCOUNT;
    }

    /**
     * Functional Interface to look up by cost category and cost source filter.
     */
    @FunctionalInterface
    public interface RateExtractor {
        /**
         * Look up costs via cost category and cost source filter.
         *
         * @param costCategory The cost category.
         * @param costSourceFilter The cost source filter.
         *
         * @return A trax number representing the cost.
         */
        TraxNumber lookupCostWithFilter(CostCategory costCategory, CostSourceFilter costSourceFilter);
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
                .setAmount(getTotalHourlyCost().getValue()));
        getCategories().forEach(category -> {
            for (CostSource source: CostSource.values()) {
                TraxNumber costBySource = getHourlyCostBySourceAndCategory(category, source);
                if (costBySource != null) {
                    costBuilder.addComponentCost(ComponentCost.newBuilder()
                            .setCategory(category)
                            .setCostSource(source)
                            .setAmount(CurrencyAmount.newBuilder()
                                    .setAmount(costBySource.getValue())));
                }
            }
        });
        return costBuilder.build();
    }

    /**
     * Get the aggregated hourly cost for a particular category.
     *
     * @param category The category.
     * @return The hourly cost for the category.
     */
    @Nonnull
    public TraxNumber getHourlyCostForCategory(@Nonnull final CostCategory category) {
        calculateCosts();
        Map<CostSource, TraxNumber> costsByCategory = finalCostsByCategoryAndSource.row(category);
        return costsByCategory.values().stream().collect(TraxCollectors.sum("total hourly cost for category: " + category));
    }

    /**
     * Get the aggregated hourly cost for all categories except the excluded categories.
     *
     * @param excludeCategories The categories to exclude.
     * @return The hourly cost for all categories except for the categories to exclude.
     */
    @Nonnull
    public TraxNumber getTotalHourlyCostExcluding(@Nonnull final Set<CostCategory> excludeCategories) {
        calculateCosts();
        return finalCostsByCategoryAndSource.cellSet().stream().filter(e -> !excludeCategories.contains(e.getRowKey()))
                .map(e -> e.getValue()).collect(TraxCollectors.sum("total hourly costs with exclusions"));
    }

    @Nonnull
    private Map<CostSource, TraxNumber> getHourlyCostForCategoryBySource(CostCategory category) {
        calculateCosts();
        return Collections.unmodifiableMap(finalCostsByCategoryAndSource.row(category));
    }

    /**
     * Get the categories that this journal has prices for.
     *
     * @return The set of categories.
     */
    @Nonnull
    public Set<CostCategory> getCategories() {
        calculateCosts();
        return Collections.unmodifiableSet(finalCostsByCategoryAndSource.rowKeySet());
    }

    /**
     * Get the total (hourly) cost for the entity.
     *
     * @return The total (hourly) cost for the entity.
     */
    public TraxNumber getTotalHourlyCost() {
        calculateCosts();
        return finalCostsByCategoryAndSource.values().stream().collect(TraxCollectors.sum("total hourly cost"));
    }

    /**
     * Get the cost from a given journal entry for a given cost category.
     *
     * @param costSourceFilter The journal entry.
     * @param costCategory The cost category.
     *
     * @return The TraxNumber representing the cost from a journal for a particular category.
     */
    public TraxNumber getHourlyCostFilterEntries(CostCategory costCategory,
                                                 CostSourceFilter costSourceFilter) {
        calculateCosts();
        Map<CostSource, TraxNumber> costBySource = finalCostsByCategoryAndSource.row(costCategory);
        return costBySource.keySet().stream().filter(costSourceFilter::filter).map(costBySource::get)
                .collect(TraxCollectors.sum("Total sum excluding filter"));
    }

    /**
     * Given a cost category and cost source, get the final hourly cost.
     *
     * @param category The cost category.
     * @param source The cost source.
     *
     * @return A trax number representing the final cost for a given category and source.
     */
    public TraxNumber getHourlyCostBySourceAndCategory(CostCategory category, CostSource source) {
        calculateCosts();
        return finalCostsByCategoryAndSource.get(category, source);
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
        synchronized (finalCostsByCategoryAndSource) {
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
                            dependencyJournal.finalCostsByCategoryAndSource.cellSet().forEach((category) -> descBuilder.append("    ")
                                    .append(category.getRowKey()).append(" : ").append(category.getValue())
                                    .append(category.getColumnKey()).append("\n"));
                        }
                        return descBuilder.toString();
                    })
                    .collect(Collectors.joining("\n")))
                .add("finalCosts", finalCostsByCategoryAndSource.cellSet().stream()
                    .map(categoryEntry -> categoryEntry.getRowKey() + " : " + categoryEntry.getColumnKey() + ":" + categoryEntry.getValue())
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
     * Builder for the {@link CostJournal}.
     *
     * The intention is to have the cost journal be immutable once constructed, but in reality it
     * may be better to have the cost journal be mutable (append-only), and track operations on it.
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

        private final Map<CostCategory, SortedSet<QualifiedJournalEntry<ENTITY_CLASS_>>> costEntries =
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
                final TraxNumber amount) {
            if (logger.isTraceEnabled()) {
                logger.trace("On-demand {} purchase of {} from payee {} (id: {}) at price {}",
                        category, amount, infoExtractor.getName(payee), infoExtractor.getId(payee), price);
            }
            Preconditions.checkArgument(category != CostCategory.RI_COMPUTE);
            final Set<QualifiedJournalEntry<ENTITY_CLASS_>> prices = costEntries.computeIfAbsent(category, k -> new TreeSet<>());
            prices.add(new OnDemandJournalEntry<>(payee, price, amount, category, Optional.of(CostSource.ON_DEMAND_RATE)));
            return this;
        }

        /**
         * Record the RI discounted cost.
         *
         * @param category The cost category.
         * @param riData The RI data containing the RI related info.
         * @param riBoughtPercentage The percentage of the RI covering the VM.
         *
         * @return Builder containing the RI Discount journal entry.
         */
        @Nonnull
        public Builder<ENTITY_CLASS_> recordRIDiscount(
                @Nonnull final CostCategory category,
                @Nonnull final ReservedInstanceData riData,
                @Nonnull final TraxNumber riBoughtPercentage) {
            final Set<QualifiedJournalEntry<ENTITY_CLASS_>> prices = costEntries.computeIfAbsent(category, k -> new TreeSet<>());
            prices.add(new RIDiscountJournalEntry<>(riData, riBoughtPercentage, category, CostSource.RI_INVENTORY_DISCOUNT, false));
            return this;
        }

        /**
         * Record the RI buy discount.
         *
         * @param category The cost category.
         * @param riData An {@link ReservedInstanceData} instance, used to access the
         * {@link ReservedInstanceBought} providing the discount.
         * @param riBoughtPercentage The percentage of the RI covering the VM.
         *
         * @return Builder containing the RI Discount journal entry.
         */
        @Nonnull
        public Builder<ENTITY_CLASS_> recordBuyRIDiscount(
                @Nonnull final CostCategory category,
                @Nonnull final ReservedInstanceData riData,
                @Nonnull final TraxNumber riBoughtPercentage) {
            final Set<QualifiedJournalEntry<ENTITY_CLASS_>> prices = costEntries.computeIfAbsent(category, k -> new TreeSet<>());
            prices.add(new RIDiscountJournalEntry<>(
                    riData,
                    riBoughtPercentage,
                    category,
                    CostSource.BUY_RI_DISCOUNT,
                    true));
            return this;
        }

        /**
         * Record the license cost associated with a VM covered(partially or fully) by a Reserved Instance.
         *
         * @param category The cost category.
         * @param riData The reserved Instance info
         * @param riBoughtPercentage The percentage of the VM covered by an RI.
         * @param price The cost of the reserved instance license.
         * @param isBuyRI If the source is Buy RI, then true.
         *
         * @return Builder containing the Reserved Instance License journal entry.
         */
        @Nonnull
        public Builder<ENTITY_CLASS_> recordReservedLicenseCost(@Nonnull final CostCategory category,
                                                                @Nonnull final ReservedInstanceData riData,
                                                                @Nonnull final TraxNumber riBoughtPercentage,
                                                                @Nonnull final Price price,
                                                                @Nonnull final Boolean isBuyRI) {
            final Set<QualifiedJournalEntry<ENTITY_CLASS_>> prices = costEntries.computeIfAbsent(category, k -> new TreeSet<>());
            prices.add(new ReservedLicenseJournalEntry<>(price, riData, riBoughtPercentage, category,
                    isBuyRI ? Optional.of(CostSource.BUY_RI_DISCOUNT) : Optional.of(CostSource.RI_INVENTORY_DISCOUNT )));
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
         * @param couponsCovered The number of coupons covered by the RI.
         * @param hourlyCost The hourly cost for using the RI.
         * @return The builder, for method chaining.
         */
        @Nonnull
        public Builder<ENTITY_CLASS_> recordRiCost(
                @Nonnull final ReservedInstanceData payee,
                @Nonnull final TraxNumber couponsCovered,
                @Nonnull final TraxNumber hourlyCost) {
            if (logger.isTraceEnabled()) {
                logger.trace("RI {} coverage by {} at cost {}",
                        payee.getReservedInstanceBought().getId(), couponsCovered, hourlyCost);
            }
            final Set<QualifiedJournalEntry<ENTITY_CLASS_>> prices =
                    costEntries.computeIfAbsent(CostCategory.RI_COMPUTE, k -> new TreeSet<>());
            prices.add(new RIJournalEntry<>(payee, couponsCovered,  hourlyCost, CostCategory.RI_COMPUTE, null));
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
     * A helper class to tabulate a list of {@link QualifiedJournalEntry}s in an ASCII table.
     * The logic of tabulation is closely tied to the entry, but we split it off for readability.
     *
     * @param <ENTITY_CLASS> See {@link QualifiedJournalEntry}.
     */
    private abstract static class JournalEntryTabulator<ENTITY_CLASS> {

        private Class<? extends QualifiedJournalEntry> entryClass;

        private JournalEntryTabulator(Class<? extends QualifiedJournalEntry> entryClass) {
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
                               @Nonnull final Map<CostCategory, SortedSet<QualifiedJournalEntry<ENTITY_CLASS>>> entries) {
            final List<ColumnInfo> columnInfos = columnInfo();
            final CWC_LongestLine columnWidthCalculator = new CWC_LongestLine();
            columnInfos.forEach(colInfo -> columnWidthCalculator.add(colInfo.minWidth, colInfo.maxWidth));

            final AsciiTable table = new AsciiTable();
            table.getContext().setGrid(A7_Grids.minusBarPlusEquals());
            table.getRenderer().setCWC(columnWidthCalculator);
            table.addRule();
            table.addRow(Collections2.transform(columnInfos, colInfo -> colInfo.heading));
            boolean empty = true;
            for (Entry<CostCategory, SortedSet<QualifiedJournalEntry<ENTITY_CLASS>>> categoryEntry : entries.entrySet()) {
                final List<QualifiedJournalEntry<ENTITY_CLASS>> typeSpecificEntriesForCategory =
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
        abstract void addToTable(@Nonnull final QualifiedJournalEntry<ENTITY_CLASS> entry,
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
     * @param <ENTITY_CLASS> See {@link QualifiedJournalEntry}.
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
                @Nonnull final QualifiedJournalEntry<ENTITY_CLASS> entry,
                @Nonnull final AsciiTable asciiTable,
                @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor) {
            // We only care about on-demand journal entries, because this tabulator only
            // tabulates the on-demand entries.
            // This is an extra safeguard - the JournalEntryTabulator should already filter out
            // other types of entries.
            if (entry instanceof OnDemandJournalEntry) {
                OnDemandJournalEntry<ENTITY_CLASS> onDemandEntry = (OnDemandJournalEntry<ENTITY_CLASS>)entry;
                final int entityTypeInt = infoExtractor.getEntityType(onDemandEntry.getPayee());
                final EntityType entityType = EntityType.forNumber(entityTypeInt);

                asciiTable.addRow(
                    infoExtractor.getName(onDemandEntry.getPayee()),
                    infoExtractor.getId(onDemandEntry.getPayee()),
                    entityType == null ? Integer.toString(entityTypeInt) : entityType.name(),
                    onDemandEntry.getPrice().getUnit().name(),
                    onDemandEntry.getPrice().getEndRangeInUnits() == 0 ? "Inf" : Long.toString(onDemandEntry.getPrice().getEndRangeInUnits()),
                    Double.toString(onDemandEntry.getPrice().getPriceAmount().getAmount()),
                    Double.toString(onDemandEntry.getUnitsBought().getValue()));
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
     * @param <ENTITY_CLASS> See {@link QualifiedJournalEntry}.
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
        void addToTable(@Nonnull final QualifiedJournalEntry<ENTITY_CLASS> entry,
                        @Nonnull final AsciiTable asciiTable,
                        @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor) {
            if (entry instanceof RIJournalEntry) {
                RIJournalEntry<ENTITY_CLASS> riEntry = (RIJournalEntry<ENTITY_CLASS>) entry;
                asciiTable.addRow(
                        // It would be nice to get the name of the tier here, if we can do it without
                        // too much overhead.
                        riEntry.getRiData().getReservedInstanceSpec().getReservedInstanceSpecInfo().getTierId(),
                        riEntry.getRiData().getReservedInstanceBought().getId(),
                        riEntry.getRiData().getReservedInstanceSpec().getId(),
                        riEntry.getCouponsCovered(),
                        riEntry.getHourlyCost().getValue());
            }

        }

        @Override
        @Nonnull
        List<ColumnInfo> columnInfo() {
            return COLUMN_INFOS;
        }
    }
}
