package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal.CoverageJournalEntry;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.filter.FirstPassCoverageFilter;
import com.vmturbo.reserved.instance.coverage.allocator.key.HashableCoverageKeyCreator;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ReservedInstanceCoverageGroup;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ReservedInstanceCoverageRule;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ReservedInstanceCoverageRulesFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * Allocates {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought} coverage to
 * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO} instances, based on the
 * current coverage & utilization and provider-specific coverage rules.
 *
 * <p>
 * A first-pass rules is applied ({@link FirstPassCoverageFilter}), which fills in any partial allocation
 * between an entity and RI. This rule is mean to fill coverage assignments from the provider-specific
 * source (e.g. the AWS bill) and is based on iteration over current assignments from
 * {@link ReservedInstanceCoverageProvider}.
 *
 * <p>
 * Subsequent (provider-specific) rules determine valid coverage assignments through the use of
 * {@link com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKey} instances and a
 * {@link com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyRepository}, creating a map
 * of coverage key -> {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought} instances
 * and a separate map of coverage key -> {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}
 * instances. Only entities and RIs with overlapping coverage keys are considered potential coverage
 * assignments for the particular rule.
 */
public class ReservedInstanceCoverageAllocator {

    private static final Logger logger = LogManager.getLogger();

    private final CoverageTopology coverageTopology;

    private final boolean validateCoverages;

    private final ReservedInstanceCoverageJournal coverageJournal;

    private final FirstPassCoverageFilter firstPassFilter;

    private final ExecutorService executorService;

    private final AtomicBoolean coverageAllocated = new AtomicBoolean(false);


    private ReservedInstanceCoverageAllocator(Builder builder) {


        this.coverageTopology = Objects.requireNonNull(builder.coverageTopology);
        this.validateCoverages = builder.validateCoverages;

        final ReservedInstanceCoverageProvider coverageProvider =
                Objects.requireNonNull(builder.coverageProvider);
        this.coverageJournal = ReservedInstanceCoverageJournal.createJournal(
                coverageProvider.getAllCoverages(),
                coverageTopology);
        this.firstPassFilter = new FirstPassCoverageFilter(coverageTopology,
                builder.accountFilter,
                builder.entityFilter,
                coverageJournal);

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("ReservedInstanceCoverageAllocator-%d")
                .build();
        this.executorService = builder.concurrentProcessing ?
                Executors.newCachedThreadPool(threadFactory) :
                MoreExecutors.newDirectExecutorService();

    }

    /**
     * Allocates RI coverage to entities, based on provider-specific coverage allocation rules and
     * the current coverage & utilization of the input topology.
     *
     * @return An immutable instance of {@link ReservedInstanceCoverageAllocation}, containing the
     * allocated coverage. If this method is invoked more than once on the same instance, all invocations
     * subsequent to the first will return the initially calculated allocation
     */
    public ReservedInstanceCoverageAllocation allocateCoverage() {

        if (coverageAllocated.compareAndSet(false, true)) {
            try {
                allocateCoverageInternal();
            } finally {
                executorService.shutdown();
            }

            if (validateCoverages) {
                coverageJournal.validateCoverages();
            }
        } else {
            // If this allocator has previously been invoked, wait for all allocation threads to
            // terminate before checking results. If a reasonable timeout is required, it is expected
            // the caller of the allocator will implement a timeout.
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted waiting for previously executed allocation", e);
                return ReservedInstanceCoverageAllocation.EMPTY_ALLOCATION;
            }
        }

        return ReservedInstanceCoverageAllocation.from(
                coverageJournal.getCoverages(),
                coverageJournal.getCoverageFromJournalEntries());
    }

    /**
     * Creates a new builder
     * @return A new {@link Builder} instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * This method provides the root of allocation thread branching, in which we initially branch out
     * on a per {@link CloudProviderCoverageContext} basis, given allocations across CSPs can be allocated
     * concurrently. This method will serially process {@link CloudProviderCoverageContext} instances,
     * if the {@link ExecutorService} of this allocator is single threaded.
     */
    private void allocateCoverageInternal() {
        Set<CloudProviderCoverageContext> cloudProviderCoverageContexts =
                CloudProviderCoverageContext.createContexts(
                        coverageTopology,
                        firstPassFilter.getReservedInstances(),
                        firstPassFilter.getCoverableEntities(),
                        true);


        cloudProviderCoverageContexts.stream()
                .map(coverageContext ->
                        executorService.submit(() -> processCoverageContext(coverageContext)))
                .collect(Collectors.toSet())
                .forEach(this::waitForFuture);
    }

    private void processCoverageContext(@Nonnull CloudProviderCoverageContext coverageContext) {

        final List<ReservedInstanceCoverageRule> coverageRules =
                ReservedInstanceCoverageRulesFactory.createRules(
                        coverageContext,
                        coverageJournal,
                        HashableCoverageKeyCreator.newFactory());

        coverageRules.forEach(rule -> {

            if (rule.createsDisjointGroups()) {
                // If a rule is disjoint, it indicates each RI and topology entity within the
                // coverageContext is included (at most) in one coverage group and is therefore safe
                // to process concurrently
                rule.coverageGroups()
                        .map(group -> executorService.submit(() -> processGroup(group)))
                        // first collect in a terminal stage to allow all submissions
                        // to the executor service to complete
                        .collect(Collectors.toSet())
                        .forEach(this::waitForFuture);
            } else {
                rule.coverageGroups().forEach(this::processGroup);
            }
        });
    }

    /**
     * Processes an individual {@link ReservedInstanceCoverageGroup}, which represents potential RI <-> entity
     * assignments through an ordered set of both RI and entity OIDs. The potential assignments represent
     * valid coverage assignments based on criteria of both the RIs and entities, but do no indicate there
     * is enough coverage available to cover all entities or enough entities available to utilize all RIs.
     *
     * <p>
     * Processing will iterate over the sorted list of RIs, allocating available coverage until either all
     * RIs or entities are consumed
     *
     * @param coverageGroup The {@link ReservedInstanceCoverageGroup} to process
     */
    private void processGroup(@Nonnull ReservedInstanceCoverageGroup coverageGroup) {

        Queue<Long> entityQueue = Queues.newArrayDeque(coverageGroup.entityOids());

        coverageGroup.reservedInstanceOids()
                .stream()
                // Iterate through RIs, until the enityQueue is empty (findFirst() will short-circuit
                // once the queue is empty)
                .filter(riOid -> {
                    while (!coverageJournal.isReservedInstanceAtCapacity(riOid) &&
                            !entityQueue.isEmpty()) {

                        final long entityOid = entityQueue.peek();
                        if (!coverageJournal.isEntityAtCapacity(entityOid)) {
                            final double requestedCoverage = coverageJournal.getUncoveredCapacity(entityOid);
                            final double availableCoverage = coverageJournal.getUnallocatedCapacity(riOid);
                            final double allocatedCoverage = Math.min(requestedCoverage, availableCoverage);

                            logger.debug("Adding coverage entry (RI OID={}, Entity OID={} CoveragKey={}, " +
                                    "Requested Coverage={}, Available Coverage={}, Allocated Coverage={})",
                                    riOid, entityOid, coverageGroup.sourceKey(), requestedCoverage,
                                    availableCoverage, allocatedCoverage);

                            final CoverageJournalEntry coverageEntry = CoverageJournalEntry.of(
                                    coverageGroup.cloudServiceProvider(),
                                    coverageGroup.sourceName(),
                                    riOid,
                                    entityOid,
                                    requestedCoverage,
                                    availableCoverage,
                                    allocatedCoverage);
                            coverageJournal.addCoverageEntry(coverageEntry);

                        } else {
                            entityQueue.poll();
                        }
                    }

                    return entityQueue.isEmpty();
                }).findFirst();
    }

    private <T> T waitForFuture(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new FailedCoverageAllocationException(e);
        }
    }

    /**
     * Builds an instance of {@link ReservedInstanceCoverageAllocator}
     */
    public static class Builder {


        @Nonnull
        private ReservedInstanceCoverageProvider coverageProvider;

        @Nullable
        private AccountFilter accountFilter;

        @Nullable
        private EntityFilter entityFilter;

        @Nonnull
        private CoverageTopology coverageTopology;

        private boolean validateCoverages = false;

        private boolean concurrentProcessing = true;

        /**
         * Set the {@link ReservedInstanceCoverageProvider}, which is used for initial coverage/utilization
         * of the topology.
         *
         * <p>
         * Note: The {@code coverageProvider} is required
         *
         * @param coverageProvider An instance of {@link ReservedInstanceCoverageProvider}
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder coverageProvider(@Nonnull ReservedInstanceCoverageProvider coverageProvider) {
            this.coverageProvider = Objects.requireNonNull(coverageProvider);
            return this;
        }

        /**
         * Set the {@link CoverageTopology}, which contains the valid topology entities (potentially
         * additionally filtered) and {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought}
         * instnaces in scope for coverage allocation
         *
         * <p>
         * Note: The {@code coverageTopology} is required
         *
         * @param coverageTopology An {@link CoverageTopology} instance
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder coverageTopology(@Nonnull CoverageTopology coverageTopology) {
            this.coverageTopology = Objects.requireNonNull(coverageTopology);
            return this;
        }

        /**
         * Set an (optional) account filter for filtering coverable entities from the {@link CoverageTopology}
         * @param accountFilter An instance of {@link AccountFilter}
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder accountFilter(@Nullable AccountFilter accountFilter) {
            this.accountFilter = accountFilter;
            return this;
        }

        /**
         * Set an (optional) entity filter for filtering coverable entities from the {@link CoverageTopology}
         * @param entityFilter An instance of {@link EntityFilter}
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder entityFilter(@Nullable EntityFilter entityFilter) {
            this.entityFilter = entityFilter;
            return this;
        }

        /**
         * Set whether to validate coverage assignments after allocation. Validation will entail checking
         * whether any entities are covered over their capacity or any RIs are allocated above their
         * capacity
         *
         * @param validateCoverages A boolean indicating whether coverage validation is enabled
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder validateCoverages(boolean validateCoverages) {
            this.validateCoverages = validateCoverages;
            return this;
        }

        /**
         * Set whether concurrent processing is enabled. If true, where possible (e.g. across cloud providers,
         * within a rule with disjoint groups), the allocator will concurrently process units of work.
         *
         * @param concurrentProcessing A boolean indicating whether concurrent processing is enabled
         * @return The instance of {@link Builder} for method chaining
         */
        @Nonnull
        public Builder concurrentProcessing(boolean concurrentProcessing) {
            this.concurrentProcessing = concurrentProcessing;
            return this;
        }

        /**
         * @return A newly created instance of {@link ReservedInstanceCoverageAllocator}
         */
        @Nonnull
        public ReservedInstanceCoverageAllocator build() {
            return new ReservedInstanceCoverageAllocator(this);
        }
    }
}
