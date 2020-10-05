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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal.CoverageJournalEntry;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.filter.FirstPassCoverageFilter;
import com.vmturbo.reserved.instance.coverage.allocator.metrics.RICoverageAllocationMetricsCollector;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageGroup;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageKeyRepository;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRule;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRulesFactory;
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
 * {@link com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey} instances and a
 * {@link CoverageKeyRepository}, creating a map
 * of coverage key -> {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought} instances
 * and a separate map of coverage key -> {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}
 * instances. Only entities and RIs with overlapping coverage keys are considered potential coverage
 * assignments for the particular rule.
 */
public class ReservedInstanceCoverageAllocator {

    private static final Logger logger = LogManager.getLogger();

    private final CoverageRulesFactory coverageRulesFactory;

    private final CoverageTopology coverageTopology;

    private final RICoverageAllocationMetricsCollector metricsCollector;

    private final boolean validateCoverages;

    private final ReservedInstanceCoverageJournal coverageJournal;

    private final FirstPassCoverageFilter firstPassFilter;

    private final CloudCommitmentPreference cloudCommitmentPreference;

    private final CoverageEntityPreference coverageEntityPreference;

    private final ExecutorService executorService;

    private final AtomicBoolean coverageAllocated = new AtomicBoolean(false);


    /**
     * Constructs an instance of {@link ReservedInstanceCoverageAllocator}, based on the configuration
     * passed in.
     * @param config The {@link CoverageAllocationConfig} instance, used to configure a newly created
     *               {@link ReservedInstanceCoverageAllocator} instance.
     */
    public ReservedInstanceCoverageAllocator(@Nonnull CoverageRulesFactory coverageRulesFactory,
                                             @Nonnull CoverageAllocationConfig config) {

        Preconditions.checkNotNull(config);

        this.coverageRulesFactory = Objects.requireNonNull(coverageRulesFactory);
        this.coverageTopology = config.coverageTopology();
        this.metricsCollector = new RICoverageAllocationMetricsCollector(config.metricsProvider());
        this.cloudCommitmentPreference = config.cloudCommitmentPreference();
        this.coverageEntityPreference = config.coverageEntityPreference();
        this.validateCoverages = config.validateCoverages();

        final ReservedInstanceCoverageProvider coverageProvider =
                Objects.requireNonNull(config.coverageProvider());
        this.coverageJournal = ReservedInstanceCoverageJournal.createJournal(
                coverageProvider.getAllCoverages(),
                coverageTopology);
        this.firstPassFilter = new FirstPassCoverageFilter(coverageTopology,
                config.accountFilter(),
                config.entityFilter(),
                coverageJournal);

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("ReservedInstanceCoverageAllocator-%d")
                .build();
        this.executorService = config.concurrentProcessing() ?
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
     * This method provides the root of allocation thread branching, in which we initially branch out
     * on a per {@link CloudProviderCoverageContext} basis, given allocations across CSPs can be allocated
     * concurrently. This method will serially process {@link CloudProviderCoverageContext} instances,
     * if the {@link ExecutorService} of this allocator is single threaded.
     */
    private void allocateCoverageInternal() {

        metricsCollector.onCoverageAnalysis().observe(() -> {
            Set<CloudProviderCoverageContext> cloudProviderCoverageContexts =
                    metricsCollector.onContextCreation().observe(() ->
                            CloudProviderCoverageContext.createContexts(
                                    coverageTopology,
                                    metricsCollector.onFirstPassRIFilter()
                                            .observe(() -> firstPassFilter.getReservedInstances()),
                                    metricsCollector.onFirstPassEntityFilter()
                                            .observe(() -> firstPassFilter.getCoverageEntities()),
                                    true));


            cloudProviderCoverageContexts.stream()
                    .map(coverageContext ->
                            executorService.submit(() -> processCoverageContext(coverageContext)))
                    .collect(Collectors.toSet())
                    .forEach(this::waitForFuture);
        });
    }

    private void processCoverageContext(@Nonnull CloudProviderCoverageContext coverageContext) {

        metricsCollector.onCoverageAnalysisForCSP(coverageContext, coverageJournal)
                .observe(() -> {
                    final List<CoverageRule> coverageRules =
                            coverageRulesFactory.createRules(
                                    coverageContext,
                                    coverageJournal);

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
                });
    }

    /**
     * Processes an individual {@link CoverageRule}, which represents potential RI <-> entity
     * assignments through an ordered set of both RI and entity OIDs. The potential assignments represent
     * valid coverage assignments based on criteria of both the RIs and entities, but do no indicate there
     * is enough coverage available to cover all entities or enough entities available to utilize all RIs.
     *
     * <p>
     * Processing will iterate over the sorted list of RIs, allocating available coverage until either all
     * RIs or entities are consumed
     *
     * @param coverageGroup The {@link CoverageGroup} to process
     */
    private void processGroup(@Nonnull CoverageGroup coverageGroup) {

        Queue<Long> entityQueue = Queues.newArrayDeque(
                coverageEntityPreference.sortEntities(
                        coverageJournal,
                        coverageGroup.entityOids()));

        cloudCommitmentPreference.sortCommitments(coverageJournal,coverageGroup.commitmentOids())
                .stream()
                // Iterate through commitments, until the enityQueue is empty (findFirst() will short-circuit
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
                                    coverageGroup.sourceTag(),
                                    riOid,
                                    entityOid,
                                    requestedCoverage,
                                    availableCoverage,
                                    allocatedCoverage);
                            coverageJournal.addCoverageEntry(coverageEntry);
                            metricsCollector.onCoverageAssignment(coverageEntry);

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


}
