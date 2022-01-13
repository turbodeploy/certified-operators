package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.base.VerifyException;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.common.math.DoubleMath;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.cloud.common.commitment.CommitmentAmountUtils;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.components.common.utils.FuzzyDoubleUtils;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;

/**
 * Records coverage entries (Entity, RI, Coverage Amount) for adding coverage to an existing
 * baseline.
 */
public class CloudCommitmentCoverageJournal {

    private static final double TOLERANCE = .0000001;

    private final ReadWriteLock coveragesLock = new ReentrantReadWriteLock();

    @GuardedBy("coveragesLock")
    // <EntityOid, CloudCommitmentOid, AllocatedCoverage>
    private final Table<Long, Long, CloudCommitmentAmount> coverages;

    @GuardedBy("coveragesLock")
    private final List<CoverageJournalEntry> journalEntries = new ArrayList<>();

    private final Table<Long, CloudCommitmentCoverageTypeInfo, Double> entityCapacityTable =
            Tables.synchronizedTable(HashBasedTable.create());

    private final Table<Long, CloudCommitmentCoverageTypeInfo, Double> commitmentCapacityTable =
            Tables.synchronizedTable(HashBasedTable.create());

    private final CoverageTopology coverageTopology;

    private CloudCommitmentCoverageJournal(@Nonnull Table<Long, Long, CloudCommitmentAmount> coverages,
                                           @Nonnull CoverageTopology coverageTopology) {

        this.coverages = HashBasedTable.create(Objects.requireNonNull(coverages));
        this.coverageTopology = Objects.requireNonNull(coverageTopology);
    }

    /**
     * Create a new coverage journal.
     *
     * @param coverages The baseline coverage as input to this journal. The format must follow
     * {@literal <Entity OID, RI OID, Coverage Amount>}
     * @param coverageTopology The coverage topolggy, used to resolve entity coverage capacity
     * @return A newly created coverage journal
     */
    public static CloudCommitmentCoverageJournal createJournal(
            @Nonnull Table<Long, Long, CloudCommitmentAmount> coverages,
            @Nonnull CoverageTopology coverageTopology) {
        return new CloudCommitmentCoverageJournal(coverages, coverageTopology);
    }

    /**
     * Get the covered amount for {@code entityOid} of the specified {@code coverageType}.
     * @param entityOid The entity OID.
     * @param coverageType The coverage type info.
     * @return The covered {@link CloudCommitmentAmount}.
     */
    @Nonnull
    public double getCoveredAmount(long entityOid, CloudCommitmentCoverageTypeInfo coverageType) {

        coveragesLock.readLock().lock();
        try {
            return CommitmentAmountUtils.sumCoverageType(
                            coverages.row(entityOid).values(),
                            coverageType);
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Gets the uncovered capacity of an entity.
     *
     * @param entityOid The OID of the entity
     * @param coverageType The coverage type info.
     * @return The uncovered capacity (full capacity - covered capacity) of the entity
     */
    public double getUncoveredCapacity(long entityOid,
                                       @Nonnull CloudCommitmentCoverageTypeInfo coverageType) {

        final double capacityAmount = getEntityCapacity(entityOid, coverageType);

        if (FuzzyDoubleUtils.isPositive(capacityAmount, TOLERANCE)) {
            coveragesLock.readLock().lock();
            try {
                final double coveredAmount = getCoveredAmount(entityOid, coverageType);
                return capacityAmount - coveredAmount;

            } finally {
                coveragesLock.readLock().unlock();
            }
        } else {
            return 0.0;
        }
    }

    /**
     * Checks whether the entity is at capacity for the specified coverage type.
     * @param entityOid The OID of the entity.
     * @param coverageType The coverage type info.
     * @return True, if this entity is at or above capacity. If this entity has no configured
     * capacity, it will be treated as at capacity.
     */
    public boolean isEntityAtCapacity(long entityOid, @Nonnull CloudCommitmentCoverageTypeInfo coverageType) {
        coveragesLock.readLock().lock();
        try {
            return !FuzzyDoubleUtils.isPositive(getUncoveredCapacity(entityOid, coverageType), TOLERANCE);
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Gets the allocated coverage amount of a commitment.
     * @param commitmentOid The OID of the cloud commitment.
     * @param coverageType The coverage type info.
     * @return The allocated coverage amount of the commitment.
     */
    @Nonnull
    public double getAllocatedCoverageAmount(long commitmentOid,
                                             @Nonnull CloudCommitmentCoverageTypeInfo coverageType) {
        coveragesLock.readLock().lock();
        try {
            return CommitmentAmountUtils.sumCoverageType(
                            coverages.column(commitmentOid).values(),
                            coverageType);
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Gets the unallocated capacity of a commitment aggregate for a specified coverage type.
     * @param commitmentOid The OID of the commitment.
     * @param coverageType The coverage type info.
     * @return The unallocated capacity of the commitment.
     */
    @Nonnull
    public double getUnallocatedCapacity(long commitmentOid, @Nonnull CloudCommitmentCoverageTypeInfo coverageType) {

        final double capacity = getCommitmentCapacity(commitmentOid, coverageType);

        if (FuzzyDoubleUtils.isPositive(capacity, TOLERANCE)) {
            coveragesLock.readLock().lock();
            try {
                final double allocatedAmount = getAllocatedCoverageAmount(commitmentOid, coverageType);
                return capacity - allocatedAmount;

            } finally {
                coveragesLock.readLock().unlock();
            }
        } else {
            return capacity;
        }
    }

    /**
     * Checks whether {@code commitmentOid} is at capacity for all supported {@link CloudCommitmentCoverageTypeInfo}
     * instances for the commitment aggregate.
     * @param commitmentOid The commitment aggregate OID.
     * @return True, if the commitment does not exist or is at capacity for all coverage types. False, otherwise.
     */
    public boolean isCommitmentAtCapacity(long commitmentOid) {

        return coverageTopology.getCloudCommitment(commitmentOid)
                .map(commitmentAggregate -> commitmentAggregate.coverageTypeInfoSet()
                        .stream()
                        .allMatch(coverageType -> isCommitmentAtCapacity(commitmentOid, coverageType)))
                .orElse(true);
    }

    /**
     * Checks whether the commitment is at capacity for the specified coverage type.
     * @param commitmentOid The OID of the target RI
     * @param coverageType The coverage type info.
     * @return True, if this RI is at or above capacity. False, otherwise. RIs with unknown capacity
     * will be treated as at capacity.
     */
    public boolean isCommitmentAtCapacity(long commitmentOid,
                                          @Nonnull CloudCommitmentCoverageTypeInfo coverageType) {
        coveragesLock.readLock().lock();
        try {
            return !FuzzyDoubleUtils.isPositive(getUnallocatedCapacity(commitmentOid, coverageType), TOLERANCE);
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Retries the commitment capacity of the specified coverage type for the specified entity.
     * @param entityOid The OID of the target entity
     * @param coverageTypeInfo The coverage type info.
     * @return The coverage capacity of the entity. If the capacity or entity are unknown, an empty
     * {@link CloudCommitmentAmount} will be returned.
     */
    public double getEntityCapacity(long entityOid,
                                    @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return entityCapacityTable.row(entityOid)
                .computeIfAbsent(coverageTypeInfo, (t) -> coverageTopology.getCoverageCapacityForEntity(entityOid, coverageTypeInfo));
    }

    /**
     * Gets the commitment capacity for {@code commitmentOid} of the specified coverage type.
     * @param commitmentOid The commitment aggregate OID.
     * @param coverageType The coverage type info.
     * @return The capacity of the commitment. This will be an empty {@link CloudCommitmentAmount} if
     * the commitment OID is not recognized or the commitment does not support the specified coverage
     * type.
     */
    public double getCommitmentCapacity(long commitmentOid,
                                        @Nonnull CloudCommitmentCoverageTypeInfo coverageType) {
        return commitmentCapacityTable.row(commitmentOid)
                .computeIfAbsent(coverageType, (t) -> coverageTopology.getCommitmentCapacity(commitmentOid, coverageType));
    }

    /**
     * Add a coverage entry to this journal.
     * @param entry The journal entry to add
     */
    public void addCoverageEntry(@Nonnull CoverageJournalEntry entry) {
        Preconditions.checkNotNull(entry);

        coveragesLock.writeLock().lock();
        try {
            journalEntries.add(entry);
            addCoverage(entry.cloudCommitmentOid(),
                    entry.entityOid(),
                    entry.coverageType(),
                    entry.allocatedCoverage());

        } finally {
            coveragesLock.writeLock().unlock();
        }

    }

    /**
     * Validates the total coverage (initial + journal entries) of this journal. The validation checks
     * that each RI and each entity is at or below its capacity. If any entity or RI is above capacity,
     * a {@link VerifyException} is thrown
     *
     * @throws VerifyException If any entity or RI is above capacity
     */
    public void validateCoverages() {
        coveragesLock.readLock().lock();
        try {

            // Right now, we ony validate that an entity is not above the capacity of any single coverage
            // type. For something like AWS, in which a VM could be covered by both an RI and SP, we only
            // check the individual capacity, not that the VM's aggregate coverage is greater than 100%
            coverages.rowKeySet().stream().forEach(entityOid -> {

                final Map<CloudCommitmentCoverageTypeInfo, Double> coverageByType =
                        CommitmentAmountUtils.groupAndSum(coverages.row(entityOid).values());

                coverageByType.forEach((coverageType, allocatedAmount) -> {

                    final double coverageCapacity = getEntityCapacity(entityOid, coverageType);

                    Verify.verify(DoubleMath.fuzzyCompare(allocatedAmount, coverageCapacity, TOLERANCE) <= 0,
                            "Allocated coverage is greater than capacity (EntityOid=%s, Allocated=%s, Capacity=%s)",
                            entityOid, allocatedAmount, coverageCapacity);

                });


            });

            coverages.columnKeySet().stream().forEach(commitmentOid -> {

                final Map<CloudCommitmentCoverageTypeInfo, Double> coverageByType =
                        CommitmentAmountUtils.groupAndSum(coverages.column(commitmentOid).values());

                coverageByType.forEach((coverageType, allocatedAmount) -> {
                    final double coverageCapacity = getCommitmentCapacity(commitmentOid, coverageType);

                    Verify.verify(DoubleMath.fuzzyCompare(allocatedAmount, coverageCapacity, TOLERANCE) <= 0,
                            "Allocated coverage is greater than capacity "
                                    + "(ReservedInstanceOid=%s, Allocated=%s, Capacity=%s)",
                            commitmentOid, allocatedAmount, coverageCapacity);
                });
            });
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Gets only the coverage added through {@link #addCoverageEntry(CoverageJournalEntry)}. This
     * coverage does not include the initial coverage.
     *
     * @return The coverage from journal entries. The format follows
     * {@literal <Entity OID, RI OID, Coverage Amount>}. The table returned is immutable
     */
    public Table<Long, Long, CloudCommitmentAmount> getCoverageFromJournalEntries() {
        coveragesLock.readLock().lock();
        try {
            return journalEntries.stream()
                    .collect(ImmutableTable.toImmutableTable(
                            CoverageJournalEntry::entityOid,
                            CoverageJournalEntry::cloudCommitmentOid,
                            entry -> CommitmentAmountUtils.convertToAmount(
                                    entry.coverageType(), entry.allocatedCoverage()),
                            CommitmentAmountCalculator::sum));
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Returns the total coverage of this journal. This includes both the initial coverage and
     * any coverage recorded through journal entries
     *
     * @return The total coverage following the format of {@literal <Entity OID, RI OID, Coverage Amount>}.
     * The table returned is immutable
     */
    public Table<Long, Long, CloudCommitmentAmount> getCoverages() {
        coveragesLock.readLock().lock();
        try {
            return Tables.unmodifiableTable(coverages);
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    private void addCoverage(long commitmentOid, long entityOid,
                             @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo,
                             @Nonnull double coverageAmount) {

        coveragesLock.writeLock().lock();
        try {

            final CloudCommitmentAmount newCoverageAmount =
                    CommitmentAmountUtils.convertToAmount(coverageTypeInfo, coverageAmount);
            if (coverages.contains(entityOid, commitmentOid)) {
                coverages.put(entityOid, commitmentOid,
                        CommitmentAmountCalculator.sum(
                                coverages.get(entityOid, commitmentOid),
                                newCoverageAmount));
            } else {
                coverages.put(entityOid, commitmentOid, newCoverageAmount);
            }
        } finally {
            coveragesLock.writeLock().unlock();
        }
    }

    /**
     * A journal entry containing allocated coverage. The journal entry contains additional information
     * for debugging coverage allocation
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface CoverageJournalEntry {

        /**
         * The cloud service provider info.
         * @return The cloud service provider of both the covered entity and cloud commitment
         * represented in this coverage entry
         */
        @Nonnull
        ServiceProviderInfo cloudServiceProvider();

        /**
         * An identifier of the source of the coverage entry.
         * @return An identifier of the source of the coverage.
         */
        @Nonnull
        String sourceName();

        /**
         * The coverage type info.
         * @return The coverage type info.
         */
        @Nonnull
        CloudCommitmentCoverageTypeInfo coverageType();

        /**
         * The OID of the cloud commitment.
         * @return The OID of the cloud commitment.
         */
        long cloudCommitmentOid();

        /**
         * The entity OID.
         * @return The OID of the entity.
         */
        long entityOid();

        /**
         * The request coverage of the entity.
         * @return The requested coverage of the entity (uncovered capacity at the point this
         * entry was made)
         */
        @Nonnull
        double requestedCoverage();

        /**
         * The available capacity of the commitment.
         * @return The available capacity of the commitment (unallocated capacity at the point this entry
         * was made)
         */
        @Nonnull
        double availableCoverage();

        /**
         * The allocated coverage from the commitment to the entity.
         * @return The allocated coverage from the commitment to the entity
         */
        @Nonnull
        double allocatedCoverage();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link CoverageJournalEntry} instances.
         */
        class Builder extends ImmutableCoverageJournalEntry.Builder {}
    }
}
