package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.base.VerifyException;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.math.DoubleMath;

import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * Records coverage entries (Entity, RI, Coverage Amount) for adding coverage to an existing
 * baseline.
 */
public class ReservedInstanceCoverageJournal {

    private static final double TOLERANCE = .0000001;

    @GuardedBy("coveragesLock")
    // <EntityOid, ReservedInstanceOid, AllocatedCoverage>
    private final Table<Long, Long, Double> coverages;

    private final ReadWriteLock coveragesLock = new ReentrantReadWriteLock();

    @GuardedBy("journalEntriesLock")
    private final List<CoverageJournalEntry> journalEntries = new ArrayList<>();

    private final ReadWriteLock journalEntriesLock = new ReentrantReadWriteLock();

    private final Map<Long, Double> entityCapacityByOid = new ConcurrentHashMap<>();

    private final CoverageTopology coverageTopology;

    private final Map<Long, Double> commitmentCapacityByOid;

    private ReservedInstanceCoverageJournal(@Nonnull Table<Long, Long, Double> coverages,
                                            @Nonnull CoverageTopology coverageTopology) {

        this.coverages = HashBasedTable.create(Objects.requireNonNull(coverages));
        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.commitmentCapacityByOid = coverageTopology.getCommitmentCapacityByOid();
    }

    /**
     * Create a new coverage journal
     *
     * @param coverages The baseline coverage as input to this journal. The format must follow
     * {@literal <Entity OID, RI OID, Coverage Amount>}
     * @param coverageTopology The coverage topolggy, used to resolve entity coverage capacity
     * @return A newly created coverage journal
     */
    public static ReservedInstanceCoverageJournal createJournal(
            @Nonnull Table<Long, Long, Double> coverages,
            @Nonnull CoverageTopology coverageTopology) {
        return new ReservedInstanceCoverageJournal(coverages, coverageTopology);
    }

    /**
     * Gets the uncovered capacity of an entity.
     *
     * @param entityOid The OID of the entity
     * @return The uncovered capacity (full capacity - covered capacity) of the entity
     */
    public double getUncoveredCapacity(long entityOid) {

        final double capacity = getEntityCapacity(entityOid);

        if (DoubleMath.fuzzyCompare(capacity, 0.0, TOLERANCE) > 0) {
            coveragesLock.readLock().lock();
            try {
                final double coveredCapacity = getCoveredCapacity(entityOid);
                return capacity - coveredCapacity;

            } finally {
                coveragesLock.readLock().unlock();
            }
        } else {
            return 0.0;
        }

    }

    /**
     * Gets the covered capacity of an entity.
     *
     * @param entityOid The OID of the entity
     * @return The covered capacity of the entity. This represents the initial capacity when this
     * journal was created plus any added coverage through {@link #addCoverageEntry(CoverageJournalEntry)}
     */
    public double getCoveredCapacity(long entityOid) {

        coveragesLock.readLock().lock();
        try {
            // returns an empty map, if entityOid is not contained within the coverages table
            return coverages.row(entityOid)
                    .values()
                    .stream()
                    .mapToDouble(Double::valueOf)
                    .sum();
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * @param entityOid The OID of the entity
     * @return True, if this entity is at or above capacity. If this entity has no configured
     * capacity, it will be treated as at capacity.
     */
    public boolean isEntityAtCapacity(long entityOid) {
        coveragesLock.readLock().lock();
        try {
            return DoubleMath.fuzzyCompare(getUncoveredCapacity(entityOid), 0.0, TOLERANCE) <= 0;
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Gets the allocated coverage amount of an RI
     * @param riOid The OID of the RI
     * @return The allocated coverage amount of the RI
     */
    public double getAllocatedCoverageAmount(long riOid) {
        coveragesLock.readLock().lock();
        try {
            return coverages.column(riOid)
                    .values()
                    .stream()
                    .mapToDouble(Double::valueOf)
                    .sum();
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * Gets the unallocated capacity of an RI
     * @param riOid The OID of the RI
     * @return The unallocated capacity of the RI
     */
    public double getUnallocatedCapacity(long riOid) {

        final double capacity = commitmentCapacityByOid.getOrDefault(riOid, 0.0);

        if (DoubleMath.fuzzyCompare(capacity, 0.0, TOLERANCE) > 0) {
            coveragesLock.readLock().lock();
            try {
                final double allocatedCapacity = getAllocatedCoverageAmount(riOid);
                return capacity - allocatedCapacity;

            } finally {
                coveragesLock.readLock().unlock();
            }
        } else {
            return 0.0;
        }
    }

    /**
     * @param riOid The OID of the target RI
     * @return True, if this RI is at or above capacity. False, otherwise. RIs with unknown capacity
     * will be treated as at capacity.
     */
    public boolean isReservedInstanceAtCapacity(long riOid) {
        coveragesLock.readLock().lock();
        try {
            return DoubleMath.fuzzyCompare(getUnallocatedCapacity(riOid), 0.0, TOLERANCE) <= 0;
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    /**
     * @param entityOid The OID of the target entity
     * @return The coverage capacity of the entity. If the capacity is unknown, it defaults to 0
     */
    public double getEntityCapacity(long entityOid) {
        return entityCapacityByOid.computeIfAbsent(entityOid,
                (oid) -> coverageTopology.getCoverageCapacityForEntity(entityOid));
    }

    /**
     * @param commitmentOid The OID of the target commitment
     * @return The coverage capacity of the commitment. If the capacity is unknown, it defaults to 0
     */
    public double getCloudCommitmentCapacity(long commitmentOid) {
        return commitmentCapacityByOid.getOrDefault(commitmentOid, 0.0);
    }

    /**
     * Add a coverage entry to this journal
     * @param entry The journal entry to add
     */
    public void addCoverageEntry(@Nonnull CoverageJournalEntry entry) {
        Preconditions.checkNotNull(entry);

        journalEntriesLock.writeLock().lock();
        try {
            journalEntries.add(entry);
            addCoverage(entry.reservedInstanceOid(),
                    entry.entityOid(),
                    entry.allocatedCoverage());

        } finally {
            journalEntriesLock.writeLock().unlock();
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
            coverages.rowKeySet().stream().forEach(entityOid -> {
                final double allocatedCoverage = getCoveredCapacity(entityOid);
                final double coverageCapacity = getEntityCapacity(entityOid);

                Verify.verify(DoubleMath.fuzzyCompare(allocatedCoverage, coverageCapacity, TOLERANCE) <= 0,
                        "Allocated coverage is greater than capacity (EntityOid=%s, Allocated=%s, Capacity=%s)",
                        entityOid, allocatedCoverage, coverageCapacity);
            });

            coverages.columnKeySet().stream().forEach(riOid -> {
                final double allocatedCoverage = getAllocatedCoverageAmount(riOid);
                final double coverageCapacity = commitmentCapacityByOid.getOrDefault(riOid, 0.0);

                Verify.verify(DoubleMath.fuzzyCompare(allocatedCoverage, coverageCapacity, TOLERANCE) <= 0,
                        "Allocated coverage is greater than capacity " +
                                "(ReservedInstanceOid=%s, Allocated=%s, Capacity=%s)",
                        riOid, allocatedCoverage, coverageCapacity);
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
    public Table<Long, Long, Double> getCoverageFromJournalEntries() {
        journalEntriesLock.readLock().lock();
        try {
            return journalEntries.stream()
                    .collect(ImmutableTable.toImmutableTable(
                            CoverageJournalEntry::entityOid,
                            CoverageJournalEntry::reservedInstanceOid,
                            CoverageJournalEntry::allocatedCoverage,
                            Double::sum));
        } finally {
            journalEntriesLock.readLock().unlock();
        }
    }

    /**
     * Returns the total coverage of this journal. This includes both the initial coverage and
     * any coverage recorded through journal entries
     *
     * @return The total coverage following the format of {@literal <Entity OID, RI OID, Coverage Amount>}.
     * The table returned is immutable
     */
    public Table<Long, Long, Double> getCoverages() {
        coveragesLock.readLock().lock();
        try {
            return ImmutableTable.copyOf(coverages);
        } finally {
            coveragesLock.readLock().unlock();
        }
    }

    private void addCoverage(long riOid, long entityOid, double coverageAmount) {

        coveragesLock.writeLock().lock();
        try {
            final double totalCoverageAmount =
                    MoreObjects.firstNonNull(coverages.get(entityOid, riOid), 0.0) +
                            coverageAmount;
            coverages.put(entityOid, riOid, totalCoverageAmount);
        } finally {
            coveragesLock.writeLock().unlock();
        }
    }

    /**
     * A journal entry containing allocated coverage. The journal entry contains additional information
     * for debugging coverage allocation
     */
    @Immutable
    public static class CoverageJournalEntry {

        private final CloudServiceProvider cloudServiceProvider;

        private final String sourceName;

        private final long reservedInstanceOid;

        private final long entityOid;

        private final double requestedCoverage;

        private final double availableCoverage;

        private final double allocatedCoverage;

        private CoverageJournalEntry(@Nonnull CloudServiceProvider cloudServiceProvider,
                                     @Nonnull String sourceName,
                                    long riOid,
                                    long entityOid,
                                    double requestedCoverage,
                                    double availableCoverage,
                                    double allocatedCoverage) {
            this.cloudServiceProvider = Objects.requireNonNull(cloudServiceProvider);
            this.sourceName = Objects.requireNonNull(sourceName);
            this.reservedInstanceOid = riOid;
            this.entityOid = entityOid;
            this.requestedCoverage = requestedCoverage;
            this.availableCoverage = availableCoverage;
            this.allocatedCoverage = allocatedCoverage;
        }

        /**
         * @return The cloud service provider of both the covered entity and reserved instance
         * represented in this coverage entry
         */
        @Nonnull
        public CloudServiceProvider cloudServiceProvider() {
            return cloudServiceProvider;
        }

        /**
         * @return An identifier of the source of the coverage
         */
        @Nonnull
        public String sourceName() {
            return sourceName;
        }

        /**
         * @return The OID of the RI
         */
        public long reservedInstanceOid() {
            return reservedInstanceOid;
        }

        /**
         * @return The OID of the entity
         */
        public long entityOid() {
            return entityOid;
        }

        /**
         * @return The requested coverage of the entity (uncovered capacity at the point this
         * entry was made)
         */
        public double requestedCoverage() {
            return requestedCoverage;
        }

        /**
         * @return The available coverage of the RI (unallocated capacity at the point this entry
         * was made)
         */
        public double availableCoverage() {
            return availableCoverage;
        }

        /**
         * @return The allocated coverage from the RI to the entity
         */
        public double allocatedCoverage() {
            return allocatedCoverage;
        }

        /**
         * Creates a new journal entry
         * @param cloudServiceProvider The CSP of this journal entry (both RI and covered entity should
         *                             have the same provider).
         * @param sourceName The identifier of the source of the coverage
         * @param riOid The OID of the RI
         * @param entityOid The OID of the entity
         * @param requestedCoverage The requested coverage amount (from the entity)
         * @param availableCoverage The available coverage amount (from the RI)
         * @param allocatedCoverage The allocated coverage amount (from the RI to the entity)
         * @return A newly created instance of {@link CoverageJournalEntry}
         */
        @Nonnull
        public static CoverageJournalEntry of(@Nonnull CloudServiceProvider cloudServiceProvider,
                                              @Nonnull String sourceName,
                                              long riOid,
                                              long entityOid,
                                              double requestedCoverage,
                                              double availableCoverage,
                                              double allocatedCoverage) {
            return new CoverageJournalEntry(
                    cloudServiceProvider,
                    sourceName,
                    riOid,
                    entityOid,
                    requestedCoverage,
                    availableCoverage,
                    allocatedCoverage);
        }
    }
}
