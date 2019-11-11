package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageProvider;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * This class is a wrapper around {@link ReservedInstanceCoverageAllocator}, invoking it on the
 * realtime topology with RI inventory in order to fill in RI coverage & utilization due to any
 * delays in the coverage reported from cloud billing probes. This supplemental analysis is required,
 * due to billing probes generally returning coverage day with ~24 hour delay, which may cause
 * under reporting of coverage. Downstream impacts of lower RI coverage will be exhibited in both
 * the current costs reflected in CCC and market scaling decisions.
 * <p>
 * Supplemental RI coverage analysis is meant to build on top of the coverage allocations from
 * the providers billing data. Therefore this analysis accepts {@link EntityRICoverageUpload}
 * entries, representing the RI coverage extracted from the bill
 */
public class SupplementalRICoverageAnalysis {

    private final Logger logger = LogManager.getLogger();

    private final CoverageTopology coverageTopology;

    private final List<EntityRICoverageUpload> entityRICoverageUploads;

    private final boolean allocatorConcurrentProcessing;

    private final boolean validateCoverages;

    /**
     * Constructor for creating an instance of {@link SupplementalRICoverageAnalysis}
     * @param coverageTopology The {@link CoverageTopology} to pass through to the
     *                         {@link ReservedInstanceCoverageAllocator}
     * @param entityRICoverageUploads The baseline RI coverage entries
     * @param allocatorConcurrentProcessing A boolean flag indicating whether concurrent coverage
     *                                      allocation should be enabled
     * @param validateCoverages A boolean flag indicating whether {@link ReservedInstanceCoverageAllocator}
     *                          validation should be enabled.
     */
    public SupplementalRICoverageAnalysis(
            @Nonnull CoverageTopology coverageTopology,
            @Nonnull List<EntityRICoverageUpload> entityRICoverageUploads,
            boolean allocatorConcurrentProcessing,
            boolean validateCoverages) {

        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.entityRICoverageUploads = ImmutableList.copyOf(
                Objects.requireNonNull(entityRICoverageUploads));
        this.allocatorConcurrentProcessing = allocatorConcurrentProcessing;
        this.validateCoverages = validateCoverages;
    }


    /**
     * Invokes the {@link ReservedInstanceCoverageAllocator}, converting the response and to
     * {@link EntityRICoverageUpload} records and merging them with the source records.
     *
     * @return The merged {@link EntityRICoverageUpload} records. If an entity is covered by an
     * RI through both the bill and the coverage allocator, it will have two distinct
     * {@link Coverage} records
     */
    public List<EntityRICoverageUpload> createCoverageRecordsFromSupplementalAllocation() {

        try {
            final ReservedInstanceCoverageProvider coverageProvider = createCoverageProvider();
            final ReservedInstanceCoverageAllocator coverageAllocator =
                    ReservedInstanceCoverageAllocator
                            .newBuilder()
                            .coverageProvider(coverageProvider)
                            .coverageTopology(coverageTopology)
                            .concurrentProcessing(allocatorConcurrentProcessing)
                            .validateCoverages(validateCoverages)
                            .build();

            final ReservedInstanceCoverageAllocation coverageAllocation = coverageAllocator.allocateCoverage();

            return ImmutableList.copyOf(createCoverageUploadsFromAllocation(coverageAllocation));
        } catch (Exception e) {
            logger.error("Error during supplemental coverage analysis", e);
            // return the input RI coverage list
            return entityRICoverageUploads;
        }
    }

    /**
     * Creates an instance of {@link ReservedInstanceCoverageProvider} based on the input
     * {@link EntityRICoverageUpload} instances. The coverage provider is used by the
     * {@link ReservedInstanceCoverageAllocator} as a starting point in adding supplemental coverage.
     *
     * @return The newly created {@link ReservedInstanceCoverageProvider} instance
     */
    private ReservedInstanceCoverageProvider createCoverageProvider() {
        final Table<Long, Long, Double> entityRICoverage = entityRICoverageUploads.stream()
                .map(entityRICoverageUpload -> entityRICoverageUpload.getCoverageList()
                        .stream()
                        .map(coverage -> ImmutablePair.of(entityRICoverageUpload.getEntityId(),
                                coverage))
                        .collect(Collectors.toSet()))
                .flatMap(Set::stream)
                .collect(ImmutableTable.toImmutableTable(
                        Pair::getLeft,
                        coveragePair -> coveragePair.getRight().getReservedInstanceId(),
                        coveragePair -> coveragePair.getRight().getCoveredCoupons(),
                        (c1, c2) -> Double.sum(c1, c2)));
        return () -> entityRICoverage;
    }

    /**
     * Merges the source coverage upload records with newly created records based on allocated
     * coverage from the {@link ReservedInstanceCoverageAllocator}. The RI coverage source of the newly
     * created records with be {@link RICoverageSource#SUPPLEMENTAL_COVERAGE_ALLOCATION}.
     *
     * @param coverageAllocation The allocated coverage from the {@link ReservedInstanceCoverageAllocator}
     * @return Merged {@link EntityRICoverageUpload} records
     */
    private Collection<EntityRICoverageUpload> createCoverageUploadsFromAllocation(
            @Nonnull ReservedInstanceCoverageAllocation coverageAllocation) {

        // First build a map of entity ID to EntityRICoverageUpload for faster lookup
        // in iterating coverageAllocation
        final Map<Long, EntityRICoverageUpload> coverageUploadsByEntityOid = entityRICoverageUploads.stream()
                .collect(Collectors.toMap(
                        EntityRICoverageUpload::getEntityId,
                        Function.identity()));

        coverageAllocation.allocatorCoverageTable()
                // Iterate over each entity -> RI coverage map, reducing the number of times the
                // EntityRICoverageUpload needs to be converted to a builder
                .rowMap()
                .forEach((entityOid, riCoverageMap) ->
                    coverageUploadsByEntityOid.compute(entityOid, (oid, coverageUpload) -> {
                        // Either re-use a previously existing EntityRICoverageUpload, if one exists
                        // or create a new one
                        final EntityRICoverageUpload.Builder coverageUploadBuilder =
                                coverageUpload == null ?
                                        EntityRICoverageUpload.newBuilder()
                                                .setEntityId(entityOid)
                                                .setTotalCouponsRequired(
                                                        coverageTopology.getRICoverageCapacityForEntity(entityOid)) :
                                        coverageUpload.toBuilder();
                        // Add each coverage entry, setting the source appropriately
                        riCoverageMap.entrySet()
                                .forEach(riEntry ->
                                        coverageUploadBuilder.addCoverage(
                                                Coverage.newBuilder()
                                                        .setReservedInstanceId(riEntry.getKey())
                                                        .setCoveredCoupons(riEntry.getValue())
                                                        .setRiCoverageSource(
                                                                RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION)));


                        return coverageUploadBuilder.build();
                    }));

        return coverageUploadsByEntityOid.values();
    }
}
