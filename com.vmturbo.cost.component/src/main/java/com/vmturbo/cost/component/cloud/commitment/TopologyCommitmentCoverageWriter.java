package com.vmturbo.cost.component.cloud.commitment;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory;
import com.vmturbo.cloud.common.commitment.CommitmentAmountUtils;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentCoverage;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A writer to persist cloud commitment coverage allocations to the appropriate stores (coverage|mapping|utilization).
 */
public class TopologyCommitmentCoverageWriter {

    private static final Logger logger = LogManager.getLogger();

    private final SingleFieldDataStore<CoverageInfo, ?> commitmentCoverageStore;

    private final SingleFieldDataStore<MappingInfo, ?> commitmentMappingStore;

    private final SingleFieldDataStore<UtilizationInfo, ?> commitmentUtilizationStore;

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final CloudCommitmentTopology commitmentTopology;

    private TopologyCommitmentCoverageWriter(@Nonnull SingleFieldDataStore<CoverageInfo, ?> commitmentCoverageStore,
                                             @Nonnull SingleFieldDataStore<MappingInfo, ?> commitmentMappingStore,
                                             @Nonnull SingleFieldDataStore<UtilizationInfo, ?> commitmentUtilizationStore,
                                             @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                             @Nonnull CloudCommitmentTopology commitmentTopology) {

        this.commitmentCoverageStore = Objects.requireNonNull(commitmentCoverageStore);
        this.commitmentMappingStore = Objects.requireNonNull(commitmentMappingStore);
        this.commitmentUtilizationStore = Objects.requireNonNull(commitmentUtilizationStore);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.commitmentTopology = Objects.requireNonNull(commitmentTopology);
    }

    /**
     * Persist the cloud commitment allocations for the provided topology.
     * @param topologyInfo The topology info.
     * @param commitmentAllocations The cloud commitment allocations, representing a table of entity OID|Commitment OID|allocation.
     */
    public void persistCommitmentAllocations(@Nonnull TopologyInfo topologyInfo,
                                             @Nonnull Table<Long, Long, CloudCommitmentAmount> commitmentAllocations) {

        logger.info("Persisting {} coverage allocations for {}", commitmentAllocations.size(), topologyInfo);

        persistCoverages(topologyInfo, commitmentAllocations);
        persistMappings(topologyInfo, commitmentAllocations);
        persistUtilization(topologyInfo, commitmentAllocations);
    }

    @Nonnull
    private ScopedCommitmentCoverage.Builder resolveCoverageScope(long entityOid) {

        final ScopedCommitmentCoverage.Builder scopedCoverage = ScopedCommitmentCoverage.newBuilder()
                .setEntityOid(entityOid);

        final BiConsumer<Function<Long, Optional<TopologyEntityDTO>>, Consumer<Long>> topologyAttributeMapper = (entityExtractor, attributeSetter) ->
                entityExtractor.apply(entityOid).ifPresent(topologyEntity -> attributeSetter.accept(topologyEntity.getOid()));

        cloudTopology.getEntity(entityOid).ifPresent(entity ->
                scopedCoverage.setEntityType(entity.getEntityType()));

        topologyAttributeMapper.accept(cloudTopology::getConnectedAvailabilityZone, scopedCoverage::setZoneOid);
        topologyAttributeMapper.accept(cloudTopology::getConnectedRegion, scopedCoverage::setRegionOid);
        topologyAttributeMapper.accept(cloudTopology::getOwner, scopedCoverage::setAccountOid);
        topologyAttributeMapper.accept(cloudTopology::getConnectedService, scopedCoverage::setCloudServiceOid);
        topologyAttributeMapper.accept(cloudTopology::getServiceProvider, scopedCoverage::setServiceProviderOid);

        return scopedCoverage;
    }

    @Nonnull
    private ScopedCommitmentUtilization.Builder resolveUtilizationScope(long commitmentOid) {

        final BiConsumer<Function<Long, Optional<TopologyEntityDTO>>, Consumer<Long>> topologyAttributeMapper = (entityExtractor, attributeSetter) ->
                entityExtractor.apply(commitmentOid).ifPresent(topologyEntity -> attributeSetter.accept(topologyEntity.getOid()));

        final ScopedCommitmentUtilization.Builder scopedUtilization = ScopedCommitmentUtilization.newBuilder();
        topologyAttributeMapper.accept(cloudTopology::getEntity, scopedUtilization::setCloudCommitmentOid);
        topologyAttributeMapper.accept(cloudTopology::getOwner, scopedUtilization::setAccountOid);
        topologyAttributeMapper.accept(cloudTopology::getConnectedRegion, scopedUtilization::setRegionOid);
        topologyAttributeMapper.accept(cloudTopology::getServiceProvider, scopedUtilization::setServiceProviderOid);

        return scopedUtilization;
    }

    private void persistCoverages(@Nonnull TopologyInfo topologyInfo,
                                  @Nonnull Table<Long, Long, CloudCommitmentAmount> allocations) {

        try {
            final CoverageInfo.Builder coverageInfo = CoverageInfo.builder().topologyInfo(topologyInfo);

            // Iterate through all coverable entities (not just those that have allocations within scopedAllocations). As an optimization,
            // only entities owned by accounts that are supported by the commitment topology will be processed.
            cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .stream()
                    .filter(account -> commitmentTopology.isSupportedAccount(account.getOid()))
                    .flatMap(account -> cloudTopology.streamOwnedEntitiesOfType(account.getOid(), EntityType.VIRTUAL_MACHINE_VALUE))
                    .map(TopologyEntityDTO::getOid)
                    .map(entityOid -> createScopedCoverage(entityOid, allocations.row(entityOid).values()))
                    .forEach(scopedCoverage -> coverageInfo.putEntityCoverageMap(scopedCoverage.getEntityOid(), scopedCoverage));

            commitmentCoverageStore.setData(coverageInfo.build());
        } catch (Exception e) {
            logger.error("Error persisting commitment coverage", e);
        }
    }

    private void persistMappings(@Nonnull TopologyInfo topologyInfo,
                                 @Nonnull Table<Long, Long, CloudCommitmentAmount> allocations) {

        try {
            final MappingInfo.Builder coverageMappingInfo = MappingInfo.builder().topologyInfo(topologyInfo);

            allocations.cellSet().forEach(allocation -> coverageMappingInfo.addCloudCommitmentMapping(CloudCommitmentMapping.newBuilder()
                            .setEntityOid(allocation.getRowKey())
                            .setCloudCommitmentOid(allocation.getColumnKey())
                            .setCommitmentAmount(allocation.getValue())
                            .build()));

            commitmentMappingStore.setData(coverageMappingInfo.build());
        } catch (Exception e) {
            logger.error("Error persisting commitment mappings", e);
        }
    }

    private ScopedCommitmentCoverage createScopedCoverage(long entityOid,
                                                          @Nonnull Collection<CloudCommitmentAmount> entityAllocations) {

        final ScopedCommitmentCoverage.Builder scopedCoverage = resolveCoverageScope(entityOid);

        final Map<CloudCommitmentCoverageTypeInfo, Double> allocationsByVectorMap = CommitmentAmountUtils.groupAndSum(entityAllocations);
        commitmentTopology.getSupportedCoverageVectors(entityOid).forEach(coverageVector ->
                scopedCoverage.addCoverageVector(CloudCommitmentCoverageVector.newBuilder()
                        .setVectorType(coverageVector)
                        .setUsed(allocationsByVectorMap.getOrDefault(coverageVector, 0.0))
                        .setCapacity(commitmentTopology.getCoverageCapacityForEntity(entityOid, coverageVector))));

        return scopedCoverage.build();
    }

    private void persistUtilization(@Nonnull TopologyInfo topologyInfo,
                                    @Nonnull Table<Long, Long, CloudCommitmentAmount> allocations) {

        try {
            final UtilizationInfo.Builder utilizationInfo = UtilizationInfo.builder().topologyInfo(topologyInfo);

            cloudTopology.getAllEntitiesOfType(EntityType.CLOUD_COMMITMENT_VALUE).stream()
                    .map(TopologyEntityDTO::getOid)
                    .map(commitmentOid -> createScopedUtilization(commitmentOid, allocations.column(commitmentOid).values()))
                    .forEach(scopedUtilization -> utilizationInfo.putCommitmentUtilizationMap(scopedUtilization.getCloudCommitmentOid(),
                            scopedUtilization));

            commitmentUtilizationStore.setData(utilizationInfo.build());
        } catch (Exception e) {
            logger.error("Error persisting commitment utilization", e);
        }
    }

    private ScopedCommitmentUtilization createScopedUtilization(long commitmentOid,
                                                                @Nonnull Collection<CloudCommitmentAmount> commitmentAllocations) {

        final ScopedCommitmentUtilization.Builder scopedUtilization = resolveUtilizationScope(commitmentOid);

        final Map<CloudCommitmentCoverageTypeInfo, Double> allocationsByVectorMap = CommitmentAmountUtils.groupAndSum(commitmentAllocations);
        commitmentTopology.getCommitmentCapacityVectors(commitmentOid).forEach((capacityVectorInfo, capacity) ->
                scopedUtilization.addUtilizationVector(CloudCommitmentUtilizationVector.newBuilder()
                        .setVectorType(capacityVectorInfo)
                        .setUsed(allocationsByVectorMap.getOrDefault(capacityVectorInfo, 0.0))
                        .setCapacity(capacity)
                        .build()));

        return scopedUtilization.build();
    }

    /**
     * A factory class for creating {@link TopologyCommitmentCoverageWriter} instances.
     */
    public static class Factory {

        private final SingleFieldDataStore<CoverageInfo, ?> commitmentCoverageStore;

        private final SingleFieldDataStore<MappingInfo, ?> commitmentMappingStore;

        private final SingleFieldDataStore<UtilizationInfo, ?> commitmentUtilizationStore;

        private final CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory;

        /**
         * Constructs a new coverage writer factory.
         * @param commitmentCoverageStore The commitment coverage store.
         * @param commitmentMappingStore The commitment mapping store.
         * @param commitmentUtilizationStore The commitment utilization store.
         * @param commitmentTopologyFactory The commitment topology factory.
         */
        public Factory(@Nonnull SingleFieldDataStore<CoverageInfo, ?> commitmentCoverageStore,
                       @Nonnull SingleFieldDataStore<MappingInfo, ?> commitmentMappingStore,
                       @Nonnull SingleFieldDataStore<UtilizationInfo, ?> commitmentUtilizationStore,
                       @Nonnull CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory) {

            this.commitmentCoverageStore = Objects.requireNonNull(commitmentCoverageStore);
            this.commitmentMappingStore = Objects.requireNonNull(commitmentMappingStore);
            this.commitmentUtilizationStore = Objects.requireNonNull(commitmentUtilizationStore);
            this.commitmentTopologyFactory = Objects.requireNonNull(commitmentTopologyFactory);
        }

        /**
         * Constructs a new writer for the provided cloud topology.
         * @param cloudTopology The cloud topology, which will be used to resolve scoping information for provided
         * allocations at the point of persistence. The cloud topology should match the topology used to generate
         * the allocations.
         * @return The newly constructed coverage writer.
         */
        @Nonnull
        public TopologyCommitmentCoverageWriter newWriter(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

            final CloudCommitmentTopology cloudCommitmentTopology = commitmentTopologyFactory.createTopology(cloudTopology);

            return new TopologyCommitmentCoverageWriter(
                    commitmentCoverageStore,
                    commitmentMappingStore,
                    commitmentUtilizationStore,
                    cloudTopology,
                    cloudCommitmentTopology);
        }
    }
}
