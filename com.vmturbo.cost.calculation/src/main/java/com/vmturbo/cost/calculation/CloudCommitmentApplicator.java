package com.vmturbo.cost.calculation;

import static com.vmturbo.trax.Trax.traxConstant;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology;
import com.vmturbo.cloud.common.commitment.CommitmentAmountUtils;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.trax.TraxNumber;

/**
 * The {@link CloudCommitmentApplicator} is responsible for calculating the percentage of an
 * entity that's covered by cloud commitment, and for recording the costs associated with those
 * cloud commitment in the entity's {@link CostJournal}.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public class CloudCommitmentApplicator<ENTITY_CLASS> {

    private static final Logger logger = LogManager.getLogger();

    private static final TraxNumber NO_COVERAGE = traxConstant(0, "no coverage");

    private static final double LOG_COVERAGE_TOLERANCE = 0.00001D;

    /**
     * The journal to write the cloud commitment costs into.
     */
    private final CostJournal.Builder<ENTITY_CLASS> journal;

    /**
     * An extractor to get entity information out of the ENTITY_CLASS.
     */
    private final EntityInfoExtractor<ENTITY_CLASS> entityInfoExtractor;

    /**
     * The {@link CloudCostData} - most notably containing the cloud commitment coverage.
     */
    private final CloudCostData cloudCostData;

    private final CloudCommitmentTopology cloudCommitmentTopology;

    /**
     * Use {@link CloudCommitmentApplicator#newFactory(CloudCommitmentTopology.CloudCommitmentTopologyFactory)} to get a factory to construct applicators.
     */
    private CloudCommitmentApplicator(@Nonnull final CostJournal.Builder<ENTITY_CLASS> journal,
                                      @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                                      @Nonnull final CloudCostData cloudCostData,
                                      @Nonnull final CloudCommitmentTopology cloudCommitmentTopology) {
        this.journal = journal;
        this.entityInfoExtractor = infoExtractor;
        this.cloudCostData = cloudCostData;
        this.cloudCommitmentTopology = cloudCommitmentTopology;
    }

    /**
     * Record the cloud commitment costs of the entity the associated journal is for into the journal.
     *
     */
    public void recordCloudCommitmentCoverage() {
        final long entityOid = entityInfoExtractor.getId(journal.getEntity());
        Optional.ofNullable(cloudCostData.getCloudCommitmentMappingByEntityId().get(entityOid))
                .ifPresent(entityCloudCommitmentMapping ->
                                           recordCloudCommitmentCoverageEntries(
                                                    entityOid,
                                                    entityCloudCommitmentMapping,
                                                    cloudCostData::getExistingCloudCommitmentData));
    }

    private void recordCloudCommitmentCoverageEntries(long entityOid,
                                                            @Nonnull Set<CloudCommitmentDTO.CloudCommitmentMapping> cloudCommitmentCoverageMappings,
                                                            @Nonnull Function<Long, Optional<CloudCommitmentData>> cloudCommitmentDataResolver) {
        // capacity should be resolved per CloudCommitmentMapping, because a single entity may have commitment coverage from multiple cloud commitment types
        for (CloudCommitmentDTO.CloudCommitmentMapping cloudCommitmentCoverageMapping : cloudCommitmentCoverageMappings) {
            final long cloudCommitmentId = cloudCommitmentCoverageMapping.getCloudCommitmentOid();
            Optional<CloudCommitmentData> cloudCommitmentDataOpt = cloudCommitmentDataResolver.apply(cloudCommitmentId);
            if (cloudCommitmentDataOpt.isPresent()) {
                // one CloudCommitmentMapping can have multiple commodities
                // get a map from CloudCommitmentCoverageTypeInfo to amount
                final Map<CloudCommitmentCoverageTypeInfo, Double> usedMap = CommitmentAmountUtils.groupByKey(cloudCommitmentCoverageMapping.getCommitmentAmount());
                // for each commodity type, write a discount journal entry
                for (Map.Entry<CloudCommitmentCoverageTypeInfo, Double> usedEntry: usedMap.entrySet()) {

                    final CloudCommitmentCoverageTypeInfo coverageType = usedEntry.getKey();
                    final double usedAmount = usedEntry.getValue();
                    final double capacityAmount = cloudCommitmentTopology.getCoverageCapacityForEntity(entityOid, coverageType);

                    if (usedAmount <= 0.0 || capacityAmount <= 0.0) {
                        logger.trace("Skipping coverage entry for {} with type {} (amount={}, capacity={})",
                                entityOid, coverageType, usedAmount, capacityAmount);
                        continue;
                    }

                    final CloudCommitmentDTO.CloudCommitmentCoverageVector coverageVector = CloudCommitmentDTO.CloudCommitmentCoverageVector.newBuilder()
                            .setCapacity(capacityAmount)
                            .setUsed(usedAmount)
                            .setVectorType(coverageType)
                            .build();
                    journal.recordCloudCommitmentDiscount(CostCategory.ON_DEMAND_COMPUTE, cloudCommitmentDataOpt.get().asTopologyCommitment(), coverageVector);
                }
            } else {
                logger.debug("Unable to resolve cloud commitment data for entity " +
                        "(Entity OID={}, Cloud commitment OID={})", entityOid, cloudCommitmentId);
            }
        }
    }

    @Nonnull
    public static <ENTITY_CLASS> CloudCommitmentApplicatorFactory<ENTITY_CLASS> newFactory(CloudCommitmentTopology.CloudCommitmentTopologyFactory<ENTITY_CLASS> commitmentTopologyFactory) {
        return (journal, infoExtractor, cloudCostData, cloudTopology) -> {
            final CloudCommitmentTopology cloudCommitmentTopology = commitmentTopologyFactory.createTopology(cloudTopology);
            return new CloudCommitmentApplicator<>(journal, infoExtractor, cloudCostData, cloudCommitmentTopology);
        };
    }

    /**
     * A factory class for {@link CloudCommitmentApplicator} instances, used for dependency
     * injection and mocking for tests.
     *
     * @param <ENTITY_CLASS> See {@link CloudCommitmentApplicator}.
     */
    @FunctionalInterface
    public interface CloudCommitmentApplicatorFactory<ENTITY_CLASS> {
        @Nonnull
        CloudCommitmentApplicator<ENTITY_CLASS> newCloudCommitmentApplicator(
                @Nonnull final CostJournal.Builder<ENTITY_CLASS> journal,
                @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                @Nonnull final CloudCostData cloudCostData,
                @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology);
    }
}
