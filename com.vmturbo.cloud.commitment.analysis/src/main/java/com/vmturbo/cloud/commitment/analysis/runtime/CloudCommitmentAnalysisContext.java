package com.vmturbo.cloud.commitment.analysis.runtime;

import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.cloud.commitment.analysis.topology.MinimalCloudTopology;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalCloudTopology.MinimalCloudTopologyFactory;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.TopologyReference;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A context for {@link CloudCommitmentAnalysis}, containing shared data across {@link AnalysisStage}.
 */
public class CloudCommitmentAnalysisContext {

    private final CloudCommitmentAnalysisInfo analysisInfo;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory;

    private final TopologyEntityCloudTopologyFactory fullCloudTopologyFactory;

    private final StaticAnalysisConfig staticAnalysisConfig;

    private final String logMarker;

    private final SetOnce<MinimalCloudTopology<MinimalEntity>> sourceCloudTopology = new SetOnce<>();

    private final SetOnce<CloudTopology<TopologyEntityDTO>> cloudTierTopology = new SetOnce<>();

    private final SetOnce<Instant> analysisStartTime = new SetOnce<>();

    /**
     * Constructs a new {@link CloudCommitmentAnalysisContext} instance.
     * @param analysisInfo The analysis info, used in resolving the topology reference and building
     *                     a log marker for the analysis.
     * @param repositoryClient The repository client, use to resolve the topology.
     * @param minimalCloudTopologyFactory A minimal cloud topology factory.
     * @param fullCloudTopologyFactory A full cloud topology factory.
     * @param staticAnalysisConfig Configuration attributes that are static across all instances
     *                             of {@link CloudCommitmentAnalysis}.
     */
    public CloudCommitmentAnalysisContext(@Nonnull CloudCommitmentAnalysisInfo analysisInfo,
                                          @Nonnull RepositoryServiceBlockingStub repositoryClient,
                                          @Nonnull MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory,
                                          @Nonnull TopologyEntityCloudTopologyFactory fullCloudTopologyFactory,
                                          @Nonnull StaticAnalysisConfig staticAnalysisConfig) {

        this.analysisInfo = Objects.requireNonNull(analysisInfo);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.minimalCloudTopologyFactory = Objects.requireNonNull(minimalCloudTopologyFactory);
        this.fullCloudTopologyFactory = Objects.requireNonNull(fullCloudTopologyFactory);
        this.staticAnalysisConfig = Objects.requireNonNull(staticAnalysisConfig);

        this.logMarker = String.format("[%s|%s]", analysisInfo.getOid(), analysisInfo.getAnalysisTag());
    }

    /**
     * The log marker to use in {@link AnalysisStage} instances.
     * @return The log marker for the analysis.
     */
    @Nonnull
    public String getLogMarker() {
        return logMarker;
    }

    /**
     * Returns the analysis info associated with this context.
     * @return The {@link CloudCommitmentAnalysisInfo}.
     */
    @Nonnull
    public CloudCommitmentAnalysisInfo getAnalysisInfo() {
        return analysisInfo;
    }

    /**
     * Gets the unit (e.g. hour, day, minute, etc) associated with an analysis segment. The analysis
     * segment represents the blocks of analyzable data for both uncovered demand calculations and
     * recommendation analysis (.e.g the demand should be analyzed in 1 hour increments).
     *
     * @return The {@link TemporalUnit} to use for the analysis segments.
     */
    @Nonnull
    public TemporalUnit getAnalysisSegmentUnit() {
        return staticAnalysisConfig.analysisSegmentUnit();
    }

    /**
     * Gets the interval/amount associated with the analysis segment. Used in conjunction with the
     * {@link #getAnalysisSegmentUnit()}, allows specifying that demand should be analyzed in blocks
     * like 3 hour segments, where 3 would represent the interval and hour would represent the unit.
     *
     * @return The interval/amount for the analysis segments.
     */
    public long getAnalysisSegmentInterval() {
        return staticAnalysisConfig.analysisSegmentInterval();
    }

    /**
     * Loads the source topology associated with the {@link CloudCommitmentAnalysisInfo#getTopologyReference()}
     * from the repository and builds a {@link MinimalCloudTopology}. The cloud topology will only be
     * loaded once and saved for any subsequent requests, in order to provide a consistent topology
     * to all {@link AnalysisStage} instances.
     *
     * @return The {@link MinimalCloudTopology} associated with the topology referenced in the analysis config.
     */
    @Nonnull
    public MinimalCloudTopology<MinimalEntity> getSourceCloudTopology() {
        return sourceCloudTopology.ensureSet(() -> createMinimalCloudTopology(TopologyType.SOURCE));
    }

    /**
     * Retrieves and returns a topology containing cloud tiers represented as full {@link TopologyEntityDTO}
     * instances. Full entities are queried for cloud tiers only, in order to provide type-specific
     * info for these entities, without requiring all data for the full topology. After the first
     * retrieval, the topology will be cached for subsequent invocations.
     *
     * @return A {@link CloudTopology}, containing cloud tiers only.
     */
    @Nonnull
    public CloudTopology<TopologyEntityDTO> getCloudTierTopology() {
        return cloudTierTopology.ensureSet(this::createCloudTierTopology);
    }

    /**
     * Sets the analysis start time, which is a normalized version of the historical look back time
     * specified in the analysis config.
     *
     * @param analysisStartTime The analysis start time to store.
     * @return True, if the specified analysis start time was saved. False, if the analysis start time
     * has already been set.
     */
    public boolean setAnalysisStartTime(@Nonnull Instant analysisStartTime) {

        Preconditions.checkNotNull(analysisStartTime, "Analysis start time cannot be null");

        return this.analysisStartTime.trySetValue(analysisStartTime);
    }

    /**
     * Returns the normalized analysis start time. The analysis start time is the requested start time
     * from the analysis config, truncated based on the analysis interval.
     * @return The normalized analysis start time.
     */
    @Nonnull
    public Optional<Instant> getAnalysisStartTime() {
        return analysisStartTime.getValue();
    }

    @Nonnull
    private MinimalCloudTopology<MinimalEntity> createMinimalCloudTopology(@Nonnull TopologyType topologyType) {

        final Stream<MinimalEntity> entities =
                retrieveTopologyEntities(Type.MINIMAL, topologyType, Collections.emptySet())
                        .map(PartialEntity::getMinimal);
        final MinimalCloudTopology<MinimalEntity> cloudTopology =
                minimalCloudTopologyFactory.createCloudTopology(entities);
        return cloudTopology;
    }

    @Nonnull
    private Stream<PartialEntity> retrieveTopologyEntities(@Nonnull Type entityDTOType,
                                                           @Nonnull TopologyType topologyType,
                                                           @Nonnull Set<Integer> entityTypes) {
        final RetrieveTopologyEntitiesRequest.Builder retrieveTopologyRequest =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setReturnType(entityDTOType)
                        .setTopologyType(topologyType);

        if (!entityTypes.isEmpty()) {
            retrieveTopologyRequest.addAllEntityType(entityTypes);
        }

        // If no explicit topology reference is specified in the config, the repository will resolve
        // the request to the latest realtime topology
        if (analysisInfo.hasTopologyReference()) {
            final TopologyReference topologyReference = analysisInfo.getTopologyReference();

            // repository assumes realtime if no context ID is specified in the request.
            if (topologyReference.hasTopologyContextId()) {
                retrieveTopologyRequest.setTopologyContextId(topologyReference.getTopologyContextId());
            }

            // The topology ID will only be used for plan contexts
            if (topologyReference.hasTopologyId()) {
                retrieveTopologyRequest.setTopologyId(topologyReference.getTopologyId());
            }
        }

        return RepositoryDTOUtil.topologyEntityStream(
                repositoryClient.retrieveTopologyEntities(retrieveTopologyRequest.build()));
    }

    @Nonnull
    private CloudTopology<TopologyEntityDTO> createCloudTierTopology() {

        final Set<Integer> entityTypes = Collections.singleton(EntityType.COMPUTE_TIER_VALUE);

        final Stream<TopologyEntityDTO> entities =
                retrieveTopologyEntities(Type.FULL, TopologyType.SOURCE, entityTypes)
                        .map(PartialEntity::getFullEntity);
        final CloudTopology<TopologyEntityDTO> cloudTopology =
                fullCloudTopologyFactory.newCloudTopology(entities);
        return cloudTopology;
    }

    /**
     * A factory for creating {@link CloudCommitmentAnalysisContext} instances.
     */
    public interface AnalysisContextFactory {

        /**
         * Creates a new instance, tied to a specific {@link CloudCommitmentAnalysis}.
         *
         * @param analysisInfo The analysis info for the target {@link CloudCommitmentAnalysis}.
         * @return The newly created {@link CloudCommitmentAnalysisContext} instance.
         */
        @Nonnull
        CloudCommitmentAnalysisContext createContext(@Nonnull CloudCommitmentAnalysisInfo analysisInfo);
    }


    /**
     * The default factory class for creating {@link CloudCommitmentAnalysisContext} instances.
     */
    public static class DefaultAnalysisContextFactory implements AnalysisContextFactory {

        private final RepositoryServiceBlockingStub repositoryClient;

        private final MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory;

        private final TopologyEntityCloudTopologyFactory fullCloudTopologyFactory;

        private final StaticAnalysisConfig staticAnalysisConfig;

        /**
         * Construct the analysis context factory.
         *
         * @param repositoryClient The repository client, used to fetch topologies.
         * @param minimalCloudTopologyFactory The minimal cloud topology factory.
         * @param fullCloudTopologyFactory The full cloud topology factory.
         * @param staticAnalysisConfig The analysis config, containing static attributes.
         */
        public DefaultAnalysisContextFactory(@Nonnull RepositoryServiceBlockingStub repositoryClient,
                                             @Nonnull MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory,
                                             @Nonnull TopologyEntityCloudTopologyFactory fullCloudTopologyFactory,
                                             @Nonnull StaticAnalysisConfig staticAnalysisConfig) {

            this.repositoryClient = Objects.requireNonNull(repositoryClient);
            this.minimalCloudTopologyFactory = Objects.requireNonNull(minimalCloudTopologyFactory);
            this.fullCloudTopologyFactory = Objects.requireNonNull(fullCloudTopologyFactory);
            this.staticAnalysisConfig = Objects.requireNonNull(staticAnalysisConfig);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CloudCommitmentAnalysisContext createContext(@Nonnull final CloudCommitmentAnalysisInfo analysisInfo) {
            return new CloudCommitmentAnalysisContext(
                    analysisInfo,
                    repositoryClient,
                    minimalCloudTopologyFactory,
                    fullCloudTopologyFactory,
                    staticAnalysisConfig);
        }
    }
}
