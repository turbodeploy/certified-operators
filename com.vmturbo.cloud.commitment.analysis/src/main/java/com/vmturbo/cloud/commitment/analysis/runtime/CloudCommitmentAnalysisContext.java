package com.vmturbo.cloud.commitment.analysis.runtime;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.NotImplementedException;

import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher.CloudCommitmentSpecMatcherFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology.MinimalCloudTopologyFactory;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.TopologyReference;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
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

    private final CloudCommitmentAnalysisConfig analysisConfig;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory;

    private final TopologyEntityCloudTopologyFactory fullCloudTopologyFactory;

    private final CloudCommitmentSpecMatcherFactory cloudCommitmentSpecMatcherFactory;

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    private final ExecutorService analysisExecutorService;

    private final StaticAnalysisConfig staticAnalysisConfig;

    private final SetOnce<MinimalCloudTopology<MinimalEntity>> sourceCloudTopology = new SetOnce<>();

    private final SetOnce<CloudTopology<TopologyEntityDTO>> cloudTierTopology = new SetOnce<>();

    private final SetOnce<TimeInterval> analysisWindow = new SetOnce<>();

    private final SetOnce<CloudCommitmentSpecMatcher<?>> cloudCommitmentSpecMatcher = new SetOnce<>();

    private final SetOnce<ComputeTierFamilyResolver> computeTierFamilyResolver = new SetOnce<>();

    /**
     * Constructs a new {@link CloudCommitmentAnalysisContext} instance.
     * @param analysisInfo The analysis info, used in resolving the topology reference and building
     *                     a log marker for the analysis.
     * @param analysisConfig The analysis config, used to resolve the correct {@link CloudCommitmentSpecMatcher}
     *                       implementation.
     * @param repositoryClient The repository client, use to resolve the topology.
     * @param minimalCloudTopologyFactory A minimal cloud topology factory.
     * @param fullCloudTopologyFactory A full cloud topology factory.
     * @param cloudCommitmentSpecMatcherFactory A {@link CloudCommitmentSpecMatcherFactory}, used to create
     *                                          a {@link CloudCommitmentSpecMatcher} based on the type
     *                                          of recommendations request in the analysis config.
     * @param computeTierFamilyResolverFactory A factory for creating {@link ComputeTierFamilyResolver} instances.
     * @param analysisExecutorService The executor service to be used as a worker pool by analysis stages.
     * @param staticAnalysisConfig Configuration attributes that are static across all instances
     *                             of {@link CloudCommitmentAnalysis}.
     */
    public CloudCommitmentAnalysisContext(@Nonnull CloudCommitmentAnalysisInfo analysisInfo,
                                          @Nonnull CloudCommitmentAnalysisConfig analysisConfig,
                                          @Nonnull RepositoryServiceBlockingStub repositoryClient,
                                          @Nonnull MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory,
                                          @Nonnull TopologyEntityCloudTopologyFactory fullCloudTopologyFactory,
                                          @Nonnull CloudCommitmentSpecMatcherFactory cloudCommitmentSpecMatcherFactory,
                                          @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory,
                                          @Nonnull ExecutorService analysisExecutorService,
                                          @Nonnull StaticAnalysisConfig staticAnalysisConfig) {

        this.analysisInfo = Objects.requireNonNull(analysisInfo);
        this.analysisConfig = Objects.requireNonNull(analysisConfig);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.minimalCloudTopologyFactory = Objects.requireNonNull(minimalCloudTopologyFactory);
        this.fullCloudTopologyFactory = Objects.requireNonNull(fullCloudTopologyFactory);
        this.cloudCommitmentSpecMatcherFactory = Objects.requireNonNull(cloudCommitmentSpecMatcherFactory);
        this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
        this.analysisExecutorService = Objects.requireNonNull(analysisExecutorService);
        this.staticAnalysisConfig = Objects.requireNonNull(staticAnalysisConfig);
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
     * Returns the configured analysis bucket interval. The analysis bucket is the duration that segments
     * of analyzable demand should be grouped by for both coverage analysis and as demand data points
     * for recommendation analysis. Typically, demand will be analyzed on hourly segments, given cloud
     * commitments are generally billed at this interval.
     * @return The analysis bucket duration.
     */
    @Nonnull
    public BoundedDuration getAnalysisBucket() {
        return staticAnalysisConfig.analysisBucket();
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
     * Sets the analysis window, which is the start and end time of all demand that will be analyzed
     * by this analysis.
     * @param analysisInterval The analysis window.
     * @return True, if the provided analysis window was recorded. False, if the analysis window
     * has already been set.
     */
    public boolean setAnalysisWindow(@Nonnull TimeInterval analysisInterval) {

        Preconditions.checkNotNull(analysisInterval, "Analysis interval cannot be null");

        return this.analysisWindow.trySetValue(analysisInterval);
    }

    /**
     * Gets the analysis window.
     * @return An optional containing the analysis window.
     */
    @Nonnull
    public Optional<TimeInterval> getAnalysisWindow() {
        return analysisWindow.getValue();
    }

    /**
     * Returns the {@link CloudCommitmentSpecMatcher} implementation, based on the purchase profile
     * of the {@link CloudCommitmentAnalysisConfig}. The spec matcher will return specs corresponding
     * to the requested cloud commitment type (e.g. RI specs, if RI recommendations are requested).
     *
     * @return The {@link CloudCommitmentSpecMatcher} instance for the analysis.
     */
    @Nonnull
    public CloudCommitmentSpecMatcher<?> getCloudCommitmentSpecMatcher() {
        return cloudCommitmentSpecMatcher.ensureSet(() ->
                cloudCommitmentSpecMatcherFactory.createSpecMatcher(
                        getCloudTierTopology(),
                        analysisConfig.getPurchaseProfile()));
    }

    /**
     * Gets the {@link ComputeTierFamilyResolver}, used to compare compute tier demand within a family
     * and in order to determine whether two compute tiers are within the same family.
     * @return The {@link ComputeTierFamilyResolver}, based on {@link #getCloudTierTopology()}.
     */
    @Nonnull
    public ComputeTierFamilyResolver getComputeTierFamilyResolver() {
        return computeTierFamilyResolver.ensureSet(() ->
                computeTierFamilyResolverFactory.createResolver(
                        getCloudTierTopology()));
    }

    /**
     * Gets the cloud commitment recommendation type.
     * @return The cloud commitment recommendation type, based on the purchase profile in the
     * analysis request.
     */
    @Nonnull
    public CloudCommitmentType getRecommendationType() {
        if (analysisConfig.getPurchaseProfile().hasRiPurchaseProfile()) {
            return CloudCommitmentType.RESERVED_INSTANCE;
        } else {
            throw new NotImplementedException("Only RI recommendations are supported");
        }
    }

    @Nonnull
    public ExecutorService getAnalysisExecutorService() {
        return analysisExecutorService;
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

        final Set<Integer> entityTypes = ImmutableSet.of(
                EntityType.COMPUTE_TIER_VALUE);

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
         * @param analysisConfig The analysis config.
         * @return The newly created {@link CloudCommitmentAnalysisContext} instance.
         */
        @Nonnull
        CloudCommitmentAnalysisContext createContext(
                @Nonnull CloudCommitmentAnalysisInfo analysisInfo,
                @Nonnull CloudCommitmentAnalysisConfig analysisConfig);
    }


    /**
     * The default factory class for creating {@link CloudCommitmentAnalysisContext} instances.
     */
    public static class DefaultAnalysisContextFactory implements AnalysisContextFactory {

        private final RepositoryServiceBlockingStub repositoryClient;

        private final MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory;

        private final TopologyEntityCloudTopologyFactory fullCloudTopologyFactory;

        private final CloudCommitmentSpecMatcherFactory cloudCommitmentSpecMatcherFactory;

        private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

        private final ExecutorService analysisExecutorService;

        private final StaticAnalysisConfig staticAnalysisConfig;

        /**
         * Construct the analysis context factory.
         *
         * @param repositoryClient The repository client, used to fetch topologies.
         * @param minimalCloudTopologyFactory The minimal cloud topology factory.
         * @param fullCloudTopologyFactory The full cloud topology factory.
         * @param cloudCommitmentSpecMatcherFactory the cloud commitment spec matcher factory.
         * @param computeTierFamilyResolverFactory A factory for creating {@link ComputeTierFamilyResolver} instances.
         * @param analysisExecutorService The executor service to be used as a worker pool by analysis stages.
         * @param staticAnalysisConfig The analysis config, containing static attributes.
         */
        public DefaultAnalysisContextFactory(@Nonnull RepositoryServiceBlockingStub repositoryClient,
                                             @Nonnull MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory,
                                             @Nonnull TopologyEntityCloudTopologyFactory fullCloudTopologyFactory,
                                             @Nonnull CloudCommitmentSpecMatcherFactory cloudCommitmentSpecMatcherFactory,
                                             @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory,
                                             @Nonnull ExecutorService analysisExecutorService,
                                             @Nonnull StaticAnalysisConfig staticAnalysisConfig) {

            this.repositoryClient = Objects.requireNonNull(repositoryClient);
            this.minimalCloudTopologyFactory = Objects.requireNonNull(minimalCloudTopologyFactory);
            this.fullCloudTopologyFactory = Objects.requireNonNull(fullCloudTopologyFactory);
            this.cloudCommitmentSpecMatcherFactory = Objects.requireNonNull(cloudCommitmentSpecMatcherFactory);
            this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
            this.analysisExecutorService = Objects.requireNonNull(analysisExecutorService);
            this.staticAnalysisConfig = Objects.requireNonNull(staticAnalysisConfig);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CloudCommitmentAnalysisContext createContext(
                @Nonnull final CloudCommitmentAnalysisInfo analysisInfo,
                @Nonnull final CloudCommitmentAnalysisConfig analysisConfig) {
            return new CloudCommitmentAnalysisContext(
                    analysisInfo,
                    analysisConfig,
                    repositoryClient,
                    minimalCloudTopologyFactory,
                    fullCloudTopologyFactory,
                    cloudCommitmentSpecMatcherFactory,
                    computeTierFamilyResolverFactory,
                    analysisExecutorService,
                    staticAnalysisConfig);
        }
    }
}
