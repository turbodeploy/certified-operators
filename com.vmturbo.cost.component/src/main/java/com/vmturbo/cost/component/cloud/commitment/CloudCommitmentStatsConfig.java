package com.vmturbo.cost.component.cloud.commitment;

import java.sql.SQLException;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.AbstractMessage;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import com.vmturbo.cloud.common.stat.CloudGranularityCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentStatsServiceController;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentUploadServiceController;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.GsonAdaptersCoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.SQLCloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.coverage.TopologyCoverageFilterApplicator;
import com.vmturbo.cost.component.cloud.commitment.coverage.TopologyEntityCoverageFilter;
import com.vmturbo.cost.component.cloud.commitment.mapping.CommitmentMappingFilter;
import com.vmturbo.cost.component.cloud.commitment.mapping.CommitmentMappingFilterApplicator;
import com.vmturbo.cost.component.cloud.commitment.mapping.GsonAdaptersMappingInfo;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator;
import com.vmturbo.cost.component.cloud.commitment.utilization.GsonAdaptersUtilizationInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.SQLCloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.TopologyCommitmentUtilizationFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.TopologyUtilizationFilterApplicator;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.stores.DiagnosableDataStoreCollector;
import com.vmturbo.cost.component.stores.InMemorySingleFieldDataStore;
import com.vmturbo.cost.component.stores.InMemorySourceProjectedFieldsDataStore;
import com.vmturbo.cost.component.stores.JsonDiagnosableDataStoreCollector;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;
import com.vmturbo.cost.component.stores.SourceProjectedFieldsDataStore;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * A configuration file for cloud commitment statistics stores (coverage & utilization),
 * as well as the associated RPC classes.
 */
@Import(DbAccessConfig.class)
@Configuration
public class CloudCommitmentStatsConfig {

    // Should be defined in CostDBConfig
    @Autowired
    private DbAccessConfig dbAccessConfig;

    // Should be auto-wired from ReservedInstanceConfig
    @Autowired
    private TimeFrameCalculator timeFrameCalculator;

    @Value("${cloudCommitment.maxStatRecordsPerChunk:100}")
    private int maxStatRecordsPerChunk;

    /**
     * A bean for {@link CloudCommitmentUtilizationStore}.
     * @return A bean for {@link CloudCommitmentUtilizationStore}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentUtilizationStore cloudCommitmentUtilizationStore() {
        try {
            return new SQLCloudCommitmentUtilizationStore(dbAccessConfig.dsl(), cloudGranularityCalculator());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create cloudCommitmentUtilizationStore bean", e);
        }
    }

    /**
     * A bean for {@link CloudGranularityCalculator}.
     * @return A bean for {@link CloudGranularityCalculator}.
     */
    @Bean
    public CloudGranularityCalculator cloudGranularityCalculator() {
        return new CloudGranularityCalculator(timeFrameCalculator);
    }

    /**
     * A bean for {@link CloudCommitmentCoverageStore}.
     * @return A bean for {@link CloudCommitmentCoverageStore}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentCoverageStore cloudCommitmentCoverageStore() {
        try {
            return new SQLCloudCommitmentCoverageStore(dbAccessConfig.dsl(), cloudGranularityCalculator());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create cloudCommitmentCoverageStore bean", e);
        }
    }

    /**
     * A bean for topology commitment store GSON.
     * @return A bean for topology commitment store GSON.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public Gson topologyCommitmentStoreGson() {
        return new GsonBuilder()
                .registerTypeHierarchyAdapter(AbstractMessage.class,
                        new ComponentGsonFactory.ProtoAdapter())
                .registerTypeAdapterFactory(new GsonAdaptersUtilizationInfo())
                .registerTypeAdapterFactory(new GsonAdaptersCoverageInfo())
                .registerTypeAdapterFactory(new GsonAdaptersMappingInfo())
                .create();
    }

    /**
     * The source topology commitment utilization store.
     * @return The source topology commitment utilization store.
     */
    @Nonnull
    @Bean
    public SingleFieldDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> sourceTopologyCommitmentUtilizationStore() {
        return new InMemorySingleFieldDataStore<>(new TopologyUtilizationFilterApplicator());
    }

    /**
     * A bean for source topology commitment utilization store.
     * @param sourceTopologyCommitmentUtilizationStore The source topology commitment utilization store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for source topology commitment utilization store.
     */
    @Nonnull
    @Bean
    public DiagnosableDataStoreCollector sourceTopologyCommitmentUtilizationStoreDiagnosable(
            @Nonnull SingleFieldDataStore<UtilizationInfo, ?> sourceTopologyCommitmentUtilizationStore,
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableDataStoreCollector<>(
                sourceTopologyCommitmentUtilizationStore,
                "in_memory_source_topology_commitment_utilization",
                topologyCommitmentStoreGson,
                UtilizationInfo.class);
    }

    /**
     * The projected topology commitment utilization store.
     * @return The projected topology commitment utilization store.
     */
    @Nonnull
    @Bean
    public SingleFieldDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> projectedTopologyCommitmentUtilizationStore() {
        return new InMemorySingleFieldDataStore<>(new TopologyUtilizationFilterApplicator());
    }

    /**
     * A bean for projected topology commitment utilization store.
     * @param projectedTopologyCommitmentUtilizationStore The projected topology commitment utilization store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for projected topology commitment utilization store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableDataStoreCollector projectedTopologyCommitmentUtilizationStoreDiagnosable(
            @Nonnull SingleFieldDataStore<UtilizationInfo, ?> projectedTopologyCommitmentUtilizationStore,
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableDataStoreCollector<>(
                projectedTopologyCommitmentUtilizationStore,
                "in_memory_projected_topology_commitment_utilization",
                topologyCommitmentStoreGson,
                UtilizationInfo.class);
    }

    /**
     * A bean for topology commitment utilization store.
     * @param sourceTopologyCommitmentUtilizationStore A bean for source topology commitment
     *         utilization store.
     * @param projectedTopologyCommitmentUtilizationStore A bean for projected topology
     *         commitment utilization store.
     * @return A bean for topology commitment utilization store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public SourceProjectedFieldsDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> topologyCommitmentUtilizationStore(
            @Nonnull final SingleFieldDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> sourceTopologyCommitmentUtilizationStore,
            @Nonnull final SingleFieldDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> projectedTopologyCommitmentUtilizationStore) {
        return new InMemorySourceProjectedFieldsDataStore<>(
                sourceTopologyCommitmentUtilizationStore,
                projectedTopologyCommitmentUtilizationStore);
    }

    /**
     * The source topology commitment coverage store.
     * @return The source topology commitment coverage store.
     */
    @Nonnull
    @Bean
    public SingleFieldDataStore<CoverageInfo, TopologyEntityCoverageFilter> sourceTopologyCommitmentCoverageStore() {
        return new InMemorySingleFieldDataStore<>(new TopologyCoverageFilterApplicator());
    }

    /**
     * A bean for source topology commitment coverage store.
     * @param sourceTopologyCommitmentCoverageStore The source topology commitment coverage store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for source topology commitment coverage store.
     */
    @Nonnull
    @Bean
    public DiagnosableDataStoreCollector sourceTopologyCommitmentCoverageStoreDiagnosable(
            @Nonnull SingleFieldDataStore<CoverageInfo, ?> sourceTopologyCommitmentCoverageStore,
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableDataStoreCollector<>(
                sourceTopologyCommitmentCoverageStore,
                "in_memory_source_topology_commitment_coverage",
                topologyCommitmentStoreGson,
                CoverageInfo.class);
    }

    /**
     * The projected topology commitment coverage store.
     * @return The projected topology commitment coverage store.
     */
    @Nonnull
    @Bean
    public SingleFieldDataStore<CoverageInfo, TopologyEntityCoverageFilter> projectedTopologyCommitmentCoverageStore() {
        return new InMemorySingleFieldDataStore<>(new TopologyCoverageFilterApplicator());
    }

    /**
     * A bean for projected topology commitment coverage store.
     * @param projectedTopologyCommitmentCoverageStore The projected topology commitment coverage store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for projected topology commitment coverage store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableDataStoreCollector projectedTopologyCommitmentCoverageStoreDiagnosable(
            @Nonnull SingleFieldDataStore<CoverageInfo, ?> projectedTopologyCommitmentCoverageStore,
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableDataStoreCollector<>(
                projectedTopologyCommitmentCoverageStore,
                "in_memory_projected_topology_commitment_coverage",
                topologyCommitmentStoreGson,
                CoverageInfo.class);
    }

    /**
     * A bean for topology commitment coverage store.
     * @param sourceTopologyCommitmentCoverageStore A bean for source topology commitment
     *         coverage store.
     * @param projectedTopologyCommitmentCoverageStore A bean for projected topology
     *         commitment coverage store.
     * @return A bean for topology commitment utilization store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public SourceProjectedFieldsDataStore<CoverageInfo, TopologyEntityCoverageFilter> topologyCommitmentCoverageStore(
            @Nonnull final SingleFieldDataStore<CoverageInfo, TopologyEntityCoverageFilter> sourceTopologyCommitmentCoverageStore,
            @Nonnull final SingleFieldDataStore<CoverageInfo, TopologyEntityCoverageFilter> projectedTopologyCommitmentCoverageStore) {
        return new InMemorySourceProjectedFieldsDataStore<>(
                sourceTopologyCommitmentCoverageStore,
                projectedTopologyCommitmentCoverageStore);
    }

    /**
     * The source topology commitment mapping store.
     * @return The source topology commitment mapping store.
     */
    @Nonnull
    @Bean
    public SingleFieldDataStore<MappingInfo, CommitmentMappingFilter> sourceTopologyCommitmentMappingStore() {
        return new InMemorySingleFieldDataStore<>(new CommitmentMappingFilterApplicator());
    }

    /**
     * A bean for source topology commitment mapping store.
     * @param sourceTopologyCommitmentMappingStore The source topology commitment mapping store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for source topology commitment mapping store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableDataStoreCollector sourceTopologyCommitmentMappingStoreDiagnosable(
            @Nonnull SingleFieldDataStore<MappingInfo, ?> sourceTopologyCommitmentMappingStore,
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableDataStoreCollector<>(
                sourceTopologyCommitmentMappingStore,
                "in_memory_source_topology_commitment_mapping",
                topologyCommitmentStoreGson,
                MappingInfo.class);
    }

    /**
     * The projected topology commitment mapping store.
     * @return The projected topology commitment mapping store.
     */
    @Nonnull
    @Bean
    public SingleFieldDataStore<MappingInfo, CommitmentMappingFilter> projectedTopologyCommitmentMappingStore() {
        return new InMemorySingleFieldDataStore<>(new CommitmentMappingFilterApplicator());
    }

    /**
     * A bean for projected topology commitment mapping store.
     * @param projectedTopologyCommitmentMappingStore The projected topology commitment mapping store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for projected topology commitment mapping store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableDataStoreCollector projectedTopologyCommitmentMappingStoreDiagnosable(
            @Nonnull SingleFieldDataStore<MappingInfo, CommitmentMappingFilter> projectedTopologyCommitmentMappingStore,
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableDataStoreCollector<>(
                projectedTopologyCommitmentMappingStore,
                "in_memory_projected_topology_commitment_mapping",
                topologyCommitmentStoreGson,
                MappingInfo.class);
    }

    /**
     * A bean for topology commitment mapping store.
     * @param sourceTopologyCommitmentMappingStore A bean for source topology commitment
     *         mapping store.
     * @param projectedTopologyCommitmentMappingStore A bean for projected topology
     *         commitment mapping store.
     * @return A bean for topology commitment utilization store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public SourceProjectedFieldsDataStore<MappingInfo, CommitmentMappingFilter> topologyCommitmentMappingStore(
            @Nonnull final SingleFieldDataStore<MappingInfo, CommitmentMappingFilter> sourceTopologyCommitmentMappingStore,
            @Nonnull final SingleFieldDataStore<MappingInfo, CommitmentMappingFilter> projectedTopologyCommitmentMappingStore) {
        return new InMemorySourceProjectedFieldsDataStore<>(
                sourceTopologyCommitmentMappingStore,
                projectedTopologyCommitmentMappingStore);
    }

    /**
     * A bean for {@link CloudCommitmentUploadRpcService}.
     * @return A bean for {@link CloudCommitmentUploadRpcService}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentUploadRpcService cloudCommitmentUploadRpcService() {
        return new CloudCommitmentUploadRpcService(
                cloudCommitmentUtilizationStore(),
                cloudCommitmentCoverageStore());
    }

    /**
     * A bean for {@link CloudCommitmentStatsRpcService}.
     * @param topologyCommitmentCoverageStore A bean for topology commitment coverage store.
     * @param topologyCommitmentUtilizationStore A bean for topology commitment utilization store.
     * @param statsConverter The commitment stats converter.
     * @return A bean for {@link CloudCommitmentStatsRpcService}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentStatsRpcService cloudCommitmentStatsRpcService(
            @Nonnull final SourceProjectedFieldsDataStore<CoverageInfo, TopologyEntityCoverageFilter> topologyCommitmentCoverageStore,
            @Nonnull final SourceProjectedFieldsDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> topologyCommitmentUtilizationStore,
            @Nonnull CloudCommitmentStatsConverter statsConverter) {
        return new CloudCommitmentStatsRpcService(
                cloudCommitmentCoverageStore(),
                cloudCommitmentUtilizationStore(),
                topologyCommitmentCoverageStore,
                topologyCommitmentUtilizationStore,
                statsConverter,
                maxStatRecordsPerChunk);
    }

    /**
     * The cloud commitment stats converter.
     * @return The cloud commitment stats converter.
     */
    @Nonnull
    @Bean
    public CloudCommitmentStatsConverter statsConverter() {
        return new CloudCommitmentStatsConverter(
                new CommitmentCoverageAggregator(),
                new CommitmentUtilizationAggregator());
    }

    /**
     * A bean for {@link CloudCommitmentUploadServiceController}.
     * @return A bean for {@link CloudCommitmentUploadServiceController}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentUploadServiceController cloudCommitmentUploadServiceController() {
        return new CloudCommitmentUploadServiceController(cloudCommitmentUploadRpcService());
    }

    /**
     * A bean for {@link CloudCommitmentStatsServiceController}.
     * @param cloudCommitmentStatsRpcService A bean for {@link CloudCommitmentStatsRpcService}.
     * @return A bean for {@link CloudCommitmentStatsServiceController}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentStatsServiceController cloudCommitmentStatsServiceController(
            @Nonnull final CloudCommitmentStatsRpcService cloudCommitmentStatsRpcService) {
        return new CloudCommitmentStatsServiceController(cloudCommitmentStatsRpcService);
    }
}
