package com.vmturbo.cost.component.cloud.commitment;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.AbstractMessage;

import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.vmturbo.cloud.common.stat.CloudGranularityCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentStatsServiceController;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentUploadServiceController;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.GsonAdaptersCoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.SQLCloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.mapping.GsonAdaptersMappingInfo;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.GsonAdaptersUtilizationInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.SQLCloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.DiagnosableSingleFieldDataStore;
import com.vmturbo.cost.component.stores.InMemorySingleFieldDataStore;
import com.vmturbo.cost.component.stores.InMemorySourceProjectedFieldsDataStore;
import com.vmturbo.cost.component.stores.JsonDiagnosableSingleFieldDataStore;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;
import com.vmturbo.cost.component.stores.SourceProjectedFieldsDataStore;

/**
 * A configuration file for cloud commitment statistics stores (coverage & utilization),
 * as well as the associated RPC classes.
 */
@Configuration
public class CloudCommitmentStatsConfig {

    // Should be defined in CostDBConfig
    @Autowired
    private DSLContext dslContext;

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
        return new SQLCloudCommitmentUtilizationStore(dslContext, cloudGranularityCalculator());
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
        return new SQLCloudCommitmentCoverageStore(dslContext, cloudGranularityCalculator());
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
     * A bean for source topology commitment utilization store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for source topology commitment utilization store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableSingleFieldDataStore<UtilizationInfo> sourceTopologyCommitmentUtilizationStore(
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableSingleFieldDataStore<>(new InMemorySingleFieldDataStore<>(),
                "in_memory_source_topology_commitment_utilization", topologyCommitmentStoreGson,
                UtilizationInfo.class);
    }

    /**
     * A bean for projected topology commitment utilization store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for projected topology commitment utilization store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableSingleFieldDataStore<UtilizationInfo> projectedTopologyCommitmentUtilizationStore(
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableSingleFieldDataStore<>(new InMemorySingleFieldDataStore<>(),
                "in_memory_projected_topology_commitment_utilization", topologyCommitmentStoreGson,
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
    public SourceProjectedFieldsDataStore<UtilizationInfo> topologyCommitmentUtilizationStore(
            @Nonnull final SingleFieldDataStore<UtilizationInfo> sourceTopologyCommitmentUtilizationStore,
            @Nonnull final SingleFieldDataStore<UtilizationInfo> projectedTopologyCommitmentUtilizationStore) {
        return new InMemorySourceProjectedFieldsDataStore<>(
                sourceTopologyCommitmentUtilizationStore,
                projectedTopologyCommitmentUtilizationStore);
    }

    /**
     * A bean for source topology commitment coverage store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for source topology commitment coverage store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableSingleFieldDataStore<CoverageInfo> sourceTopologyCommitmentCoverageStore(
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableSingleFieldDataStore<>(new InMemorySingleFieldDataStore<>(),
                "in_memory_source_topology_commitment_coverage", topologyCommitmentStoreGson,
                CoverageInfo.class);
    }

    /**
     * A bean for projected topology commitment coverage store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for projected topology commitment coverage store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableSingleFieldDataStore<CoverageInfo> projectedTopologyCommitmentCoverageStore(
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableSingleFieldDataStore<>(new InMemorySingleFieldDataStore<>(),
                "in_memory_projected_topology_commitment_coverage", topologyCommitmentStoreGson,
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
    public SourceProjectedFieldsDataStore<CoverageInfo> topologyCommitmentCoverageStore(
            @Nonnull final SingleFieldDataStore<CoverageInfo> sourceTopologyCommitmentCoverageStore,
            @Nonnull final SingleFieldDataStore<CoverageInfo> projectedTopologyCommitmentCoverageStore) {
        return new InMemorySourceProjectedFieldsDataStore<>(sourceTopologyCommitmentCoverageStore,
                projectedTopologyCommitmentCoverageStore);
    }

    /**
     * A bean for source topology commitment mapping store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for source topology commitment mapping store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableSingleFieldDataStore<MappingInfo> sourceTopologyCommitmentMappingStore(
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableSingleFieldDataStore<>(new InMemorySingleFieldDataStore<>(),
                "in_memory_source_topology_commitment_mapping", topologyCommitmentStoreGson,
                MappingInfo.class);
    }

    /**
     * A bean for projected topology commitment mapping store.
     * @param topologyCommitmentStoreGson The topology commitment store GSON.
     * @return A bean for projected topology commitment mapping store.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DiagnosableSingleFieldDataStore<MappingInfo> projectedTopologyCommitmentMappingStore(
            @Nonnull final Gson topologyCommitmentStoreGson) {
        return new JsonDiagnosableSingleFieldDataStore<>(new InMemorySingleFieldDataStore<>(),
                "in_memory_projected_topology_commitment_mapping", topologyCommitmentStoreGson,
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
    public SourceProjectedFieldsDataStore<MappingInfo> topologyCommitmentMappingStore(
            @Nonnull final SingleFieldDataStore<MappingInfo> sourceTopologyCommitmentMappingStore,
            @Nonnull final SingleFieldDataStore<MappingInfo> projectedTopologyCommitmentMappingStore) {
        return new InMemorySourceProjectedFieldsDataStore<>(sourceTopologyCommitmentMappingStore,
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
     * @param topologyCommitmentUtilizationStore A bean for topology commitment utilization
     *         store.
     * @return A bean for {@link CloudCommitmentStatsRpcService}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentStatsRpcService cloudCommitmentStatsRpcService(
            @Nonnull final SourceProjectedFieldsDataStore<CoverageInfo> topologyCommitmentCoverageStore,
            @Nonnull final SourceProjectedFieldsDataStore<UtilizationInfo> topologyCommitmentUtilizationStore) {
        return new CloudCommitmentStatsRpcService(cloudCommitmentCoverageStore(),
                cloudCommitmentUtilizationStore(), topologyCommitmentCoverageStore,
                topologyCommitmentUtilizationStore, maxStatRecordsPerChunk);
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
