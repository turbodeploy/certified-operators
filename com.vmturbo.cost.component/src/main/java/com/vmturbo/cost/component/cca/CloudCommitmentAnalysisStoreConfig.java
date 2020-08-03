package com.vmturbo.cost.component.cca;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriter;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriterImpl;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecResolver;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.entity.scope.CloudScopeStore;
import com.vmturbo.cost.component.entity.scope.SQLCloudScopeStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.stats.CostStatsConfig;

/**
 * The Cloud Commitment Demand Stats Config class.
 */
@Import({CostDBConfig.class,
        TopologyProcessorListenerConfig.class,
        CostStatsConfig.class,
        ReservedInstanceSpecConfig.class})
public class CloudCommitmentAnalysisStoreConfig {

    @Autowired
    private CostDBConfig dbConfig;

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Autowired
    private CostStatsConfig costStatsConfig;

    @Autowired
    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    @Value("${recordCloudAllocationData:true}")
    private boolean recordAllocationData;

    @Value("${statsRecordsCommitBatchSize:5000}")
    private int statsRecordsCommitBatchSize;

    @Value("${statsRecordsUpdateBatchSize:5000}")
    private int statsRecordsBatchUpdateSize;

    @Value("${reservedInstanceStatCleanup.scheduler:360000}")
    private int cleanupSchedulerPeriod;

    /**
     * Bean for the cloud commitment demand writer.
     *
     * @return An instance of the CloudCommitmentDemandWriter.
     */
    @Bean
    public CloudCommitmentDemandWriter cloudCommitmentDemandWriter() {
        return new CloudCommitmentDemandWriterImpl(computeTierAllocationStore(), recordAllocationData);
    }

    /**
     * Bean for the compute tier allocation store.
     *
     * @return An instance of the ComputeTierAllocationStore.
     */
    @Bean
    public ComputeTierAllocationStore computeTierAllocationStore() {
        return new SQLComputeTierAllocationStore(dbConfig.dsl(), topologyProcessorListenerConfig.liveTopologyInfoTracker(),
                statsRecordsBatchUpdateSize,
                statsRecordsCommitBatchSize);
    }

    /**
     * Bean for the cloud scope store.
     *
     * @return An instance of the CloudScopeStore.
     */
    @Bean
    public CloudScopeStore cloudScopeStore() {
        return new SQLCloudScopeStore(dbConfig.dsl(), costStatsConfig.taskScheduler(),
                Duration.ofSeconds(cleanupSchedulerPeriod),
                statsRecordsCommitBatchSize);
    }

    /**
     * Bean for RI implementation of cloud commitment spec resolver.
     * @return An instance of {@link LocalReservedInstanceSpecResolver}
     */
    @Bean
    public CloudCommitmentSpecResolver<ReservedInstanceSpec> reservedInstanceSpecResolver() {
        return new LocalReservedInstanceSpecResolver(reservedInstanceSpecStore);
    }
}
