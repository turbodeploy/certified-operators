package com.vmturbo.cost.component.cca;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriter;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriterImpl;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.entity.scope.CloudScopeStore;
import com.vmturbo.cost.component.entity.scope.SQLCloudScopeStore;
import com.vmturbo.cost.component.stats.CostStatsConfig;

/**
 * The Cloud Commitment Demand Stats Config class.
 */
@Import({CostDBConfig.class,
        TopologyProcessorListenerConfig.class,
        CostStatsConfig.class})
public class CloudCommitmentAnalysisStoreConfig {

    @Autowired
    private CostDBConfig dbConfig;

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Autowired
    private CostStatsConfig costStatsConfig;

    @Value("${recordCloudAllocationData:true}")
    private boolean recordAllocationData;

    @Value("${statsRecordsCommitBatchSize:5000}")
    private int statsRecordsCommitBatchSize;

    @Value("${statsRecordsUpdateBatchSize:5000}")
    private int statsRecordsBatchUpdateSize;

    @Value("${reservedInstanceStatCleanup.scheduler:360000}")
    private int cleanupSchedulerPeriod;

    @Bean
    public CloudCommitmentDemandWriter cloudCommitmentDemandWriter() {
        return new CloudCommitmentDemandWriterImpl(computeTierAllocationStore(), recordAllocationData);
    }

    @Bean
    public ComputeTierAllocationStore computeTierAllocationStore() {
        return new SQLComputeTierAllocationStore(dbConfig.dsl(), topologyProcessorListenerConfig.liveTopologyInfoTracker(),
                statsRecordsBatchUpdateSize,
                statsRecordsCommitBatchSize);
    }

    @Bean
    public CloudScopeStore cloudScopeStore() {
        return new SQLCloudScopeStore(dbConfig.dsl(), costStatsConfig.taskScheduler(),
                Duration.ofSeconds(cleanupSchedulerPeriod),
                statsRecordsCommitBatchSize);
    }
}
