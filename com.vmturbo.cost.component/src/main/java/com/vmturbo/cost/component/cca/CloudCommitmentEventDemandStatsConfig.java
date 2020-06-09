package com.vmturbo.cost.component.cca;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.writer.CloudCommitmentDemandWriter;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;

/**
 * The Cloud Commitment Even Demand Stats Config class.
 */
@Import({CostDBConfig.class,
        TopologyProcessorListenerConfig.class})
public class CloudCommitmentEventDemandStatsConfig {

    @Autowired
    private CostDBConfig dbConfig;

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Value("${recordCloudAllocationData:true}")
    private boolean recordAllocationData;

    @Value("${statsRecordsCommitBatchSize:5000}")
    private int statsRecordsCommitBatchSize;

    @Value("${statsRecordsUpdateBatchSize:5000}")
    private int statsRecordsBatchUpdateSize;

    @Bean
    public CloudCommitmentDemandWriter cloudCommitmentDemandWriter() {
        return new CloudCommitmentEventDemandWriterImpl(sqlComputeTierAllocationStore(), recordAllocationData);
    }

    public SQLComputeTierAllocationStore sqlComputeTierAllocationStore() {
        return new SQLComputeTierAllocationStore(dbConfig.dsl(), topologyProcessorListenerConfig.liveTopologyInfoTracker(),
                statsRecordsBatchUpdateSize,
                statsRecordsCommitBatchSize);
    }
}
