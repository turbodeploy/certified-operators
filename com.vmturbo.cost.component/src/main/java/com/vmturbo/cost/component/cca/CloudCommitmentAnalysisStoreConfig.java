package com.vmturbo.cost.component.cca;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

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

/**
 * The Cloud Commitment Demand Stats Config class.
 */
@Import({CostDBConfig.class,
        TopologyProcessorListenerConfig.class,
        ReservedInstanceSpecConfig.class})
public class CloudCommitmentAnalysisStoreConfig {

    @Autowired
    private CostDBConfig dbConfig;

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Autowired
    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    @Value("${cca.recordCloudAllocationData:true}")
    private boolean recordAllocationData;

    @Value("${cca.recordCommitBatchSize:100}")
    private int recordCommitBatchSize;

    @Value("${cca.recordUpdateBatchSize:100}")
    private int recordUpdateBatchSize;

    // Default cleanup is once per day
    @Value("${cca.cloudScopeCleanupPeriodSeconds:86400}")
    private int cloudScopeCleanupPeriodSeconds;

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
                recordUpdateBatchSize,
                recordCommitBatchSize);
    }


    @Bean(destroyMethod = "shutdown")
    protected ThreadPoolTaskScheduler cloudScopeCleanupScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        scheduler.initialize();
        return scheduler;
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("EntityCloudScope-cleanup-%d").build();
    }

    /**
     * Bean for the cloud scope store.
     *
     * @return An instance of the CloudScopeStore.
     */
    @Bean
    public CloudScopeStore cloudScopeStore() {
        return new SQLCloudScopeStore(dbConfig.dsl(),
                cloudScopeCleanupScheduler(),
                Duration.ofSeconds(cloudScopeCleanupPeriodSeconds),
                recordCommitBatchSize);
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
