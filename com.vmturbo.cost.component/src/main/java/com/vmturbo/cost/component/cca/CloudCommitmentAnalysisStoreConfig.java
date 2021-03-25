package com.vmturbo.cost.component.cca;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriter;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriterImpl;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.entity.scope.SQLCloudScopeStore;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.topology.event.library.TopologyEventProvider;

/**
 * The Cloud Commitment Demand Stats Config class.
 */
@Lazy
@Import({CostDBConfig.class,
        TopologyProcessorListenerConfig.class})
@Configuration
public class CloudCommitmentAnalysisStoreConfig {

    @Autowired
    private CostDBConfig dbConfig;

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Value("${cca.recordCommitBatchSize:1000}")
    private int recordCommitBatchSize;

    @Value("${cca.recordUpdateBatchSize:1000}")
    private int recordUpdateBatchSize;

    @Value("${cca.recordFetchBatchSize:1000}")
    private int recordFetchBatchSize;

    // Default cleanup is once per day
    @Value("${cca.cloudScopeCleanupPeriodSeconds:86400}")
    private int cloudScopeCleanupPeriodSeconds;

    @Value("${cca.recordCloudAllocationData:true}")
    private boolean recordAllocationData;

    @Autowired
    private TopologyInfoTracker topologyInfoTracker;

    @Value("${maxTrackedLiveTopologies:10}")
    private int maxTrackedLiveTopologies;

    /**
     * Bean for the compute tier allocation store.
     *
     * @return An instance of the ComputeTierAllocationStore.
     */
    @Bean
    public SQLComputeTierAllocationStore computeTierAllocationStore() {
        return new SQLComputeTierAllocationStore(dbConfig.dsl(),
                topologyProcessorListenerConfig.liveTopologyInfoTracker(),
                Executors.newFixedThreadPool(1,
                        new ThreadFactoryBuilder()
                                .setNameFormat("ComputeTierAllocation-listener-%d")
                                .build()),
                recordUpdateBatchSize,
                recordCommitBatchSize,
                recordFetchBatchSize);
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
    public SQLCloudScopeStore cloudScopeStore() {
        return new SQLCloudScopeStore(dbConfig.dsl(), cloudScopeCleanupScheduler(),
                Duration.ofSeconds(cloudScopeCleanupPeriodSeconds),
                recordCommitBatchSize,
                recordFetchBatchSize);
    }

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
     * Bean for the topology event provider.
     * @return An instance of {@link TopologyEventProvider}.
     */
    @Bean
    public TopologyEventProvider topologyEventProvider() {
        return new CCATopologyEventProvider(
                computeTierAllocationStore(),
                cloudScopeStore());
    }

    /**
     * Bean for TopologyInfoTracker.
     * @return The singleton instance of the live {@link TopologyInfoTracker}.
     */
    @Bean
    public TopologyInfoTracker liveTopologyInfoTracker() {
        topologyInfoTracker = new TopologyInfoTracker(
                     TopologyInfoTracker.SUCCESSFUL_REALTIME_TOPOLOGY_SUMMARY_SELECTOR,
                     maxTrackedLiveTopologies);
        return topologyInfoTracker;
    }
}
