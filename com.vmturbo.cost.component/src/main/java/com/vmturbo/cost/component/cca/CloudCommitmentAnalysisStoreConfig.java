package com.vmturbo.cost.component.cca;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriter;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriterImpl;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.entity.scope.SQLEntityCloudScopeStore;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.event.library.TopologyEventProvider;

/**
 * The Cloud Commitment Demand Stats Config class.
 */
@Lazy
@Import({DbAccessConfig.class,
        TopologyProcessorListenerConfig.class})
@Configuration
public class CloudCommitmentAnalysisStoreConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

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

    /**
     * Bean for the compute tier allocation store.
     *
     * @return An instance of the ComputeTierAllocationStore.
     */
    @Bean
    public SQLComputeTierAllocationStore computeTierAllocationStore() {
        try {
            return new SQLComputeTierAllocationStore(dbAccessConfig.dsl(),
                    topologyProcessorListenerConfig.liveTopologyInfoTracker(), Executors.newFixedThreadPool(1,
                    new ThreadFactoryBuilder().setNameFormat("ComputeTierAllocation-listener-%d").build()),
                    recordUpdateBatchSize, recordCommitBatchSize, recordFetchBatchSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create SQLComputeTierAllocationStore bean", e);
        }
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
    public SQLEntityCloudScopeStore cloudScopeStore() {
        try {
            return new SQLEntityCloudScopeStore(dbAccessConfig.dsl(), cloudScopeCleanupScheduler(),
                    Duration.ofSeconds(cloudScopeCleanupPeriodSeconds), recordCommitBatchSize,
                    recordFetchBatchSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create SQLCloudScopeStore bean", e);
        }
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
}
