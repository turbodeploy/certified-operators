package com.vmturbo.integrations.intersight;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.integrations.intersight.tasks.SyncTargetsTask;
import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Configuration for interacting with Intersight in the integration component.
 */
@Configuration
@Import({
    TopologyProcessorClientConfig.class,
})
public class IntersightConfig {

    @Value("${intersightHost:bordeaux.default}")
    private String intersightHost;

    @Value("${intersightPort:8443}")
    private int intersightPort;

    @Value("${intersightClientId}")
    private String intersightClientId;

    @Value("${intersightClientSecret}")
    private String intersightClientSecret;

    @Value("${intersightHealthCheckIntervalSeconds:60}")
    private long intersightHealthCheckIntervalSeconds;

    @Value("${intersightTargetsSyncIntervalSeconds:10}")
    private long intersightTargetsSyncIntervalSeconds;

    @Autowired
    private TopologyProcessorClientConfig topologyProcessorClientConfig;

    /**
     * Construct and return a {@link IntersightConnection} to access the Intersight instance.
     *
     * @return a {@link IntersightConnection} to access the Intersight instance
     */
    @Bean
    public IntersightConnection getIntersightConnection() {
        return new IntersightConnection(intersightHost, intersightPort, intersightClientId,
                intersightClientSecret);
    }

    /**
     * Construct and return a {@link IntersightMonitor} to be set up to monitor this Intersight
     * integration component.
     *
     * @return a {@link IntersightMonitor} to monitor this Intersight integration component.
     */
    @Bean
    public IntersightMonitor getIntersightMonitor() {
        return new IntersightMonitor(intersightHealthCheckIntervalSeconds, getIntersightConnection(),
                topologyProcessorClientConfig.topologyProcessorRpcOnly());
    }

    /**
     * A {@link ScheduledExecutorService} to sync targets from Intersight.
     *
     * @return a {@link ScheduledExecutorService} to sync targets from Intersight
     */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService syncTargetScheduler() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "intersight-targets-sync").build();
        return Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    /**
     * Entry point to schedule all integration tasks.
     */
    public void scheduleTasks() {
        syncTargetScheduler().scheduleAtFixedRate(new SyncTargetsTask(getIntersightConnection(),
                        topologyProcessorClientConfig.topologyProcessorRpcOnly()),
                intersightTargetsSyncIntervalSeconds, intersightTargetsSyncIntervalSeconds,
                TimeUnit.SECONDS);
    }
}
