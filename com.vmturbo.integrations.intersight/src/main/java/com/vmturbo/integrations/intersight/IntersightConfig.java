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

import com.vmturbo.integrations.intersight.targetsync.IntersightTargetSyncService;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Configuration for interacting with Intersight in the integration component.
 */
@Configuration
@Import({
    TopologyProcessorClientConfig.class, IntersightConnectionConfig.class, IntersightLicenseSyncConfig.class
})
public class IntersightConfig {

    @Autowired
    private IntersightConnectionConfig intersightConnectionConfig;

    @Autowired
    private IntersightLicenseSyncConfig intersightLicenseSyncConfig;

    @Value("${intersightHealthCheckIntervalSeconds:60}")
    private long intersightHealthCheckIntervalSeconds;

    @Value("${intersightTargetSyncIntervalSeconds:20}")
    private long intersightTargetSyncIntervalSeconds;

    /**
     * How long in seconds to hold off target status update since the target is created/modified
     * in Intersight.  This is because the real target credentials sent out of band to the
     * assist and stored as Kubernetes secrets will not show up on the probe container
     * immediately due to the kubelet sync period (default 1m) and cache propagation delay.
     * Premature target status update will give users incorrect target validation results.
     *
     * <p>Strictly speaking on target creation, we will not hold off updating to a
     * validated/connected state, which will give the user a confirmation that the connection is
     * good as soon as the credentials are propagated.  But we will hold off any update of
     * validation failures.
     *
     * <p>For target modification, we will always hold off until this no update period is over,
     * because we can't rule out the possibility that the user may have accidentally made a bad
     * change and immediately reporting a good connection may give a wrong impression.
     */
    @Value("${intersightTargetSyncNoUpdateOnChangePeriodSeconds:180}")
    private long intersightTargetSyncNoUpdateOnChangePeriodSeconds;

    @Autowired
    private TopologyProcessorClientConfig topologyProcessorClientConfig;

    /**
     * Construct and return a {@link IntersightMonitor} to be set up to monitor this Intersight
     * integration component.
     *
     * @return a {@link IntersightMonitor} to monitor this Intersight integration component.
     */
    @Bean
    public IntersightMonitor getIntersightMonitor() {
        return new IntersightMonitor(intersightHealthCheckIntervalSeconds,
                intersightConnectionConfig.getIntersightConnection(),
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
                "intersight-target-sync-service").build();
        return Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    /**
     * Entry point to schedule all integration tasks.
     */
    public void scheduleTasks() {
        syncTargetScheduler().scheduleAtFixedRate(
                new IntersightTargetSyncService(
                        intersightConnectionConfig.getIntersightConnection(),
                        topologyProcessorClientConfig.topologyProcessorRpcOnly(),
                        intersightTargetSyncNoUpdateOnChangePeriodSeconds),
                intersightTargetSyncIntervalSeconds,
                intersightTargetSyncIntervalSeconds,
                TimeUnit.SECONDS);

        intersightLicenseSyncConfig.intersightLicenseSyncService().start();
        intersightLicenseSyncConfig.intersightLicenseCountUpdater().start();
    }
}
