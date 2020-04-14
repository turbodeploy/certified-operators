package com.vmturbo.clustermgr.management;

import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.clustermgr.ClustermgrDBConfig;
import com.vmturbo.component.status.api.ComponentStatusClientConfig;

/**
 * Spring configuration for beans related to component registration and health.
 */
@Configuration
@Import({ClustermgrDBConfig.class, ComponentStatusClientConfig.class})
public class ComponentRegistrationConfig {

    @Autowired
    private ClustermgrDBConfig dbConfig;

    @Autowired
    private ComponentStatusClientConfig componentStatusClientConfig;

    @Value("${healthCheckIntervalSeconds:60}")
    private long healthCheckIntervalSeconds;

    @Value("${healthCheckConnectTimeoutSeconds:10}")
    private long healthCheckConnectTimeout;

    @Value("${healthCheckReadTimeout:10}")
    private long healthCheckReadTimeout;

    /**
     * How long to allow an component instance to be unhealthy before forcefully deregistering it.
     */
    @Value("${unhealthyDeregistrationSeconds:3600}")
    private long unhealthyDeregistrationSeconds;

    /**
     * Registry for components.
     *
     * @return The {@link ComponentRegistry} bean.
     */
    @Bean
    public ComponentRegistry componentRegistry() {
        return new ComponentRegistry(dbConfig.dsl(), Clock.systemUTC(),
            unhealthyDeregistrationSeconds, TimeUnit.SECONDS);
    }

    /**
     * Listener for component notifications which registers/unregisters components.
     *
     * @return The {@link ClustermgrComponentStatusListener} bean.
     */
    @Bean
    public ClustermgrComponentStatusListener componentStatusListener() {
        ClustermgrComponentStatusListener statusListener =
            new ClustermgrComponentStatusListener(componentRegistry());
        componentStatusClientConfig.componentStatusNotificationReceiver().addListener(statusListener);
        return statusListener;
    }

    /**
     * Health checker for registered services.
     *
     * @return The {@link ComponentHealthChecker} bean.
     */
    @Bean
    public ComponentHealthChecker componentHealthChecker() {
        final ThreadFactory scheduleThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("health-check-schedule-%d").build();
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("health-check-%d").build();
        return new ComponentHealthChecker(componentRegistry(),
            Executors.newSingleThreadScheduledExecutor(scheduleThreadFactory),
            // Like a cached threadpool, but limited so 5 threads.
            new ThreadPoolExecutor(0, 5,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), threadFactory),
            healthCheckIntervalSeconds,
            healthCheckConnectTimeout,
            healthCheckReadTimeout,
            TimeUnit.SECONDS);
    }
}
