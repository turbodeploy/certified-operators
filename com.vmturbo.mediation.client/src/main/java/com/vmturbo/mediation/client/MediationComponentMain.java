package com.vmturbo.mediation.client;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.ExecutionStatus;
import com.vmturbo.mediation.common.ProbeConfigurationLoadException;
import com.vmturbo.mediation.common.ProbeProperties;
import com.vmturbo.mediation.common.WorkerLifecycleListener;

/**
 * Main component of mediation client microservice.
 *
 * @param <A> type of account values to use with this probe.
 */
@Configuration("theComponent")
@EnableDiscoveryClient
@Import({MediationComponentConfig.class})
public class MediationComponentMain<A> extends BaseVmtComponent {

    @Value("${probe-directory:probe-jars}")
    private File probeDirectory;
    @Value("${keep-alive-interval-sec:5}")
    private long keepAliveIntervalSec;
    @Value("${instance_id}")
    private String instanceId;
    @Value("${negotiation.timeout.sec:30}")
    private long negotiationTimeoutSec;
    @Value("${chunk.send.delay.msec:50}")
    private long chunkSendDelay;

    private Logger log = LogManager.getLogger();

    public static void main(String[] args) {
        startContext(MediationComponentMain.class);
    }

    @Bean
    public MediationWorker mediationWorker() {
        try {
            return new MediationWorker(config(), probeProperties(), lifecycleListener(),
                    threadPool(), negotiationTimeoutSec, keepAliveIntervalSec,
                    chunkSendDelay);
        } catch (ProbeConfigurationLoadException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public ProbeProperties<A> probeProperties() throws ProbeConfigurationLoadException {
        return ProbeProperties.parse(probeDirectory);
    }

    @Bean
    public MediationComponentConfig config() {
        return new MediationComponentConfig();
    }

    @Bean
    public WorkerLifecycleListener lifecycleListener() {
        return new WorkerLifecycleListener() {

            @Override
            public void onStopping() {
                setStatus(ExecutionStatus.STOPPING);
            }

            @Override
            public void onStopped() {
                setStatus(ExecutionStatus.TERMINATED);
            }

            @Override
            public void onRunning() {
                setStatus(ExecutionStatus.RUNNING);
            }

            @Override
            public void onStarting() {
                setStatus(ExecutionStatus.STARTING);
            }
        };
    }

    @Override
    public String getComponentName() {
        try {
            return probeProperties().getProbeType();
        } catch (ProbeConfigurationLoadException e) {
            log.error("Unable to get probe properties. Returning default component name", e);
            return "Unknown";
        }
    }

    @Override
    protected void onStartComponent() {
        super.onStartComponent();
        mediationWorker().startMediationHandshake();
    }

    @Override
    public void onStopComponent() {
        mediationWorker().close();
    }

    @Override
    public void onFailedComponent() {
        mediationWorker().close();
        super.onStopComponent();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat(instanceId + "-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
}
