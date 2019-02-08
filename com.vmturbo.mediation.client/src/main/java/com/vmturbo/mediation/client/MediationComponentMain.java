package com.vmturbo.mediation.client;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.ExecutionStatus;
import com.vmturbo.mediation.common.ProbeConfigurationLoadException;
import com.vmturbo.mediation.common.ProbeProperties;
import com.vmturbo.mediation.common.ProbesConfig;
import com.vmturbo.mediation.common.RemoteComposer;
import com.vmturbo.mediation.common.WorkerLifecycleListener;

/**
 * Main component of mediation client microservice.
 */
@Configuration("theComponent")
@Import({MediationComponentConfig.class})
public class MediationComponentMain extends BaseVmtComponent {

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
    public RemoteComposer remoteComposer() {
        try {
            return new RemoteComposer(probePropertiesCollection(), config(), lifecycleListener(), threadPool(),
                negotiationTimeoutSec, keepAliveIntervalSec, chunkSendDelay);
        } catch (ProbeConfigurationLoadException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public Collection<ProbeProperties<?>> probePropertiesCollection() throws ProbeConfigurationLoadException {
        return ProbesConfig.parse(probeDirectory);
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
    protected void onStartComponent() {
        super.onStartComponent();
        remoteComposer().startMediationHandshake();
    }

    @Override
    public void onStopComponent() {
        remoteComposer().close();
    }

    @Override
    public void onFailedComponent() {
        remoteComposer().close();
        super.onStopComponent();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat(instanceId + "-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
}
