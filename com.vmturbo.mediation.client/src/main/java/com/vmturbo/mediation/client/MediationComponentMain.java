package com.vmturbo.mediation.client;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.ExecutionStatus;
import com.vmturbo.mediation.common.ProbeConfigurationLoadException;
import com.vmturbo.mediation.common.ProbeProperties;
import com.vmturbo.mediation.common.ProbesConfig;
import com.vmturbo.mediation.common.RemoteComposer;
import com.vmturbo.mediation.common.WorkerLifecycleListener;
import com.vmturbo.mediation.diagnostic.MediationDiagnosticsConfig;

/**
 * Main component of mediation client microservice.
 */
@Configuration("theComponent")
@Import({MediationComponentConfig.class,
         MediationDiagnosticsConfig.class})
@Component
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
    @Value("${chunk.timeout.sec:900}")
    private long chunkTimeoutSec;
    @Value("${mediation.dataSharingMaxSizeKb:1024}")
    private long dataSharingMaxSizeKb;

    @Autowired
    private MediationDiagnosticsConfig diagnosticsConfig;

    private Logger log = LogManager.getLogger();

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(MediationComponentMain.class);
    }

    @Bean
    public RemoteComposer remoteComposer() {
        try {
            return new RemoteComposer(probePropertiesCollection(), config(), lifecycleListener(), threadPool(),
                negotiationTimeoutSec, keepAliveIntervalSec, chunkSendDelay, chunkTimeoutSec, dataSharingMaxSizeKb);
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

    /**
     * Set up an executor service and thread pool for async operations.
     * Initialize the thread name with the component instance id as a prefix to aid
     * with filtering logs.
     *
     * @return a new ExecutorService with thread factory configured.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat(instanceId + "-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    /**
     * Collecting diags from probes and write them the output stream arg
     * that is used for returning the diags.
     *
     * @param diagnosticZip used for the diags to be written on.
     */
    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagsHandler().dump(diagnosticZip);
        log.info("Done collecting diags from {}", instanceId);
    }

}
