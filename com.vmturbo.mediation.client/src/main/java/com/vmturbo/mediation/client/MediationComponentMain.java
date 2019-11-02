package com.vmturbo.mediation.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.io.FileUtils;
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

    @Value("${diags_tmpdir_path:/tmp/diags}")
    @Nonnull
    private String envTmpDiagsDir;
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

    private Logger log = LogManager.getLogger();
    private int streamBufferSize = (int)FileUtils.ONE_KB;

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(MediationComponentMain.class);
    }

    @VisibleForTesting
    @Nonnull
    protected String getEnvTmpDiagsDir() {
        return envTmpDiagsDir;
    }

    @Bean
    public RemoteComposer remoteComposer() {
        try {
            return new RemoteComposer(probePropertiesCollection(), config(), lifecycleListener(), threadPool(),
                negotiationTimeoutSec, keepAliveIntervalSec, chunkSendDelay, chunkTimeoutSec);
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
        iterateOverAllInPath(diagnosticZip, new File(getEnvTmpDiagsDir()));
        log.info("Done collecting diags from {}", instanceId);
    }

    private void iterateOverAllInPath(final ZipOutputStream diagnosticZip, File path) {
        File[] probeDiagsFiles = path.listFiles();
        if (probeDiagsFiles != null) {
            for (File probeDiagsFile : probeDiagsFiles) {
                if (probeDiagsFile.isDirectory()) {
                    iterateOverAllInPath(diagnosticZip, probeDiagsFile);
                } else {
                    writeDiagsFileToStream(probeDiagsFile.getName(), diagnosticZip);
                }
            }
        }
    }

    private void writeDiagsFileToStream(String diagsFile, ZipOutputStream diagsZip) {
        try {
            diagsZip.putNextEntry(new ZipEntry(diagsFile));
            FileInputStream inputStream =
                new FileInputStream(Paths.get(getEnvTmpDiagsDir(), diagsFile).toString());
            byte[] bytes = new byte[streamBufferSize];
            int length;
            while ((length = inputStream.read(bytes)) >= 0) {
                diagsZip.write(bytes, 0, length);
            }
        } catch (IOException e) {
            log.error("Failed to collect diags {} from {}", diagsFile, instanceId, e);
        }
    }

}
