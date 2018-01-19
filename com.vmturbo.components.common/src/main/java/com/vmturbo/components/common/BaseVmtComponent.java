package com.vmturbo.components.common;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.DeadlockHealthMonitor;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.components.common.health.MemoryMonitor;
import com.vmturbo.components.common.health.SimpleHealthStatus;
import com.vmturbo.components.common.metrics.ScheduledMetrics;

/**
 * Provide common aspects of all VmtComponent types:
 * <ul>
 * <li>name
 * <li>status
 * </ul>
 * TODO: This will likely be the place to inject a state machine for each component to verify state-transitions
 * e.g. is start() legal, and if so what is the next state?
 **/
@Configuration
@Import({BaseVmtComponentConfig.class})
public abstract class BaseVmtComponent implements IVmtComponent {

    /**
     * The number of seconds to wait for gRPC server to shutdown
     * during the shutdown procedure for the component.
     */
    private static final int GRPC_SHUTDOWN_WAIT_S = 10;

    /**
     * The URL at which to expose Prometheus metrics.
     */
    private static final String METRICS_URL = "/metrics";

    private Logger logger = LogManager.getLogger();

    private ExecutionStatus status = ExecutionStatus.NEW;

    @Value("${component_type:}")
    private String componentType;

    private static final int SCHEDULED_METRICS_DELAY_MS=60000;

    @Value("${scheduledMetricsIntervalMs:60000}")
    private int scheduledMetricsIntervalMs;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @GuardedBy("grpcServerLock")
    private Server grpcServer;

    private final Object grpcServerLock = new Object();

    @Autowired
    BaseVmtComponentConfig baseConfig;

    @Autowired(required = false)
    DiagnosticService diagnosticService;

    /**
     * Embed a component for monitoring dependency/subcomponent health
     */
    private CompositeHealthMonitor healthMonitor = new CompositeHealthMonitor(componentType +" Component");

    /**
     * Constructor for BaseVmtComponent
     */
    public BaseVmtComponent() {
        // install a component ExecutionStatus-based health check into the monitor.
        // This health check will return true if the component is in the RUNNING state and false for
        // all other states.
        getHealthMonitor().addHealthCheck(new HealthStatusProvider() {
            private HealthStatus lastStatus;

            @Override
            public String getName() { return "ExecutionStatus"; }

            @Override
            public HealthStatus getHealthStatus() {
                lastStatus = new SimpleHealthStatus(
                        getComponentStatus().equals(ExecutionStatus.RUNNING),
                        getComponentStatus().name(),
                        lastStatus);
                return lastStatus;
            }
        });
    }

    /**
     * The metrics endpoint that exposes Prometheus metrics on the pre-defined /metrics URL.
     */
    @Bean
    public ServletRegistrationBean metricsServlet() {
        return new ServletRegistrationBean(
            new MetricsServlet(CollectorRegistry.defaultRegistry), METRICS_URL);
    }

    /**
     * Status is only to be set by individual VmtComponent subclasses.
     *
     * @param status the current execution status of the component; initially NEW
     */
    protected void setStatus(ExecutionStatus status) {
        logger.info("Component transition from " + this.status.toString() + " to " + status.toString() +
                " for " + getComponentName());
        this.status = status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExecutionStatus getComponentStatus() {
        return status;
    }

    /**
     * Retrieve the {@link CompositeHealthMonitor} associated with this component.
     *
     * @return the {@link CompositeHealthMonitor instance} embedded in this component
     */
    @Override
    public CompositeHealthMonitor getHealthMonitor() { return healthMonitor; }

    /**
     * Get the component's health information. The component will be considered healthy if all
     * it's dependents are healthy.
     *
     * @return the component's health information
     */
    public HealthStatusProvider getComponentHealth() { return healthMonitor; }

    @Bean
    public CommandLineRunner startup() {
        return args -> startComponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void startComponent() {
        setStatus(ExecutionStatus.STARTING);
        DefaultExports.initialize();
        // start up the default scheduled metrics too
        ScheduledMetrics.initializeScheduledMetrics(scheduledMetricsIntervalMs,SCHEDULED_METRICS_DELAY_MS);
        // add the additional health checks
        logger.info("Adding memory and deadlock health checks");
        getHealthMonitor().addHealthCheck(baseConfig.deadlockHealthMonitor());
        getHealthMonitor().addHealthCheck(baseConfig.memoryMonitor());
        startGrpc();
        onStartComponent();
        setStatus(ExecutionStatus.RUNNING);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public final void stopComponent() {
        setStatus(ExecutionStatus.STOPPING);
        onStopComponent();
        stopGrpc();
        setStatus(ExecutionStatus.TERMINATED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void pauseComponent() {
        onPauseComponent();
        setStatus(ExecutionStatus.PAUSED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void resumeComponent() {
        onResumeComponent();
        setStatus(ExecutionStatus.RUNNING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void failedComponent() {
        onFailedComponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void configurationPropertiesChanged(@Nonnull final Environment environment,
                                               @Nonnull final Set<String> changedPropertyKeys) {
        onConfigurationPropertiesChanged(environment, changedPropertyKeys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void dumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        onDumpDiags(diagnosticZip);

        // diagnosticService must be injected by the particular component. Check that happened correctly.
        if (diagnosticService == null) {
            throw new RuntimeException("DiagnosticService missing");
        }
        diagnosticService.dumpDiags(diagnosticZip);
    }

    // START: Methods to allow component implementations to hook into
    // the component lifecycle. BaseVmtComponent should NOT have any
    // code here. These methods are effectively abstract, but made non-abstract
    // for convenience.
    protected void onStartComponent() {}

    protected void onStopComponent() {}

    protected void onPauseComponent() {}

    protected void onResumeComponent() {}

    protected void onFailedComponent() {}

    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {}

    protected void onConfigurationPropertiesChanged(@Nonnull final Environment environment,
                                                    @Nonnull Set<String> changedPropertyKeys) {}

    /**
     * Components must override this method if they want to use gRPC, and use the
     * provided builder to create a {@link Server}.
     *
     * @param builder An empty builder for the server.
     * @return An optional containing the gRPC server to use, or an empty optional if
     *         gRPC is not desired for this component.
     */
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        return Optional.empty();
    }
    // END: Methods to allow component implementatinos to hook into
    // the component lifecycle.

    private void startGrpc() {
        synchronized (grpcServerLock) {
            final ServerBuilder serverBuilder = ServerBuilder.forPort(grpcPort);
            final Optional<Server> builtServer = buildGrpcServer(serverBuilder);
            if (builtServer.isPresent()) {
                grpcServer = serverBuilder.build();
                try {
                    grpcServer.start();
                    logger.info("Initialized gRPC server on port {}.", grpcPort);
                } catch (IOException e) {
                    logger.error("Failed to start gRPC server. gRPC methods will not be available!", e);
                    stopGrpc();
                }
            } else {
                logger.info("Not initializing gRPC server for {}", getComponentName());
            }
        }
    }

    private void stopGrpc() {
        synchronized (grpcServerLock) {
            if (grpcServer != null) {
                grpcServer.shutdownNow();
                try {
                    if (grpcServer.awaitTermination(GRPC_SHUTDOWN_WAIT_S, TimeUnit.SECONDS)) {
                        logger.info("gRPC server successfully stopped.");
                    } else {
                        logger.error("gRPC server failed to stop after {} seconds!",
                                GRPC_SHUTDOWN_WAIT_S);
                    }
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for gRPC server to stop. " +
                                    "gRPC server is: {}",
                            grpcServer.isTerminated() ? "stopped" : "running");
                }
                grpcServer = null;
            }
        }
    }

}
