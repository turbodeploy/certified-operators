package com.vmturbo.components.common.tracing;

import static com.vmturbo.components.common.BaseVmtComponent.PROP_INSTANCE_ID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.Configuration.SenderConfiguration;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.logging.Logging.TracingConfiguration;
import com.vmturbo.components.common.utils.EnvironmentUtils;

/**
 * Responsible for initializing and refreshing the static tracing configuration.
 *
 * <p/>A key requirement for the initial pass of the tracing framework was that we should be able
 * to control the tracing - turn it on or off for specific components and, in the future,
 * for specific subsets of each component. OpenTracing doesn't have an easy interface to let
 * us do this. The typical pattern is to initialize a {@link Tracer} once, and use it for
 * the lifetime of the application.
 *
 * <p/>* To get around the singleton limitation we use a {@link DelegatingTracer}. When a new
 * tracing configuration is requested (via
 * {@link TracingManager#refreshTracingConfiguration(TracingConfiguration)}), the
 * {@link TracingManager} is responsible for creating the appropriate
 * {@link Tracer} and calling {@link DelegatingTracer#updateDelegate(Tracer)}.
 */
public class TracingManager {

    private static final Logger logger = LogManager.getLogger();

    private static final String DEFAULT_JAEGER_ENDPOINT = "http://jaeger-collector:14268";

    private static final TracingConfiguration DEFAULT_CONFIG = TracingConfiguration.newBuilder()
        .setJaegerEndpoint(DEFAULT_JAEGER_ENDPOINT)
        // We start off with no sampling.
        .setSamplingRate(0)
        .build();

    /**
     * The delegating tracer that contains the currently active {@link Tracer} implementation.
     */
    private final DelegatingTracer delegatingTracer;

    private final String serviceName;

    @GuardedBy("this")
    private TracingConfiguration lastTracingConfig = TracingConfiguration.getDefaultInstance();

    private static final TracingManager INSTANCE = new TracingManager();

    private TracingManager() {
        this.delegatingTracer = new DelegatingTracer();
        // The GlobalTracer can only be set once, so we set it to the delegating tracer.
        GlobalTracer.registerIfAbsent(this.delegatingTracer);

        // We should always have this available in the environment.
        this.serviceName = EnvironmentUtils.getOptionalEnvProperty(PROP_INSTANCE_ID)
            .orElse("foo");

        refreshTracingConfiguration(DEFAULT_CONFIG);
    }

    /**
     * Get the instance of {@link TracingManager} shared in this component.
     *
     * @return The {@link TracingManager}.
     */
    @Nonnull
    public static TracingManager get() {
        return INSTANCE;
    }

    @Nonnull
    public synchronized TracingConfiguration getTracingConfiguration() {
        return lastTracingConfig;
    }

    /**
     * Update the tracing configuration in this component to reflect the input
     * {@link TracingConfiguration}.
     *
     * @param newConfig The desired tracing configuration. Any set fields in the input will
     *                  take effect. Any unset fields will retain their current values.
     * @return The new {@link TracingConfiguration} for the component.
     */
    @Nonnull
    public synchronized TracingConfiguration refreshTracingConfiguration(@Nonnull final TracingConfiguration newConfig) {
        final TracingConfiguration.Builder updatedConfigBldr = lastTracingConfig.toBuilder()
            .mergeFrom(newConfig);
        if (updatedConfigBldr.getSamplingRate() > 1) {
            updatedConfigBldr.setSamplingRate(1);
        } else if (updatedConfigBldr.getSamplingRate() < 0) {
            updatedConfigBldr.setSamplingRate(0);
        }

        final TracingConfiguration updatedConfig = updatedConfigBldr.build();

        if (!updatedConfig.equals(lastTracingConfig)) {
            logger.info("Refreshing tracing manager with configuration: {}", newConfig);
            final Tracer newTracer = newJaegerTracer(updatedConfig);
            delegatingTracer.updateDelegate(newTracer);
            logger.info("Successfully refreshed tracing configuration.");

            lastTracingConfig = updatedConfig;
        }
        return lastTracingConfig;
    }

    @Nonnull
    private Tracer newJaegerTracer(@Nonnull final TracingConfiguration tracingConfiguration) {
        final SenderConfiguration senderConfiguration =
            senderConfiguration(tracingConfiguration.getJaegerEndpoint());

        final SamplerConfiguration samplerConfiguration = extractSamplerConfig(tracingConfiguration);

        final ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv()
            // Log the reported spans. This just logs that a span with a certain ID was reported.
            .withLogSpans(true)
            .withSender(senderConfiguration);

        final Configuration jaegerConfig = new Configuration(serviceName)
            .withSampler(samplerConfiguration)
            .withReporter(reporterConfig);

        return jaegerConfig.getTracer();
    }

    @Nonnull
    private SenderConfiguration senderConfiguration(@Nonnull final String endpoint) {
        return SenderConfiguration.fromEnv()
            .withEndpoint(endpoint + "/api/traces");
    }

    @Nonnull
    private SamplerConfiguration extractSamplerConfig(@Nonnull final TracingConfiguration tracingConfiguration) {
        return SamplerConfiguration.fromEnv()
            .withType("probabilistic")
            .withParam(tracingConfiguration.getSamplingRate());
    }
}
