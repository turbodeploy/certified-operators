package com.vmturbo.components.common.tracing;

import static com.vmturbo.components.common.BaseVmtComponent.PROP_INSTANCE_ID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.primitives.Doubles;

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
 * <p/>
 * Disable the AlwaysOnTracer via ALWAYS_ON_TRACER_ENABLED environment flag.
 */
public class TracingManager {

    private static final Logger logger = LogManager.getLogger();

    private static final String DEFAULT_JAEGER_ENDPOINT = EnvironmentUtils
        .getOptionalEnvProperty("JAEGER_ENDPOINT")
        .orElse("http://jaeger-collector:14268");

    private static final boolean ALWAYS_ON_TRACER_ENABLED = isAlwaysOnTracerEnabled();

    private static final TracingConfiguration DEFAULT_CONFIG = TracingConfiguration.newBuilder()
        .setJaegerEndpoint(DEFAULT_JAEGER_ENDPOINT)
        // We start off with no sampling.
        .setProbabilisticSamplingRate(0)
        .setLogSpans(false)
        .build();

    /**
     * The delegating tracer that contains the currently active {@link Tracer} implementation.
     */
    private final DelegatingTracer delegatingTracer;

    /**
     * The delegating tracer that contains the currently active always-on {@link Tracer} implementation.
     */
    private final DelegatingTracer alwaysOnTracer;

    private final String serviceName;

    @GuardedBy("this")
    private TracingConfiguration lastTracingConfig = TracingConfiguration.getDefaultInstance();

    private static final TracingManager INSTANCE = new TracingManager();

    private TracingManager() {
        this.delegatingTracer = new DelegatingTracer();
        this.alwaysOnTracer = new DelegatingTracer();
        // The GlobalTracer can only be set once, so we set it to the delegating tracer.
        GlobalTracer.registerIfAbsent(this.delegatingTracer);

        // We should always have this available in the environment.
        this.serviceName = EnvironmentUtils.getOptionalEnvProperty(PROP_INSTANCE_ID)
            .orElse("turbo");

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
     * {@see Tracing#trace(String, Tracer)}.
     * <p/>
     * Note that if the AlwaysOnTracer is disabled via the ALWAYS_ON_TRACER_ENABLED environment flag,
     * the tracer returned will have the same configuration as the global tracer.
     *
     * @return A tracer configured to always sample every new trace.
     */
    @Nonnull
    public static Tracer alwaysOnTracer() {
        return get().alwaysOnTracer;
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
        if (updatedConfigBldr.hasRateLimitedSamplesPerSecond()) {
            if (updatedConfigBldr.getRateLimitedSamplesPerSecond() < 0) {
                logger.warn("Illegal rate limiting sampling rate {}, setting to 0.",
                    updatedConfigBldr.getRateLimitedSamplesPerSecond());
                updatedConfigBldr.setRateLimitedSamplesPerSecond(0);
            }
        } else if (updatedConfigBldr.hasProbabilisticSamplingRate()) {
            final double probSamplingRate = updatedConfigBldr.getProbabilisticSamplingRate();
            if (probSamplingRate > 1 || probSamplingRate < 0) {
                updatedConfigBldr.setProbabilisticSamplingRate(
                    Doubles.constrainToRange(probSamplingRate, 0, 1));
                logger.warn("Illegal probabilistic sampling rate {}, setting to {}.",
                    probSamplingRate, updatedConfigBldr.getProbabilisticSamplingRate());
            }
        }

        final TracingConfiguration updatedConfig = updatedConfigBldr.build();

        if (!updatedConfig.equals(lastTracingConfig)) {
            logger.info("Refreshing tracing manager with configuration: {}", newConfig);
            final Tracer newTracer = newJaegerTracer(updatedConfig);
            delegatingTracer.updateDelegate(newTracer);
            logger.info("Successfully refreshed global tracing configuration.");

            if (ALWAYS_ON_TRACER_ENABLED) {
                final Tracer newAlwaysOnTracer = newAlwaysOnTracer(updatedConfig);
                alwaysOnTracer.updateDelegate(newAlwaysOnTracer);
                logger.info("Successfully refreshed always-on tracing configuration.");
            } else {
                // If ALWAYS_ON_TRACER is not enabled, delegate to the regular global tracer.
                logger.info("AlwaysOnTracer disabled. Setting AlwaysOnTracer to match global tracer.");
                alwaysOnTracer.updateDelegate(newTracer);
            }

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
            .withLogSpans(tracingConfiguration.getLogSpans())
            .withSender(senderConfiguration);

        final Configuration jaegerConfig = new Configuration(serviceName)
            .withSampler(samplerConfiguration)
            .withReporter(reporterConfig);

        return jaegerConfig.getTracer();
    }

    /**
     * Create a new tracer with a constant sampling rate so that all trace requests result in
     * traces being sampled.
     *
     * @param tracingConfiguration The tracing configuration to use. The sampling part of the configuration
     *                             is ignored and it's forced to use a constant sampling rate so that
     *                             all traces result in actually creating trace spans.
     * @return A new Tracer that is always sampling.
     */
    @Nonnull
    private Tracer newAlwaysOnTracer(@Nonnull final TracingConfiguration tracingConfiguration) {
        final SenderConfiguration senderConfiguration =
            senderConfiguration(tracingConfiguration.getJaegerEndpoint());

        final ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv()
            // Don't log spans for always-on tracer because they can get a bit spammy.
            .withLogSpans(false)
            .withSender(senderConfiguration);

        final Configuration jaegerConfig = new Configuration(serviceName)
            .withSampler(SamplerConfiguration.fromEnv().withType("const").withParam(1.0))
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
        final SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv();
        if (tracingConfiguration.hasRateLimitedSamplesPerSecond()) {
            samplerConfig
                .withType("ratelimiting")
                .withParam(tracingConfiguration.getRateLimitedSamplesPerSecond());
        } else {
            samplerConfig
                .withType("probabilistic")
                .withParam(tracingConfiguration.getProbabilisticSamplingRate());
        }

        return samplerConfig;
    }

    /**
     * Check if the environment flag to control whether the always on tracer is enabled has been set
     * and return its value. If no value is set, return a default.
     *
     * @return Value of the ALWAYS_ON_TRACER_ENABLED flag.
     */
    private static boolean isAlwaysOnTracerEnabled() {
        try {
            return EnvironmentUtils
                .getOptionalEnvProperty("ALWAYS_ON_TRACER_ENABLED")
                .map(Boolean::parseBoolean)
                .orElse(true);
        } catch (Exception e) {
            logger.error("Unable to check ALWAYS_ON_TRACER_ENABLED. Defaulting to false ", e);
            return false;
        }
    }
}
