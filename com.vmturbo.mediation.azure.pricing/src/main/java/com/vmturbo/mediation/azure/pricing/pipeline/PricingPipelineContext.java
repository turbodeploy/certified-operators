package com.vmturbo.mediation.azure.pricing.pipeline;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.PipelineContext;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.mediation.util.target.status.ProbeStageTracker;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * The {@link PricingPipelineContext} is information that's shared by all stages
 * in a pipeline.
 *
 * <p>This is the place to put generic info that applies to many stages, as well as utility objects
 * that have some state which are shared across the stages.
 *
 * <p>The context is immutable, but objects inside it may be mutable.
 *
 * @param <E> The enum used for probe stages for this particular kind of discovery
 */
public class PricingPipelineContext<E extends ProbeStageEnum>
        extends PipelineContext implements AutoCloseable {
    private final Logger logger = LogManager.getLogger();

    private static final String PIPELINE_STAGE_LABEL = "stage";

    private static final DataMetricSummary PIPELINE_STAGE_SUMMARY = DataMetricSummary.builder()
            .withName("pricing_discovery")
            .withHelp("Duration of the individual stages in a pricing discovery.")
            .withLabelNames(/*TopologyPipeline.TOPOLOGY_TYPE_LABEL,*/ PIPELINE_STAGE_LABEL) // TODO
            .build()
            .register();

    private final String accountName;
    private final ProbeStageTracker<E> stageTracker;
    private final Deque<AutoCloseable> closeables = new ArrayDeque<>();

    /**
     * Create a new {@link PricingPipelineContext}.
     *
     * @param accountName an account name to use in log messages
     * @param stageTracker the tracker for probe discovery stages
     */
    public PricingPipelineContext(
            @Nonnull final String accountName,
            @Nonnull ProbeStageTracker stageTracker) {
        this.accountName = Objects.requireNonNull(accountName);
        this.stageTracker = stageTracker;
    }

    @Nonnull
    public String getAccountName() {
        return accountName;
    }

    @Nonnull
    @Override
    public String getPipelineName() {
        return String.format("Pricing Discovery Pipeline (%s)", accountName);
    }

    @Nonnull
    public ProbeStageTracker<E> getStageTracker() {
        return stageTracker;
    }

    @Override
    public DataMetricTimer startStageTimer(String stageName) {
        return PIPELINE_STAGE_SUMMARY.labels(/*getTopologyTypeName(),*/ stageName).startTimer();
    }

    @Override
    public TracingScope startStageTrace(String stageName) {
        return Tracing.trace(stageName);
    }

    /**
     * Arrange for the object to be closed when the context is closed. Objects will
     * be closed reverse order of when autoClose was called (LIFO order).
     *
     * @param closeable an object that should be closed when the context is closed.
     */
    public void autoClose(@Nonnull AutoCloseable closeable) {
        closeables.addFirst(closeable);
    }

    @Override
    public void close() throws Exception {
        Exception firstEx = null;

        while (!closeables.isEmpty()) {
            AutoCloseable closeable = closeables.removeFirst();
            try {
                closeable.close();
            } catch (Exception ex) {
                if (firstEx == null) {
                    firstEx = ex;
                } else {
                    logger.error("Close threw additional error", ex);
                }
            }
        }

        if (firstEx != null) {
            throw firstEx;
        }
    }
}
