package com.vmturbo.plan.orchestrator.market;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.market.component.api.ProjectedTopologyListener;

/**
 * Listens to projected topology notifications from the market component. We do this to speed
 * up certain "transient" plans (see {@link PlanDTOUtil#isTransientPlan(TopologyInfo)}. Instead of
 * having the projected topology persisted to disk in Repository/ArangoDB, and then querying it
 * from the plan orchestrator, we listen for the projected topology for those transient plans
 * directly.
 */
public class PlanProjectedTopologyListener implements ProjectedTopologyListener {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The list of registered processors, populated at Spring context startup.
     */
    private final Set<ProjectedTopologyProcessor> registeredProcessors = new HashSet<>();

    /**
     * Add a {@link ProjectedTopologyProcessor}. Intended to be used at startup, for various
     * Spring beans. We use this method instead of constructor injection so that the
     * configuration class containing the {@link PlanProjectedTopologyListener} doesn't have to
     * (transitively) import large amounts of configurations.
     *
     * @param processor The {@link ProjectedTopologyProcessor}.
     */
    public void addProjectedTopologyProcessor(@Nonnull final ProjectedTopologyProcessor processor) {
        logger.info("Adding projected topology processor: {}", processor.getClass().getSimpleName());
        registeredProcessors.add(processor);
    }

    @Override
    public void onProjectedTopologyReceived(final long projectedTopologyId,
                                            @Nonnull final TopologyDTO.TopologyInfo sourceTopologyInfo,
                                            @Nonnull final RemoteIterator<ProjectedTopologyEntity> topology,
                                            @Nonnull final SpanContext tracingContext) {
        final Set<ProjectedTopologyProcessor> appliesTo = registeredProcessors.stream()
            .filter(processor -> processor.appliesTo(sourceTopologyInfo))
            .collect(Collectors.toSet());
        if (appliesTo.isEmpty()) {
            // If no processors apply to this topology, drain the iterator.
            // We don't expect it to be empty, since we haven't read anything from it.
            RemoteIteratorDrain.drainIterator(topology,
                TopologyDTOUtil.getProjectedTopologyLabel(sourceTopologyInfo),
                false);
        } else {
            if (appliesTo.size() > 1) {
                // At the time of this writing we don't expect multiple processors for a particular
                // topology type. In the future we may need that, in which case we need a visitor
                // wrapper around the RemoteIterator (since we can't pass the whole RemoteIterator
                // to two different processors, because only one of them will be able to read it).
                logger.warn("Unexpected - multiple projected topology processors ({}) for projected topology {}. Choosing first!",
                    appliesTo.stream()
                        .map(processor -> processor.getClass().getSimpleName())
                        .collect(Collectors.joining(",")), sourceTopologyInfo);
            }
            final ProjectedTopologyProcessor processor = appliesTo.iterator().next();
            try (TracingScope tracingScope = Tracing.trace("plan_handle_projected_topology", tracingContext)) {
                processor.handleProjectedTopology(projectedTopologyId, sourceTopologyInfo, topology);
            } catch (RuntimeException e) {
                logger.error("Projected topology processing by processor " +
                    processor.getClass().getSimpleName() + " failed!", e);
            } catch (CommunicationException e) {
                logger.error("Failed to read next chunk of topology due to communication error.", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Failed to read next chunk of topology. Interrupted while waiting.", e);
            } catch (TimeoutException e) {
                logger.error("Failed to read next chunk of topology due to timeout.", e);
            } finally {
                // Make sure we drain the iterator to the best of our ability. We expect/hope
                // it's empty.
                RemoteIteratorDrain.drainIterator(topology,
                    TopologyDTOUtil.getProjectedTopologyLabel(sourceTopologyInfo), true);
            }
        }
    }
}
