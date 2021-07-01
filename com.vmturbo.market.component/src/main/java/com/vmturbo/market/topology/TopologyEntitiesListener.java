package com.vmturbo.market.topology;

import static com.vmturbo.matrix.component.external.MatrixInterface.Component.CONSUMER_2_PROVIDER;
import static com.vmturbo.matrix.component.external.MatrixInterface.Component.OVERLAY;
import static com.vmturbo.matrix.component.external.MatrixInterface.Component.UNDERLAY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.opentracing.SpanContext;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.memory.FlyweightTopologyProto;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.market.runner.MarketRunner;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor.
 */
public class TopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LogManager.getLogger();

    private final MarketRunner marketRunner;

    private final LicenseCheckClient licenseCheckClient;

    private Optional<Integer> maxPlacementsOverride;

    private boolean useQuoteCacheDuringSNM;

    private  boolean replayProvisionsForRealTime;

    private final float rightsizeLowerWatermark;

    private final float rightsizeUpperWatermark;

    private final float discountedComputeCostFactor;

    // TODO: we need to make sure that only a single instance of TopologyEntitiesListener
    // be created and used. Using public constructor here can not guarantee it!
    @SuppressWarnings("unused")
    private TopologyEntitiesListener() {
        // private - do not call
        throw new RuntimeException("private constructor called");
    }

    TopologyEntitiesListener(@Nonnull MarketRunner marketRunner,
                             @Nonnull final Optional<Integer> maxPlacementsOverride,
                             final boolean useQuoteCacheDuringSNM,
                             final boolean replayProvisionsForRealTime,
                             final float rightsizeLowerWatermark,
                             final float rightsizeUpperWatermark,
                             final float discountedComputeCostFactor,
                             @Nonnull final LicenseCheckClient licenseCheckClient) {
        this.marketRunner = Objects.requireNonNull(marketRunner);
        this.maxPlacementsOverride = Objects.requireNonNull(maxPlacementsOverride);
        this.useQuoteCacheDuringSNM = useQuoteCacheDuringSNM;
        this.replayProvisionsForRealTime = replayProvisionsForRealTime;
        this.licenseCheckClient = Objects.requireNonNull(licenseCheckClient);
        this.rightsizeLowerWatermark = rightsizeLowerWatermark;
        this.rightsizeUpperWatermark = rightsizeUpperWatermark;
        this.discountedComputeCostFactor = discountedComputeCostFactor;

        maxPlacementsOverride.ifPresent(maxPlacementIterations ->
            logger.info("Overriding max placement iterations to: {}", maxPlacementIterations));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologyNotification(TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<DataSegment> entityIterator,
                                       @Nonnull final SpanContext tracingContext) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        final FlyweightTopologyProto flyweights = new FlyweightTopologyProto();

        // Do not cache {@link TopologyEntityDTO}'s if analysis is already running on a RT topology
        if (marketRunner.isAnalysisRunningForRtTopology(topologyInfo)) {
            drainTopologyEntities(entityIterator, topologyContextId, topologyId);
            return;
        }
        if (licenseCheckClient.isDevFreemium()) {
            drainTopologyEntities(entityIterator, topologyContextId, topologyId);
            logger.error("You are running a developer freemium edition. Analysis is disabled.");
            return;
        }
        final Long2ObjectMap<TopologyEntityDTO> entities = new Long2ObjectOpenHashMap<>();

        Collection<TopologyDTO.TopologyExtension> exts = new ArrayList<>();
        try (TracingScope scope = Tracing.trace("receive_topology", tracingContext)) {
            while (entityIterator.hasNext()) {
                for (DataSegment ds : entityIterator.nextChunk()) {
                    if (ds.hasEntity()) {
                        entities.put(ds.getEntity().getOid(), flyweights.tryDeduplicate(ds.getEntity()));
                    } else if (ds.hasExtension()) {
                        exts.add(ds.getExtension());
                    }
                }
            }
            // Construct the Matrix.
            final MatrixInterface matrix = loadMatrix(exts);
            if (!matrix.isEmpty()) {
                TheMatrix.setInstance(topologyId, matrix);
            }
        } catch (CommunicationException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId + " for " +
                "context " + topologyContextId, e);
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId + " for " +
                "context " + topologyContextId, e);
        }
        if (topologyInfo.hasPlanInfo()) {
            logger.info("{} Scheduling market analysis for {} plan received entities.",
                    TopologyDTOUtil.formatPlanLogPrefix(topologyInfo.getTopologyContextId()),
                    entities.size());
        }

        logger.info("Topology {} {} deduplication results: {}", topologyId, topologyContextId, flyweights);
        marketRunner.scheduleAnalysis(topologyInfo, entities.values(), tracingContext, false, maxPlacementsOverride,
            useQuoteCacheDuringSNM, replayProvisionsForRealTime, rightsizeLowerWatermark,
            rightsizeUpperWatermark, discountedComputeCostFactor);
    }

    /**
     * Reload the matrix.
     * We need that to deserialize the Matrix from the Topology Extensions.
     *
     * @param exts   The extensions.
     * @return The matrix.
     * @throws InterruptedException In case the chunked receiver got closed in mid-step.
     * @throws TimeoutException In case the chunked receiver got stuck in mid-step.
     * @throws CommunicationException if implementation detected some inconsistency of the data
     *                                received
     */
    private MatrixInterface loadMatrix(@Nonnull Collection<TopologyDTO.TopologyExtension> exts)
        throws InterruptedException, TimeoutException, CommunicationException {
        final MatrixInterface matrix = TheMatrix.newInstance();
        final MatrixInterface.Codec importer = matrix.getMatrixImporter();
        MatrixInterface.Component current = null;
        for (TopologyDTO.TopologyExtension e : exts) {
            final TopologyDTO.TopologyExtension.Matrix topoMatrix = e.getMatrix();
            if (topoMatrix.getEdgesCount() > 0) {
                current = switchMatrixComponent(current, OVERLAY, importer);
                topoMatrix.getEdgesList().forEach(importer::next);
            } else if (topoMatrix.getUnderlayCount() > 0) {
                current = switchMatrixComponent(current, UNDERLAY, importer);
                topoMatrix.getUnderlayList().forEach(importer::next);
            } else if (topoMatrix.getConsumerToProviderCount() > 0) {
                current = switchMatrixComponent(current, CONSUMER_2_PROVIDER, importer);
                topoMatrix.getConsumerToProviderList().forEach(importer::next);
            } else {
                throw new IllegalStateException("Unrecognized Matrix component type.");
            }
        }
        importer.finish();
        return matrix;
    }

    /**
     * Switches the importer to a desired component if needed.
     * The components are:
     * <ul>
     *     <li>{@link MatrixInterface.Component#OVERLAY}
     *     <li>{@link MatrixInterface.Component#UNDERLAY}
     *     <li>{@link MatrixInterface.Component#CONSUMER_2_PROVIDER}
     * </ul>
     *
     * @param current  The current component.
     * @param desired  The desired component.
     * @param importer The importer.
     * @return The desired component.
     */
    private MatrixInterface.Component switchMatrixComponent(
        final @Nullable MatrixInterface.Component current,
        final @Nonnull MatrixInterface.Component desired,
        final @Nonnull MatrixInterface.Codec importer) {
        if (current != desired) {
            // Finish previous section if needed.
            if (current != null) {
                importer.finish();
            }
            importer.start(desired);
        }
        return desired;
    }

    private void drainTopologyEntities(@Nonnull final RemoteIterator<DataSegment> entityIterator,
                                       final long topologyContextId,
                                       final long topologyId) {
        try {
            // drain the iterator and return.
            while (entityIterator.hasNext()) {
                entityIterator.nextChunk();
            }
        } catch (CommunicationException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId + " for " +
                "context " + topologyContextId, e);
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId + " for " +
                "context " + topologyContextId, e);
        }
    }
}
