package com.vmturbo.topology.processor.analysis;

import java.time.Clock;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.AnalysisDTO;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineFactory;

/**
 * See: topology/AnalysisDTO.proto.
 */
public class AnalysisRpcService extends AnalysisServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final Clock clock;

    private TopologyPipelineFactory topologyPipelineFactory;

    private final TopologyHandler topologyHandler;

    private EntityStore entityStore;

    private final StitchingJournalFactory journalFactory;

    public AnalysisRpcService(@Nonnull final TopologyPipelineFactory topologyPipelineFactory,
                              @Nonnull final TopologyHandler topologyHandler,
                              @Nonnull final IdentityProvider identityProvider,
                              @Nonnull final EntityStore entityStore,
                              @Nonnull final StitchingJournalFactory journalFactory,
                              @Nonnull final Clock clock) {
        this.topologyPipelineFactory = Objects.requireNonNull(topologyPipelineFactory);
        this.topologyHandler = topologyHandler;
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.journalFactory = Objects.requireNonNull(journalFactory);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public void startAnalysis(AnalysisDTO.StartAnalysisRequest request,
                              StreamObserver<AnalysisDTO.StartAnalysisResponse> responseObserver) {
        final long topologyId;
        if (!request.hasTopologyId()) {
            logger.info("Received analysis request for the real-time topology.");
            // We need to assign a new topology ID to this latest topology.
            topologyId = identityProvider.generateTopologyId();
        } else {
            topologyId = request.getTopologyId();
            logger.info("Received analysis request for projected topology {}", topologyId);
        }

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(request.getPlanId())
                .setTopologyId(topologyId)
                .setCreationTime(clock.millis())
                .setTopologyType(TopologyType.PLAN)
                .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(request.getPlanProjectType())
                    .setPlanType(request.getPlanType()))
                .addAllAnalysisType(getAnalysisTypes(request.getPlanType(), topologyHandler))
                .build();

        try {
            final TopologyBroadcastInfo broadcastInfo;

            if (request.hasTopologyId()) {
                broadcastInfo = topologyPipelineFactory.planOverOldTopology(topologyInfo,
                        request.getScenarioChangeList(), request.getPlanScope())
                    .run(request.getTopologyId());
            } else {
                broadcastInfo = topologyPipelineFactory.planOverLiveTopology(topologyInfo,
                        request.getScenarioChangeList(), request.getPlanScope(), journalFactory)
                    .run(entityStore);
            }
            responseObserver.onNext(StartAnalysisResponse.newBuilder()
                    .setEntitiesBroadcast(broadcastInfo.getEntityCount())
                    .setTopologyId(broadcastInfo.getTopologyId())
                    .setTopologyContextId(broadcastInfo.getTopologyContextId())
                    .build());
            responseObserver.onCompleted();
        } catch (TopologyPipelineException e) {
            logger.error("Analysis failed with pipeline exception.", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Analysis interrupted: {}", e.getMessage());
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Get the types of analysis which should be run for this plan. MARKET_ANALYSIS is always
     * added. Only run WASTED_FILES if there are related targets added, and do not run it for
     * cloud migration plan.
     *
     * @param planType the type of the plan
     * @param topologyHandler the {@link TopologyHandler} instance
     * @return set of {@link AnalysisType}s
     */
    private Set<AnalysisType> getAnalysisTypes(@Nonnull String planType,
                                               @Nonnull TopologyHandler topologyHandler) {
        final Set<AnalysisType> analysisTypes = new HashSet<>();
        analysisTypes.add(AnalysisType.MARKET_ANALYSIS);
        // do not run wasted files analysis for Cloud Migration plan or if no related targets
        if (!StringConstants.CLOUD_MIGRATION_PLAN_TYPE.equals(planType) &&
                topologyHandler.includesWastedFiles()) {
            analysisTypes.add(AnalysisType.WASTED_FILES);
        }
        return analysisTypes;
    }
}
