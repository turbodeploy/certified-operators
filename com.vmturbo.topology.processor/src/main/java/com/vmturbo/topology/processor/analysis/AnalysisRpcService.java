package com.vmturbo.topology.processor.analysis;

import java.time.Clock;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.AnalysisDTO;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.QueueCapacityExceededException;

/**
 * See: topology/AnalysisDTO.proto.
 */
public class AnalysisRpcService extends AnalysisServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final Clock clock;

    private final TopologyPipelineExecutorService pipelineExecutorService;

    private final TopologyHandler topologyHandler;

    private final StitchingJournalFactory journalFactory;

    AnalysisRpcService(@Nonnull final TopologyPipelineExecutorService pipelineExecutorService,
                       @Nonnull final TopologyHandler topologyHandler,
                       @Nonnull final IdentityProvider identityProvider,
                       @Nonnull final StitchingJournalFactory journalFactory,
                       @Nonnull final Clock clock) {
        this.pipelineExecutorService = Objects.requireNonNull(pipelineExecutorService);
        this.topologyHandler = topologyHandler;
        this.identityProvider = Objects.requireNonNull(identityProvider);
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

        PlanTopologyInfo.Builder planTopologyInfoBuilder = PlanTopologyInfo.newBuilder()
                .setPlanProjectType(request.getPlanProjectType())
                .setPlanType(request.getPlanType());
        if (request.hasPlanSubType()) {
            planTopologyInfoBuilder.setPlanSubType(request.getPlanSubType());
        }
        PlanTopologyInfo planTopologyInfo = planTopologyInfoBuilder.build();

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(request.getPlanId())
                .setTopologyId(topologyId)
                .setCreationTime(clock.millis())
                .setTopologyType(TopologyType.PLAN)
                .setPlanInfo(planTopologyInfo)
                .addAllAnalysisType(getAnalysisTypes(
                        request.getPlanType(),
                        request.getPlanSubType(),
                        topologyHandler))
                .build();

        try {
            if (request.hasTopologyId()) {
                pipelineExecutorService.queuePlanOverPlanPipeline(request.getTopologyId(), topologyInfo,
                    request.getScenarioChangeList(), request.getPlanScope());
            } else {
                pipelineExecutorService.queuePlanPipeline(topologyInfo,
                    request.getScenarioChangeList(), request.getPlanScope(), journalFactory);
            }

            responseObserver.onNext(StartAnalysisResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (QueueCapacityExceededException e) {
            responseObserver.onError(Status.UNAVAILABLE.withDescription(e.getMessage())
                .asException());
        }
    }

    /**
     * Get the types of analysis which should be run for this plan. MARKET_ANALYSIS is always
     * added. Only run WASTED_FILES if there are related targets added, and do not run it for
     * cloud migration plan.
     *
     * @param planType the type of the plan
     * @param planSubType the sub-type of the plan
     * @param topologyHandler the {@link TopologyHandler} instance
     * @return set of {@link AnalysisType}s
     */
    private Set<AnalysisType> getAnalysisTypes(@Nonnull String planType,
                                               @Nonnull String planSubType,
                                               @Nonnull TopologyHandler topologyHandler) {

        // Run buy RI impact analysis, if this is OCP with RI buy only
        final Set<AnalysisType> analysisTypes = new HashSet<>();
        if (StringConstants.OPTIMIZE_CLOUD_PLAN.equals(planType) &&
                StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY.equals(planSubType)) {
            analysisTypes.add(AnalysisType.BUY_RI_IMPACT_ANALYSIS);
        } else {
            analysisTypes.add(AnalysisType.MARKET_ANALYSIS);
            // do not run wasted files analysis for Cloud Migration plan or if no related targets
            if (!StringConstants.CLOUD_MIGRATION_PLAN.equals(planType) &&
                    topologyHandler.includesWastedFiles()) {
                analysisTypes.add(AnalysisType.WASTED_FILES);
            }
        }

        return analysisTypes;
    }
}
