package com.vmturbo.cost.component.rpc;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.CostDebug;
import com.vmturbo.common.protobuf.cost.CostDebug.DisableCostRecordingRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.DisableCostRecordingResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.GetBuyRIImpactCsvRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.GetBuyRIImpactCsvResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.GetRecordedCostsRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.RecordedCost;
import com.vmturbo.common.protobuf.cost.CostDebug.TriggerBuyRIAlgorithmRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.TriggerBuyRIAlgorithmResponse;
import com.vmturbo.common.protobuf.cost.CostDebugServiceGrpc.CostDebugServiceImplBase;
import com.vmturbo.cost.component.reserved.instance.BuyRIImpactReportGenerator;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.cost.component.topology.CostJournalRecorder;

/**
 * See cost/CostDebug.proto
 */
public class CostDebugRpcService extends CostDebugServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final CostJournalRecorder costJournalRecording;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final ReservedInstanceAnalysisInvoker invoker;

    private final BuyRIImpactReportGenerator buyRIImpactReportGenerator;

    public CostDebugRpcService(@Nonnull final CostJournalRecorder costJournalRecording,
                    @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
                    @Nonnull final ReservedInstanceAnalysisInvoker invoker,
                               @Nonnull BuyRIImpactReportGenerator buyRIImpactReportGenerator) {
        this.costJournalRecording = costJournalRecording;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
        this.invoker = invoker;
        this.buyRIImpactReportGenerator = Objects.requireNonNull(buyRIImpactReportGenerator);
    }

    @Override
    public void enableCostRecording(EnableCostRecordingRequest request,
                                  StreamObserver<EnableCostRecordingResponse> responseObserver) {
        this.costJournalRecording.overrideEntitySelector(request);
        responseObserver.onNext(EnableCostRecordingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void disableCostRecording(DisableCostRecordingRequest request,
                                     StreamObserver<DisableCostRecordingResponse> responseObserver) {
        this.costJournalRecording.resetEntitySelector();
        responseObserver.onNext(DisableCostRecordingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getRecordedCosts(GetRecordedCostsRequest request,
                                 StreamObserver<RecordedCost> responseObserver) {

        final Set<Long> targetEntities = new HashSet<>();
        if (request.hasEntityId()) {
            targetEntities.add(request.getEntityId());
        }

        costJournalRecording.getJournalDescriptions(targetEntities)
                .map(cost -> RecordedCost.newBuilder()
                        .setCostJournalDescription(cost)
                        .build())
                .forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void logEntityRIMapping(CostDebug.LogEntityRIMappingRequest request, StreamObserver<CostDebug.LogEntityRIMappingResponse> responseObserver) {
        final List<Long> entityIdList = request.getEntityIdList();
        entityReservedInstanceMappingStore.logEntityCoverage(entityIdList);
        responseObserver.onNext(CostDebug.LogEntityRIMappingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void logRIEntityMapping(CostDebug.LogRIEntityMappingRequest request, StreamObserver<CostDebug.LogRIEntityMappingResponse> responseObserver) {
        final List<Long> riIdList = request.getRiIdList();
        entityReservedInstanceMappingStore.logRICoverage(riIdList);
        responseObserver.onNext(CostDebug.LogRIEntityMappingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     *
     * @param request {@inheritDoc}
     * @param responseObserver {@inheritDoc}
     */
    @Override
    public void triggerBuyRIAlgorithm(TriggerBuyRIAlgorithmRequest request,
                                      StreamObserver<TriggerBuyRIAlgorithmResponse> responseObserver) {
        final StartBuyRIAnalysisRequest startBuyRIAnalysisRequest = invoker.getStartBuyRIAnalysisRequest();
        if (startBuyRIAnalysisRequest.getAccountsList().isEmpty()) {
            logger.info("No BA's found. RI Buy analysis will not be triggered.");
            return;
        }
        invoker.invokeBuyRIAnalysis(startBuyRIAnalysisRequest);
        responseObserver.onNext(TriggerBuyRIAlgorithmResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Provides a report (formatted as CSV) mapping entity -> buy RI coverage and the cost impact from
     * buy RI actions. This report is useful in debugging the projected on-demand compute cost, providing
     * the mappings generated by BuyRIImpactAnalysis in the market component.
     * <p>
     * This response can be converted to a csv file taking the curl command generated by swagger and piping
     * it to jq to extract the csv report string and send it to a file.
     * <p>
     * Ex: curl [...] | jq -r '.response.csvString' > [output_csv_file]
     * @param request {@inheritDoc}
     * @param responseObserver {@inheritDoc}
     */
    @Override
    public void getBuyRIImpactCsv(final GetBuyRIImpactCsvRequest request, final StreamObserver<GetBuyRIImpactCsvResponse> responseObserver) {
        responseObserver.onNext(GetBuyRIImpactCsvResponse.newBuilder()
                .setCsvString(buyRIImpactReportGenerator.generateCsvReportAsString(request))
                .build());
        responseObserver.onCompleted();
    }
}
