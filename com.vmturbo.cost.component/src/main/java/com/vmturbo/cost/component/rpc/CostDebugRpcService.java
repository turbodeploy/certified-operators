package com.vmturbo.cost.component.rpc;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import com.vmturbo.common.protobuf.cost.CostDebug;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.CostDebug.DisableCostRecordingRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.DisableCostRecordingResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.GetRecordedCostsRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.RecordedCost;
import com.vmturbo.common.protobuf.cost.CostDebugServiceGrpc.CostDebugServiceImplBase;
import com.vmturbo.cost.component.topology.CostJournalRecorder;

/**
 * See cost/CostDebug.proto
 */
public class CostDebugRpcService extends CostDebugServiceImplBase {

    private final CostJournalRecorder costJournalRecording;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    public CostDebugRpcService(@Nonnull final CostJournalRecorder costJournalRecording,
                    @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore) {
        this.costJournalRecording = costJournalRecording;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
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
}
