package com.vmturbo.cost.component.rpc;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.util.List;

public class RIBuyContextFetchRpcService extends RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceImplBase {
    private ActionContextRIBuyStore actionContextRIBuyStore;

    public RIBuyContextFetchRpcService(@Nonnull ActionContextRIBuyStore actionContextRIBuyStore) {
        this.actionContextRIBuyStore = actionContextRIBuyStore;
    }

    @Override
    public void getRIBuyContextData(Cost.GetRIBuyContextRequest request,
                                    StreamObserver<Cost.riBuyDemandStats> responseObserver) {
        List<Stats.StatSnapshot> historicalContextForRIBuyAction = actionContextRIBuyStore
                .getHistoricalContextForRIBuyAction(request.getActionId());
        final Cost.riBuyDemandStats response = Cost.riBuyDemandStats.newBuilder()
                .addAllStatSnapshots(historicalContextForRIBuyAction)
                .setSnapshotDate(historicalContextForRIBuyAction.get(0).getSnapshotDate()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteRIBuyContextData(Cost.DeleteRIBuyContextDataRequest request,
                                       StreamObserver<Cost.DeleteRIBuyContextDataResponse> responseObserver) {
        if (request.hasTopologyContextId()) {
            int rowsDeleted = actionContextRIBuyStore.deleteRIBuyContextData(request.getTopologyContextId());
            boolean deleted = (rowsDeleted > 0) ? true : false;
            Cost.DeleteRIBuyContextDataResponse response = Cost.DeleteRIBuyContextDataResponse
                    .newBuilder().setDeleted(deleted).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}