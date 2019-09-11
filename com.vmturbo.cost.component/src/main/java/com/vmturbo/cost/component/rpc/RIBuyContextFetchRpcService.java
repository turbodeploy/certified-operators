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

    @Override
    public void getRIBuyContextData(Cost.GetRIBuyContextRequest request, StreamObserver<Cost.riBuyDemandStats> responseObserver) {
        List<Stats.StatSnapshot> historicalContextForRIBuyAction = actionContextRIBuyStore.getHistoricalContextForRIBuyAction(request.getActionId());
        final Cost.riBuyDemandStats response = Cost.riBuyDemandStats.newBuilder()
                .addAllStatSnapshots(historicalContextForRIBuyAction)
                .setSnapshotDate(historicalContextForRIBuyAction.get(0).getSnapshotDate()).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public RIBuyContextFetchRpcService(@Nonnull ActionContextRIBuyStore actionContextRIBuyStore) {
        this.actionContextRIBuyStore = actionContextRIBuyStore;
    }
}
