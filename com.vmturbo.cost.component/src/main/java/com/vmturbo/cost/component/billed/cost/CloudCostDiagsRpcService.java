package com.vmturbo.cost.component.billed.cost;

import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.common.protobuf.cost.CloudCostDiagsServiceGrpc.CloudCostDiagsServiceImplBase;
import com.vmturbo.common.protobuf.cost.CloudCostServices.ExportCloudCostDiagsRequest;
import com.vmturbo.common.protobuf.cost.CloudCostServices.ExportCloudCostDiagsResponse;
import com.vmturbo.cost.component.billedcosts.TagGroupStore;
import com.vmturbo.cost.component.billedcosts.TagStore;

/**
 * Cloud cost diags RPC service for exporting cloud cost and cost tag tables.
 */
public class CloudCostDiagsRpcService extends CloudCostDiagsServiceImplBase {

    private final CloudCostStore cloudCostStore;
    private final TagGroupStore tagGroupStore;
    private final TagStore tagStore;
    private final CloudScopeIdentityStore cloudScopeIdentityStore;

    /**
     * Contructs a new {@link CloudCostDiagsRpcService} instance.
     * @param cloudCostStore The cloud cost (billed cost) store.
     */
    public CloudCostDiagsRpcService(@Nonnull final CloudCostStore cloudCostStore,
            @Nonnull final TagStore tagStore,
            @Nonnull final TagGroupStore tagGroupStore,
            @Nonnull final CloudScopeIdentityStore cloudScopeIdentityStore) {
        this.cloudCostStore = Objects.requireNonNull(cloudCostStore);
        this.tagGroupStore = Objects.requireNonNull(tagGroupStore);
        this.tagStore = Objects.requireNonNull(tagStore);
        this.cloudScopeIdentityStore = Objects.requireNonNull(cloudScopeIdentityStore);
    }

    @Override
    public void exportCloudCostDiags(ExportCloudCostDiagsRequest request,
            StreamObserver<ExportCloudCostDiagsResponse> responseObserver) {
        boolean exportCloudCostDiags = request.getExportCloudCostDiags();
        cloudCostStore.setExportCloudCostDiags(exportCloudCostDiags);
        tagGroupStore.setExportCloudCostDiags(exportCloudCostDiags);
        tagStore.setExportCloudCostDiags(exportCloudCostDiags);
        cloudScopeIdentityStore.setExportCloudCostDiags(exportCloudCostDiags);
        responseObserver.onNext(ExportCloudCostDiagsResponse.newBuilder()
                .setExportCloudCostDiags(exportCloudCostDiags)
                .build());
        responseObserver.onCompleted();
    }
}
