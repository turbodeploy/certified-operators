package com.vmturbo.action.orchestrator.rpc;

import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionsDebug.GetPipelineEnabledRequest;
import com.vmturbo.common.protobuf.action.ActionsDebug.NewPipelineEnabled;
import com.vmturbo.common.protobuf.action.ActionsDebugServiceGrpc.ActionsDebugServiceImplBase;

/**
 * Debug services for actions. Some methods may be dangerous to allow in
 * production because they could mistakenly introduce incorrect actions
 * or remove important actions recommended by the market.
 *
 * However, it is very valuable to be able to introduce custom action plans
 * in development and test environments because it permits the injection
 * of synthetic actions into the system for testing purposes.
 *
 * Never enable in production.
 * To enable in other environments, start the action orchestrator with the system property
 * -grpc.debug.services.enabled=true. The setting is, by design, not dynamically configurable.
 */
public class ActionsDebugRpcService extends ActionsDebugServiceImplBase {

    private final ActionStorehouse actionStorehouse;

    // TODO: Remove https://vmturbo.atlassian.net/browse/OM-71232
    private final LiveActionPipelineFactory liveActionPipelineFactory;

    /**
     * Create a new ActionsRpcService.
     * @param actionStorehouse The storehouse containing action stores.
     */
    public ActionsDebugRpcService(@Nonnull final ActionStorehouse actionStorehouse,
                                  @Nonnull final LiveActionPipelineFactory liveActionPipelineFactory) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.liveActionPipelineFactory = Objects.requireNonNull(liveActionPipelineFactory);
    }

    @Override
    public void overrideActionPlan(ActionPlan request, StreamObserver<GetActionCountsResponse> responseObserver) {
        try {
            ActionStore actionStore = actionStorehouse.storeActions(request);
            final Stream<ActionView> actionViewStream = actionStore.getActionViews().getAll();

            ActionsRpcService.observeActionCounts(actionViewStream, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to receive action plan. " + e.getMessage())
                .asException());
        } catch (InterruptedException e) {
            responseObserver.onError(
                    Status.CANCELLED.withDescription("Thread interrupted executing the request")
                            .asException());
        }
    }

    /**
     * setNewPipelineEnabled.
     * TODO: Remove https://vmturbo.atlassian.net/browse/OM-71232
     *
     * @param request The request.
     * @param responseObserver The response observer.
     */
    @Override
    public void setNewPipelineEnabled(NewPipelineEnabled request,
                                      StreamObserver<NewPipelineEnabled> responseObserver) {
        liveActionPipelineFactory.setUseNewPipeline(request.getEnabled());
        responseObserver.onNext(request);
        responseObserver.onCompleted();
    }

    /**
     * getNewPipelineEnabled.
     * TODO: Remove https://vmturbo.atlassian.net/browse/OM-71232
     *
     * @param request The request.
     * @param responseObserver The response observer.
     */
    @Override
    public void getNewPipelineEnabled(GetPipelineEnabledRequest request,
                                      StreamObserver<NewPipelineEnabled> responseObserver) {
        responseObserver.onNext(NewPipelineEnabled.newBuilder()
            .setEnabled(liveActionPipelineFactory.getUseNewPipeline()).build());
        responseObserver.onCompleted();
    }
}
