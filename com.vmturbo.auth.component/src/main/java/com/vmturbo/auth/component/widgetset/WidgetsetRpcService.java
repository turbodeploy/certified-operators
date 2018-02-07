package com.vmturbo.auth.component.widgetset;

import java.util.Optional;

import javax.ws.rs.NotAuthorizedException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.DeleteWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.UpdateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc;

/**
 * Provide the GRPC entry points for {@link WidgetsetsServiceGrpc} creating, updating, searching,
 * deletion for Widgetsets.
 **/
public class WidgetsetRpcService extends WidgetsetsServiceGrpc.WidgetsetsServiceImplBase {

    private final IWidgetsetStore widgetsetStore;

    public WidgetsetRpcService(IWidgetsetStore widgetsetStore) {
        this.widgetsetStore = widgetsetStore;
    }

    @Override
    public void getWidgetsetList(Widgets.GetWidgetsetListRequest request,
                                 StreamObserver<Widgetset> responseObserver) {

        long queryUserOid = getQueryUserOid();
        widgetsetStore.search(queryUserOid, request.getCategoriesList(), request.getScopeType())
                .forEachRemaining(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void getWidgetset(Widgets.GetWidgetsetRequest request,
                             StreamObserver<Widgetset> responseObserver) {
        long queryUserOid = getQueryUserOid();
        final Optional<Widgetset> widgetset = widgetsetStore.fetch(queryUserOid, request.getOid());
        if (widgetset.isPresent()) {
            responseObserver.onNext(widgetset.get());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Widgetset: " + request.getOid() + " not found.")
                    .asException());
        }
    }

    @Override
    public void createWidgetset(Widgets.CreateWidgetsetRequest request,
                                StreamObserver<Widgetset> responseObserver) {
        if (!request.hasWidgetsetInfo()) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Widgetset to create not found.")
                    .asException());
        }
        long queryUserOid = getQueryUserOid();
        Widgetset newWidgetSet = widgetsetStore.createWidgetSet(queryUserOid, request.getWidgetsetInfo());
        responseObserver.onNext(newWidgetSet);
        responseObserver.onCompleted();
    }

    @Override
    public void updateWidgetset(UpdateWidgetsetRequest request,
                                StreamObserver<Widgetset> responseObserver) {
        long queryUserOid = getQueryUserOid();
        responseObserver.onNext(widgetsetStore.update(queryUserOid, request.getOid(),
                request.getWidgetsetInfo()));
        responseObserver.onCompleted();
    }

    @Override
    public void deleteWidgetset(DeleteWidgetsetRequest request,
                                StreamObserver<Widgetset> responseObserver) {
        long queryUserOid = getQueryUserOid();
        final Optional<Widgetset> widgetset = widgetsetStore.delete(queryUserOid, request.getOid());
        if (widgetset.isPresent()) {
            responseObserver.onNext(widgetset.get());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Widgetset: " + request.getOid() + " not found.")
                    .asException());
        }
    }

    /**
     * Fetch the User OID for the current request from the JWT created by the sender. If the
     * id (key USER_UUID) is not a valid number, throw {@link NotAuthorizedException}
     *
     * @return an Optional containing the User OID for the requester, if set, or Optional.empty()
     * otherwise
     * @throws NotAuthorizedException if the USER_UUID_KEY in the JWT cannot be converted to a number
     */
    private long getQueryUserOid() {
        String queryUserOidString = SecurityConstant.USER_UUID_KEY.get();
        final long userOid;
        try {
            userOid = Long.valueOf(queryUserOidString);
        } catch(NumberFormatException e) {
            throw new NotAuthorizedException("Invalid USER_UUID_KEY in JWT: " + queryUserOidString);
        }
        return userOid;
    }

}
