package com.vmturbo.auth.component.widgetset;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.ws.rs.NotAuthorizedException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.jooq.tools.StringUtils;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.component.store.AuthProvider;
import com.vmturbo.auth.component.store.db.tables.records.WidgetsetRecord;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.DeleteWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.TransferWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.UpdateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc;

/**
 * Provide the GRPC entry points for {@link WidgetsetsServiceGrpc} creating, updating, searching,
 * deletion for Widgetsets.
 **/
public class WidgetsetRpcService extends WidgetsetsServiceGrpc.WidgetsetsServiceImplBase {

    private final IWidgetsetStore widgetsetStore;

    private final AuthProvider authProvider;

    private static final String DEFAULT_OWNER_USERID = "?";

    public WidgetsetRpcService(IWidgetsetStore widgetsetStore, AuthProvider authProvider) {
        this.widgetsetStore = widgetsetStore;
        this.authProvider = authProvider;
    }

    @Override
    public void getWidgetsetList(Widgets.GetWidgetsetListRequest request,
                                 StreamObserver<Widgetset> responseObserver) {

        widgetsetStore.search(request.getCategoriesList(), request.getScopeType(),
            getQueryUserOid(), isAdmin())
                .forEachRemaining(widgetsetRecord -> {
                    responseObserver.onNext(fromDbWidgetset(widgetsetRecord));
                });
        responseObserver.onCompleted();
    }

    @Override
    public void getWidgetset(Widgets.GetWidgetsetRequest request,
                             StreamObserver<Widgetset> responseObserver) {
        final Optional<WidgetsetRecord> widgetsetRecordOptional = widgetsetStore.fetch(
            request.getOid(), getQueryUserOid());
        if (widgetsetRecordOptional.isPresent()) {
            responseObserver.onNext(fromDbWidgetset(
                    widgetsetRecordOptional.get()));
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
        WidgetsetRecord newWidgetSetRecord = widgetsetStore.createWidgetSet(
                request.getWidgetsetInfo(), getQueryUserOid());
        responseObserver.onNext(fromDbWidgetset(newWidgetSetRecord));
        responseObserver.onCompleted();
    }

    @Override
    public void updateWidgetset(UpdateWidgetsetRequest request,
                                StreamObserver<Widgetset> responseObserver) {
        final WidgetsetRecord widgetsetRecord = widgetsetStore.update(request.getOid(),
                request.getWidgetsetInfo(), getQueryUserOid());
        responseObserver.onNext(fromDbWidgetset(widgetsetRecord));
        responseObserver.onCompleted();
    }

    @Override
    public void deleteWidgetset(DeleteWidgetsetRequest request,
                                StreamObserver<Widgetset> responseObserver) {
        final Optional<WidgetsetRecord> widgetsetRecordOptional =
                widgetsetStore.delete(request.getOid(), getQueryUserOid());
        if (widgetsetRecordOptional.isPresent()) {
            responseObserver.onNext(fromDbWidgetset(
                    widgetsetRecordOptional.get()));
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Widgetset: " + request.getOid() + " not found.")
                    .asException());
        }
    }

    @Override
    public void transferWidgetset(TransferWidgetsetRequest request,
                                  StreamObserver<Widgetset> responseObserver) {
        final String removedUserid = request.getRemovedUserid();
        try {
            widgetsetStore.transferOwnership(Collections.singleton(Long.parseLong(removedUserid)),
                    getQueryUserOid()).forEachRemaining(widgetsetRecord -> {
                        responseObserver.onNext(fromDbWidgetset(widgetsetRecord));
                    });
            responseObserver.onCompleted();
        } catch (NumberFormatException nfe) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                .withDescription("User id: " + request.getRemovedUserid() + " not found.")
                .asException());
        }
    }

    /**
     * Fetch the Userid for the current request from the JWT created by the sender.
     *
     * @return the Userid String for the requester - must not be empty
     * otherwise
     * @throws NotAuthorizedException if the USER_UUID_KEY in the JWT is empty
     */
    private String getQueryUserid() {
        String queryUseridString = SecurityConstant.USER_ID_CTX_KEY.get();
        if (StringUtils.isEmpty(queryUseridString)) {
            throw new NotAuthorizedException("Invalid USER_ID_CTX_KEY in JWT: " + queryUseridString);
        }
        return queryUseridString;
    }

    /**
     * Fetch the User OID for the current request from the JWT created by the sender. If the
     * id (key USER_UUID_KEY) is not a valid number, throw {@link NotAuthorizedException}
     *
     * @return the Userid String for the requester - must not be a valid long number
     * @throws NotAuthorizedException if the USER_UUID_KEY in the JWT is not a number
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

    private boolean isAdmin() {
        List<String> userRoles = SecurityConstant.USER_ROLES_KEY.get();
        return userRoles.contains(SecurityConstant.ADMINISTRATOR) || userRoles.contains(SecurityConstant.SITE_ADMIN);
    }

    /**
     * Create a {@link Widgets.Widgetset} from the given DB record. The Widgetset protobuf contains
     * the unique ID and owner userid for this widgetset. The WidgetsetInfo protobuf contains
     * the shape and content information about the widgetset.
     *
     * If the query user matches the current call,
     *
     * @param dbWidgetset the DB record for the widgetset
     * @return a Widgetset protobuf initialized from the widgetset DB record plus the owner userid
     */
    private Widgets.Widgetset fromDbWidgetset(WidgetsetRecord dbWidgetset) {
        Widgets.Widgetset.Builder protoWidgetset = Widgets.Widgetset.newBuilder();
        protoWidgetset.setOid(dbWidgetset.getOid());
        long queryUserOid = getQueryUserOid();
        String queryUserid = getQueryUserid();
        if (queryUserOid == dbWidgetset.getOwnerOid()) {
            protoWidgetset.setOwnerUserid(queryUserid);
        } else {
            protoWidgetset.setOwnerUserid(authProvider.findUsername(dbWidgetset.getOwnerOid())
                    .orElse(DEFAULT_OWNER_USERID));
        }
        Widgets.WidgetsetInfo.Builder protoWidgetsetInfo = Widgets.WidgetsetInfo.newBuilder();
        if (dbWidgetset.getDisplayName() != null) {
            protoWidgetsetInfo.setDisplayName(dbWidgetset.getDisplayName());
        }
        if (dbWidgetset.getCategory() != null) {
            protoWidgetsetInfo.setCategory(dbWidgetset.getCategory());
        }
        if (dbWidgetset.getScope() != null) {
            protoWidgetsetInfo.setScope(dbWidgetset.getScope());
        }
        if (dbWidgetset.getScopeType() != null) {
            protoWidgetsetInfo.setScopeType(dbWidgetset.getScopeType());
        }
        if (dbWidgetset.getSharedWithAllUsers() != null) {
            protoWidgetsetInfo.setSharedWithAllUsers(dbWidgetset.getSharedWithAllUsers() != 0);
        }
        if (dbWidgetset.getWidgets() != null) {
            protoWidgetsetInfo.setWidgets(dbWidgetset.getWidgets());
        }
        return protoWidgetset.setInfo(protoWidgetsetInfo.build()).build();
    }
}
