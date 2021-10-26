package com.vmturbo.group.service;

import java.util.List;
import java.util.Objects;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.DeleteEntityCustomTagRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.DeleteEntityCustomTagResponse;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.DeleteEntityCustomTagsRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.DeleteEntityCustomTagsResponse;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTags;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTagsCreateResponse;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetAllEntityCustomTagsRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetAllEntityCustomTagsResponse;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetEntityCustomTagsRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.GetEntityCustomTagsResponse;
import com.vmturbo.common.protobuf.group.EntityCustomTagsServiceGrpc.EntityCustomTagsServiceImplBase;
import com.vmturbo.common.protobuf.tag.Tag.DeleteTagListRequest;
import com.vmturbo.common.protobuf.tag.Tag.DeleteTagListResponse;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.entitytags.EntityCustomTagsStore;

/**
 * Implementation of user defined entity tags service.
 */
public class EntityCustomTagsRpcService extends EntityCustomTagsServiceImplBase {

    private final EntityCustomTagsStore entityCustomTagsStore;
    private final Logger logger = LogManager.getLogger();

    /**
     * Construct the user defined entity tags gRPC service.
     * @param entityCustomTagsStoreArg is the store to make transaction with.
     */
    public EntityCustomTagsRpcService(final EntityCustomTagsStore entityCustomTagsStoreArg) {
        entityCustomTagsStore = Objects.requireNonNull(entityCustomTagsStoreArg);
    }

    @Override
    public void createTags(final EntityCustomTagsCreateRequest request,
                             final StreamObserver<EntityCustomTagsCreateResponse> responseObserver) {
        if (!request.hasEntityId()) {
            final String errMsg = "Incoming EntityCustomTags create request does not contain entity id: "
                    + request;
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        if (!request.hasTags()) {
            final String errMsg = "Incoming EntityCustomTags create request does not contain any tags: "
                    + request;
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        final Tags entityCustomTags = request.getTags();

        try {
            entityCustomTagsStore.insertTags(request.getEntityId(), entityCustomTags);
        } catch (StoreOperationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(EntityCustomTagsCreateResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTags(DeleteEntityCustomTagsRequest request,
            StreamObserver<DeleteEntityCustomTagsResponse> responseObserver) {
        if (!request.hasEntityOid()) {
            final String errMsg =
                    "Incoming EntityCustomTags delete request does not contain entity id: "
                            + request;
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }
        try {
            entityCustomTagsStore.deleteTags(request.getEntityOid());
        } catch (StoreOperationException e) {
            logger.error("Could not delete user defined  tags for Entity: '"
                    + request.getEntityOid() + "'");
            responseObserver.onError(e.getStatus().withDescription(e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(DeleteEntityCustomTagsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTag(DeleteEntityCustomTagRequest request,
                          StreamObserver<DeleteEntityCustomTagResponse> responseObserver) {
        if (!request.hasEntityOid()) {
            final String errMsg =
                    "Incoming EntityCustomTags delete request does not contain entity id: "
                            + request;
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }
        if (!request.hasTagKey()) {
            final String errMsg =
                    "Incoming EntityCustomTags delete request does not contain tag key: "
                            + request;
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }
        try {
            int affectedRows = entityCustomTagsStore.deleteTag(request.getEntityOid(),
                    request.getTagKey());

            // We need to fail if no tag was deleted to inform the client that nothing was actually
            // deleted.
            if (affectedRows == 0) {
                final String errMsg = "Could not delete user defined tags for Entity: '"
                        + request.getEntityOid() + "' no such tag Key: '" + request.getTagKey();

                logger.error(errMsg);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
                return;
            }
        } catch (StoreOperationException e) {
            logger.error("Could not delete user defined tag for Entity: '" + request.getEntityOid()
                    + "' and Key: '" + request.getTagKey() + "'");
            responseObserver.onError(e.getStatus().withDescription(e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(DeleteEntityCustomTagResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTagList(DeleteTagListRequest request,
            StreamObserver<DeleteTagListResponse> responseObserver) {
        if (!request.hasOid()) {
            final String errMsg =
                    "Incoming EntityCustomTags delete request does not contain entity id: "
                            + request;
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        int affectedRows;
        try {
            affectedRows = entityCustomTagsStore.deleteTagList(request.getOid(),
                    request.getTagKeyList());
        } catch (StoreOperationException e) {
            logger.error("Could not delete user defined tags for Entity: '" + request.getOid()
                    + "' and Keys: '" + request.getTagKeyList() + "'");
            responseObserver.onError(e.getStatus().withDescription(e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(
                DeleteTagListResponse.newBuilder()
                        .setAffectedRows(affectedRows)
                        .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void getTags(final GetEntityCustomTagsRequest request,
            final StreamObserver<GetEntityCustomTagsResponse> responseObserver) {
        if (!request.hasEntityId()) {
            final String errMsg = "Incoming EntityCustomTags get request does not contain entity id: "
                    + request;
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        Tags tags = entityCustomTagsStore.getTags(request.getEntityId());

        responseObserver.onNext(GetEntityCustomTagsResponse.newBuilder().setTags(tags).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllTags(final GetAllEntityCustomTagsRequest request,
            final StreamObserver<GetAllEntityCustomTagsResponse> responseObserver) {
        List<EntityCustomTags> tags = entityCustomTagsStore.getAllTags();

        GetAllEntityCustomTagsResponse.Builder response = GetAllEntityCustomTagsResponse.newBuilder();
        tags.forEach(
                tag -> response.addEntityCustomTags(tag)
        );

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
}
