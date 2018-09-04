package com.vmturbo.cost.component.rpc;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceImplBase;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.sql.utils.DbException;

/**
 * Implements the RPC calls supported by the cost component for retrieving Cloud cost data.
 */
public class CostRpcService extends CostServiceImplBase {

    private static final String ERR_MSG = "Invalid discount deletion input: No associated account ID specified";
    private static final String NO_ASSOCIATED_ACCOUNT_ID_OR_DISCOUNT_INFO_PRESENT = "No associated account id or discount info present.";
    private static final String INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE = "Invalid arguments for discount update";
    private static final String CREATING_A_DISCOUNT_WITH_ASSOCIATED_ACCOUNT_ID = "Creating a discount: {} with associated account id: {}";
    private static final String DELETING_A_DISCOUNT = "Deleting a discount: {}";
    private static final String UPDATING_A_DISCOUNT = "Updating a discount: {}";
    private static final String FAILED_TO_UPDATE_DISCOUNT = "Failed to update discount ";
    private static final String FAILED_TO_FIND_THE_UPDATED_DISCOUNT = "Failed to find the updated discount";
    private static final Logger logger = LogManager.getLogger();
    private final DiscountStore discountStore;

    /**
     * Create a new CostRpcService.
     *
     * @param discountStore The store containing account discounts
     */
    public CostRpcService(@Nonnull final DiscountStore discountStore) {
        this.discountStore = Objects.requireNonNull(discountStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createDiscount(DiscountRequest request,
                               StreamObserver<CreateDiscountResponse> responseObserver) {
        logger.info(CREATING_A_DISCOUNT_WITH_ASSOCIATED_ACCOUNT_ID,
                request.getDiscountInfo(), request.getId());
        if (!request.hasId() || !request.hasDiscountInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(NO_ASSOCIATED_ACCOUNT_ID_OR_DISCOUNT_INFO_PRESENT).asRuntimeException());
            return;
        }
        try {
            final Discount discount = discountStore.persistDiscount(request.getId(), request.getDiscountInfo());
            responseObserver.onNext(CreateDiscountResponse.newBuilder()
                    .setDiscount(discount)
                    .build());
            responseObserver.onCompleted();
        } catch (DuplicateAccountIdException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getMessage())
                    .asException());
        } catch (DbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDiscount(DeleteDiscountRequest request,
                               StreamObserver<DeleteDiscountResponse> responseObserver) {
        if (!request.hasId()) {
            logger.error(ERR_MSG);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(ERR_MSG).asRuntimeException());
            return;
        }
        final long id = request.getId();

        logger.info(DELETING_A_DISCOUNT, id);
        try {
            discountStore.deleteDiscountByDiscountId(id);
            responseObserver.onNext(DeleteDiscountResponse.newBuilder()
                    .setDeleted(true)
                    .build());
            responseObserver.onCompleted();
        } catch (DiscountNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage())
                    .asException());
        } catch (DbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getDiscounts(GetDiscountRequest request,
                             StreamObserver<Discount> responseObserver) {
        try {
            if (request.hasFilter()) {
                request.getFilter().getAssociatedAccountIdList().forEach(id ->
                {
                    try {
                        discountStore.getDiscountByAssociatedAccountId(id)
                                .stream().forEach(responseObserver::onNext);
                    } catch (DbException e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                discountStore.getAllDiscount().forEach(responseObserver::onNext);
            }
            responseObserver.onCompleted();
        } catch (DbException | RuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDiscount(DiscountRequest request,
                               StreamObserver<UpdateDiscountResponse> responseObserver) {
        if (!request.hasId() || !request.hasDiscountInfo()) {
            logger.error(INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE).asRuntimeException());
            return;
        }
        logger.info(UPDATING_A_DISCOUNT, request);
        try {
            discountStore.updateDiscount(request.getId(), request.getDiscountInfo());
            Discount discount = discountStore.getDiscountByDiscountId(request.getId()).stream()
                    .findFirst().orElseThrow(() -> new DbException(FAILED_TO_FIND_THE_UPDATED_DISCOUNT));
            final UpdateDiscountResponse res = UpdateDiscountResponse.newBuilder()
                    .setUpdatedDiscount(discount)
                    .build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        } catch (DiscountNotFoundException e) {
            logger.error(FAILED_TO_UPDATE_DISCOUNT + request.getId(), e);
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        } catch (DbException e) {
            logger.error(FAILED_TO_UPDATE_DISCOUNT + request.getId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
        }
    }
}
