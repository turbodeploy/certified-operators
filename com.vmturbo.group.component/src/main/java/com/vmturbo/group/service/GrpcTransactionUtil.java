package com.vmturbo.group.service;

import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.utils.ThrowingConsumer;
import com.vmturbo.group.service.TransactionProvider.Stores;

/**
 * Class to share functionality related to gRPC processing of transactional operations
 * with different stores. Pay attention at the fact that this class is logging its messages
 * with a logger passed from constructor.
 */
public class GrpcTransactionUtil {

    private final TransactionProvider transactionProvider;
    private final Logger logger;

    /**
     * Constructs transaction util.
     *
     * @param transactionProvider transaction provider to use
     * @param logger logger to use
     */
    public GrpcTransactionUtil(@Nonnull TransactionProvider transactionProvider,
            @Nonnull Logger logger) {
        this.transactionProvider = Objects.requireNonNull(transactionProvider);
        this.logger = Objects.requireNonNull(logger);
    }

    /**
     * Executes gRPC request with stores in transaction.
     *
     * @param responseObserver response observer to report errors to
     * @param storeOperation operation to execute within transaction
     */
    public void executeOperation(@Nonnull StreamObserver<?> responseObserver,
            @Nonnull ThrowingConsumer<Stores, StoreOperationException> storeOperation) {
        try {
            transactionProvider.transaction(stores -> {
                storeOperation.accept(stores);
                return true;
            });
        } catch (StoreOperationException e) {
            logger.error("Failed to perform operation", e);
            responseObserver.onError(
                    e.getStatus().withDescription(e.getLocalizedMessage()).asException());
        } catch (InterruptedException e) {
            logger.error("Thread interrupted while executing operation", e);
            responseObserver.onError(
                    Status.CANCELLED.withDescription("Thread interrupted").asException());
        }
    }
}
