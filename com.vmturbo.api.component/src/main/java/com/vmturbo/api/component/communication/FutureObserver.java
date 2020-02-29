package com.vmturbo.api.component.communication;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * gRPC result observer resulting in {@link Future} object for asynchronous executions of several
 * calls in parallel.
 *
 * @param <S> source message that is received in observer
 * @param <R> result type that future will return
 */
public abstract class FutureObserver<S, R> implements StreamObserver<S> {
    private final CompletableFuture<R> future = new CompletableFuture<>();

    @Override
    public void onError(Throwable t) {
        future.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
        future.complete(createResult());
    }

    /**
     * Creates a result that should be later put into a future.
     *
     * @return result of execution (successful)
     */
    @Nonnull
    protected abstract R createResult();

    /**
     * Returns a future holding/waiting for result.
     *
     * @return future
     */
    @Nonnull
    public Future<R> getFuture() {
        return future;
    }

    /**
     * Return gRPC exception status, if it is a gRPC exception.
     *
     * @param exception exception to check
     * @return status or {@link Optional#empty()} if it is not a gRPC exception
     */
    @Nonnull
    protected Optional<Status> getStatus(@Nonnull Throwable exception) {
        if (exception instanceof StatusException) {
            return Optional.of(((StatusException)exception).getStatus());
        } else if (exception instanceof StatusRuntimeException) {
            return Optional.of(((StatusRuntimeException)exception).getStatus());
        } else {
            return Optional.empty();
        }
    }
}
