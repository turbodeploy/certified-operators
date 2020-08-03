/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import com.vmturbo.history.db.VmtDbException;

/**
 * {@link RequestBasedReader} reader that will be used to process API requests of a single type.
 *
 * @param <I> type of the API request that will be processed and data from the
 *                 history will be returned.
 * @param <R> type of the records that will be returned by the reader.
 */
public interface RequestBasedReader<I, R> {

    /**
     * Processes the request and creates output through {@link StreamObserver} instance.
     *
     * @param request that will be processed
     * @param responseObserver sends response/error to the client.
     * @throws VmtDbException in case of error while retrieving requested data from
     *                 DB.
     */
    void processRequest(@Nonnull I request, @Nonnull StreamObserver<R> responseObserver)
                    throws VmtDbException;
}
