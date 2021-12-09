/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.jooq.exception.DataAccessException;

/**
 * {@link RequestBasedReader} reader that will be used to process API requests of a single type.
 *
 * @param <Q> type of the API request that will be processed and data from the
 *                 history will be returned.
 * @param <C> type of the record chunks that will be returned by the reader.
 */
public interface RequestBasedReader<Q, C> {

    /**
     * Processes the request and creates output through {@link StreamObserver} instance.
     *
     * @param request that will be processed
     * @param responseObserver sends response/error to the client.
     * @throws DataAccessException in case of error while retrieving requested data from
     *                 DB.
     */
    void processRequest(@Nonnull Q request, @Nonnull StreamObserver<C> responseObserver)
                    throws DataAccessException;
}
