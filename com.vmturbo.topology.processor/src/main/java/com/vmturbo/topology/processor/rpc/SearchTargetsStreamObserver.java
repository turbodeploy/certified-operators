package com.vmturbo.topology.processor.rpc;

import java.util.Collection;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.Search.SearchTargetsResponse;

/**
 * An implementation of StreamObserver to use when calling TargetSearchRpcService locally.
 */
public class SearchTargetsStreamObserver implements StreamObserver<SearchTargetsResponse> {

    private final Logger logger = LogManager.getLogger();
    private Collection<Long> targetIds;

    @Override
    public void onNext(SearchTargetsResponse searchTargetsResponse) {
        targetIds = searchTargetsResponse.getTargetsList();
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Failed to get target IDs. Error: {}", throwable.toString());
    }

    @Override
    public void onCompleted() {}

    public Collection<Long> getTargetIds() {
        return targetIds;
    }
}
