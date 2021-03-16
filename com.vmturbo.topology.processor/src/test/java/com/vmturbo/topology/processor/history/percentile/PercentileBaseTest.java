package com.vmturbo.topology.processor.history.percentile;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.grpc.stub.StreamObserver;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.stats.Stats.GetTimestampsRangeRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetTimestampsRangeResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;

/**
 * Base class for percentile editor-related testing.
 */
public abstract class PercentileBaseTest extends BaseGraphRelatedTest {
    protected StatsHistoryServiceMole history;
    protected GrpcTestServer grpcServer;

    /**
     * Set up the blocking stub with percentile timestamps response rounded and equally spread within the
     * requested period by the window duration.
     *
     * @param windowHours maintenance window duration
     * @return history blocking stub
     * @throws IOException when failed
     */
    protected StatsHistoryServiceBlockingStub setUpBlockingStub(int windowHours) throws IOException {
        long maintenancePeriod = TimeUnit.HOURS.toMillis(windowHours);
        Function<GetTimestampsRangeRequest, List<Long>> stampsGetter = (req) -> {
            List<Long> timestamps = new LinkedList<>();
            for (long stamp = req.getStartTimestamp() / maintenancePeriod
                            * maintenancePeriod; stamp < req.getEndTimestamp() / maintenancePeriod
                                            * maintenancePeriod; stamp += maintenancePeriod) {
                timestamps.add(stamp);
            }
            return timestamps;
        };
        return setUpBlockingStub(stampsGetter);
    }

    /**
     * Set up the blocking stub with percentile timestamps response from the passed list of
     * points.
     *
     * @param stampsGetter supplier of all timestamps available for the request
     * @return history blocking stub
     * @throws IOException when failed
     */
    protected StatsHistoryServiceBlockingStub setUpBlockingStub(
                    Function<GetTimestampsRangeRequest, List<Long>> stampsGetter)
                    throws IOException {
        history = Mockito.spy(new StatsHistoryServiceMole());
        Answer<Void> answerGetStamps = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                GetTimestampsRangeRequest req = invocation.getArgumentAt(0, GetTimestampsRangeRequest.class);
                List<Long> allTimestamps = stampsGetter.apply(req);
                List<Long> timestamps = allTimestamps.stream()
                                .filter(stamp -> stamp >= req.getStartTimestamp()
                                                && stamp < req.getEndTimestamp())
                                .sorted().collect(Collectors.toList());
                @SuppressWarnings("unchecked")
                StreamObserver<GetTimestampsRangeResponse> observer = invocation.getArgumentAt(1, StreamObserver.class);
                observer.onNext(GetTimestampsRangeResponse.newBuilder().addAllTimestamp(timestamps).build());
                observer.onCompleted();
                return null;
            }
        };
        Mockito.doAnswer(answerGetStamps).when(history).getPercentileTimestamps(Mockito.any(),
                        Mockito.any());
        grpcServer = GrpcTestServer.newServer(history);
        grpcServer.start();
        return StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Cleans up resources.
     */
    protected void shutdown() {
        grpcServer.close();
    }

}
