package com.vmturbo.extractor.util;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Utility class with stuff that's used both topology and model tests.
 */
public class ExtractorTestUtil {

    private ExtractorTestUtil() {
    }

    /**
     * WriterConfig used for tests.
     */
    public static final WriterConfig config = ImmutableWriterConfig.builder()
            .addAllReportingCommodityWhitelist(
                    ModelDefinitions.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST.stream()
                            .map(CommodityType::getNumber)
                            .collect(Collectors.toList()))
            .insertTimeoutSeconds(60)
            .lastSeenAdditionalFuzzMinutes(10)
            .lastSeenUpdateIntervalMinutes(20)
            .build();

    // TODO look into moles and use it instead of this
    /**
     * Set up an in-process group service for use in tests.
     *
     * <p>This is a work in progress... it will have a parameter (or parameters) to specify
     * the grouping structures that it should use in building service response, in some simple form
     * like a Map&lt;Long, Collection&lt;Long&gt;&gt; linking group ids to member entity ids.</p>
     *
     * @param grpcCleanup a {@link GrpcCleanupRule} that will manage setup and teardown fo the grpc
     *                    resources
     * @return the client endpoint for the service
     * @throws IOException if there's a problem
     */
    public static GroupServiceBlockingStub groupService(GrpcCleanupRule grpcCleanup) throws IOException {
        GroupServiceImplBase serviceImpl = mock(GroupServiceImplBase.class, delegatesTo(
                new GroupServiceImplBase() {
                    @Override
                    public void getGroups(final GetGroupsRequest request, final StreamObserver<Grouping> responseObserver) {
                        responseObserver.onNext(Grouping.newBuilder()
                                .setDefinition(GroupDefinition.newBuilder()
                                        .setDisplayName("myGroup").build())
                                .build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void getMembers(final GetMembersRequest request, final StreamObserver<GetMembersResponse> responseObserver) {
                        super.getMembers(request, responseObserver);
                    }
                }
        ));
        String serverName = InProcessServerBuilder.generateName();
        grpcCleanup.register(InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(serviceImpl).build().start());
        ManagedChannel channel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName)
                .directExecutor().build());
        return GroupServiceGrpc.newBlockingStub(channel);
    }
}
