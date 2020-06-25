package com.vmturbo.extractor.topology;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Iterator;

import io.grpc.testing.GrpcCleanupRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.extractor.util.ExtractorTestUtil;

/**
 * Tests of the {@link TopologyEntitiesListener} class.
 *
 * <p>This is currently in a WIP state, working on setup design before cranking out real tests.</p>
 */
public class TopologyEntitiesListenerTest {

    private ITopologyWriter writer;
    private TopologyEntitiesListener listener;

    /**
     * Rule to manage the in-process grpc service we use for group service testing.
     */
    @Rule
    public GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    /**
     * Set up stuff we'll need for tests.
     */
    @Before
    public void begin() {
        writer = mock(ITopologyWriter.class);
//        listener = new TopologyEntitiesListener(mock(GroupServiceBlockingStub.class),
//                ImmutableList.of(() -> writer), ExtractorTestUtil.config);
    }


    /**
     * Test that can access our mock group service.
     *
     * <p>This probably won't survive - it's a WIP.</p>
     *
     * @throws IOException if there's a problem with the service
     */
    @Test
    public void testGroupService() throws IOException {
        GroupServiceBlockingStub groupServiceClient = ExtractorTestUtil.groupService(grpcCleanup);
        final Iterator<Grouping> groups = groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setIncludeTemporary(false)
                        .setIncludeHidden(false)
                        .build())
                .build());
        while (groups.hasNext()) {
            final Grouping group = groups.next();
            System.out.println(group.getDefinition().getDisplayName());
        }
    }

    /**
     * Another WIP test of the mock group service.
     *
     * <p>This probably also won't survive.</p>
     *
     * @throws IOException if there's a problem accessing the group service
     */
    @Test
    public void testGroupService2() throws IOException {
        GroupServiceBlockingStub groupServiceClient = ExtractorTestUtil.groupService(grpcCleanup);
        final Iterator<Grouping> groups = groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setIncludeTemporary(false)
                        .setIncludeHidden(false)
                        .build())
                .build());
        while (groups.hasNext()) {
            final Grouping group = groups.next();
            System.out.println(group.getDefinition().getDisplayName());
        }
    }
}
