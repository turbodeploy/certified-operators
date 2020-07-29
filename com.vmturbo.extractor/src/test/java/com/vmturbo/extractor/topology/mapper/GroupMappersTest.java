package com.vmturbo.extractor.topology.mapper;

import static com.vmturbo.extractor.util.GroupServiceTestUtil.groupList;
import static com.vmturbo.extractor.util.GroupServiceTestUtil.makeGroup;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.BILLING_FAMILY;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.REGULAR;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.RESOURCE;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.STORAGE_CLUSTER;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.util.GroupServiceTestUtil;
import com.vmturbo.platform.common.dto.CommonDTOREST.GroupDTO.GroupType;

/**
 * Tests of GroupMappers class.
 */
public class GroupMappersTest {

    private final GroupServiceMole groupMole = spy(GroupServiceMole.class);

    /** Rule to manage Grpc server. */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupMole);

    private GroupServiceBlockingStub groupService;

    /** set up mocked group service. */
    @Before
    public void before() {
        groupService = GroupServiceTestUtil.getGroupService(grpcServer);
    }

    /**
     * Check that the group type mapper works for all group types.
     *
     * <p>We'll throw a group of each known type at the mapper to make sure that we get the right
     * mapped name back. For unmapped types we expect the {@link GroupType} enum name.</p>
     */
    @Test
    public void testGroupFetcherFetchesAllGroupTypes() {
        final List<Grouping> getGroupsResponse = groupList(
                makeGroup(REGULAR, 100L),
                makeGroup(RESOURCE, 200L),
                makeGroup(BILLING_FAMILY, 300L),
                makeGroup(COMPUTE_HOST_CLUSTER, 400L),
                makeGroup(COMPUTE_VIRTUAL_MACHINE_CLUSTER, 500L),
                makeGroup(STORAGE_CLUSTER, 100L));
        doReturn(getGroupsResponse).when(groupMole).getGroups(any(GetGroupsRequest.class));
        final Iterator<Grouping> groups = groupService.getGroups(GetGroupsRequest.newBuilder().build());
        // successively peel off each returned group and check that its type maps to the expected
        // value. Note that by using string literals here instead of the corresponding constants we
        // protect against those constants being inadvertantly changed, e.g. during other editing
        // in the vicinity.
        assertThat(getNextMappedTypeName(groups), is(EntityType.GROUP));
        assertThat(getNextMappedTypeName(groups), is(EntityType.RESOURCE_GROUP));
        assertThat(getNextMappedTypeName(groups), is(EntityType.BILLING_FAMILY));
        assertThat(getNextMappedTypeName(groups), is(EntityType.COMPUTE_CLUSTER));
        assertThat(getNextMappedTypeName(groups), is(EntityType.K8S_CLUSTER));
        assertThat(getNextMappedTypeName(groups), is(EntityType.STORAGE_CLUSTER));
        assertThat(groups.hasNext(), is(false));
        // make sure we've got all group types covered (add new groups above if not)
        assertThat(GroupType.values().length, is(getGroupsResponse.size()));
    }

    /**
     * Utility class to take the next group from the supplied iterator and apply the group type mapper
     * to its type.
     *
     * @param groups group iterator
     * @return the group type name, according to our mapper
     */
    private EntityType getNextMappedTypeName(final Iterator<Grouping> groups) {
        return GroupMappers.mapGroupTypeToName(groups.next().getDefinition().getType());
    }
}
