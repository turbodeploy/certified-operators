package com.vmturbo.topology.processor.group.policy;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class PolicyManagerTest {

    private final PolicyServiceMole policyServiceMole = Mockito.spy(new PolicyServiceMole());

    private final GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());

    private final GroupResolver groupResolver = Mockito.mock(GroupResolver.class);

    private final TopologyGraph topologyGraph = Mockito.mock(TopologyGraph.class);

    private final long id1 = 1L;
    private final long id2 = 2L;
    private final long id3 = 3L;
    private final long id4 = 4L;

    private final Group group1 = PolicyGroupingHelper.policyGrouping(
        "Group 1", EntityType.PHYSICAL_MACHINE_VALUE, id1);

    private final Group group2 = PolicyGroupingHelper.policyGrouping(
        "Group 2", EntityType.STORAGE_VALUE, id2);

    private final Group group3 = PolicyGroupingHelper.policyGrouping(
        "Group 3", EntityType.VIRTUAL_MACHINE_VALUE, id3);

    private final Group group4 = PolicyGroupingHelper.policyGrouping(
        "Group 4", EntityType.APPLICATION_SERVER_VALUE, id4);

    private final Policy policy12 = bindToGroup(id1, id2, 12L);
    private final Policy policy34 = bindToGroup(id3, id4, 34L);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyServiceMole, groupServiceMole);

    private com.vmturbo.topology.processor.group.policy.PolicyManager policyManager;

    @Before
    public void setup() throws Exception {
        when(groupServiceMole.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(Arrays.asList(id1, id2, id3, id4))
                    .build())).thenReturn(Arrays.asList(group1, group2, group3, group4));
        when(policyServiceMole.getAllPolicies(any()))
            .thenReturn(Stream.of(policy12, policy34)
                .map(policy -> PolicyResponse.newBuilder()
                    .setPolicy(policy)
                    .build())
                .collect(Collectors.toList()));

        final PolicyServiceGrpc.PolicyServiceBlockingStub policyRpcService =
            PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());

        final GroupServiceBlockingStub groupServiceStub =
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the GroupService to test
        policyManager = new com.vmturbo.topology.processor.group.policy.PolicyManager(
            policyRpcService, groupServiceStub, new PolicyFactory());
    }

    @Test
    public void testApplyPoliciesResolvesGroups() throws Exception {
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        policyManager.applyPolicies(topologyGraph, groupResolver);

        verify(groupResolver, times(4)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertThat(groupArguments.getAllValues(), containsInAnyOrder(group1, group2, group3, group4));
    }

    @Test
    public void testGroupResolutionErrorDoesNotStopProcessing() throws Exception {
        // The first policy will have an exception, but the second one should still be run.
        ArgumentCaptor<Group> groupArguments = ArgumentCaptor.forClass(Group.class);
        when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
            .thenThrow(new GroupResolutionException("error!"));

        policyManager.applyPolicies(topologyGraph, groupResolver);

        verify(groupResolver, atLeast(2)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertThat(groupArguments.getAllValues(), hasItems(group3, group4));
    }

    private static Policy bindToGroup(@Nonnull final long consumerId,
                                      @Nonnull final long providerId,
                                      final long id) {
        return Policy.newBuilder()
            .setId(id)
            .setBindToGroup(BindToGroupPolicy.newBuilder()
                .setConsumerGroupId(consumerId)
                .setProviderGroupId(providerId))
            .build();
    }
}
