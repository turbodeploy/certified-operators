package com.vmturbo.topology.processor.group.policy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupFetcher;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class PolicyManagerTest {

    private final TestPolicyRpcService policyServiceBackend =
        Mockito.spy(new TestPolicyRpcService());

    private final TopologyFilterFactory filterFactory = Mockito.mock(TopologyFilterFactory.class);
    private final GroupResolver groupResolver = Mockito.mock(GroupResolver.class);
    private final GroupFetcher groupFetcher = Mockito.mock(GroupFetcher.class);

    private final TopologyGraph topologyGraph = Mockito.mock(TopologyGraph.class);

    private final PolicyGrouping group1 = PolicyGroupingHelper.policyGrouping(
        "Group 1", EntityType.PHYSICAL_MACHINE_VALUE, 1L);

    private final PolicyGrouping group2 = PolicyGroupingHelper.policyGrouping(
        "Group 2", EntityType.STORAGE_VALUE, 2L);

    private final PolicyGrouping group3 = PolicyGroupingHelper.policyGrouping(
        "Group 3", EntityType.VIRTUAL_MACHINE_VALUE, 3L);

    private final PolicyGrouping group4 = PolicyGroupingHelper.policyGrouping(
        "Group 4", EntityType.APPLICATION_SERVER_VALUE, 4L);

    private final PolicyGroupingID id1 = PolicyGroupingHelper.policyGroupingID(1L);
    private final PolicyGroupingID id2 = PolicyGroupingHelper.policyGroupingID(2L);
    private final PolicyGroupingID id3 = PolicyGroupingHelper.policyGroupingID(3L);
    private final PolicyGroupingID id4 = PolicyGroupingHelper.policyGroupingID(4L);

    private final Map<PolicyGroupingID, PolicyGrouping> groupingMap =
            new HashMap<PolicyGroupingID, PolicyGrouping>() {{
                put(id1, group1);
                put(id2, group2);
                put(id3, group3);
                put(id4, group4);
            }};

    private final Policy policy12 = bindToGroup(group1, group2, 12L);
    private final Policy policy34 = bindToGroup(group3, group4, 34L);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyServiceBackend);

    private com.vmturbo.topology.processor.group.policy.PolicyManager policyManager;

    @Before
    public void setup() throws Exception {
        when(groupFetcher.getGroupings(Sets.newHashSet(id1, id2, id3, id4))).thenReturn(groupingMap);

        final PolicyServiceGrpc.PolicyServiceBlockingStub policyRpcService =
            PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the GroupService to test
        policyManager = new com.vmturbo.topology.processor.group.policy.PolicyManager(
            policyRpcService, groupFetcher, new PolicyFactory());
    }

    @Test
    public void testApplyPoliciesResolvesGroups() throws Exception {
        ArgumentCaptor<PolicyGrouping> groupArguments = ArgumentCaptor.forClass(PolicyGrouping.class);
        policyManager.applyPolicies(topologyGraph, groupResolver);

        verify(groupResolver, times(4)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertThat(groupArguments.getAllValues(), containsInAnyOrder(group1, group2, group3, group4));
    }

    @Test
    public void testGroupResolutionErrorDoesNotStopProcessing() throws Exception {
        // The first policy will have an exception, but the second one should still be run.
        ArgumentCaptor<PolicyGrouping> groupArguments = ArgumentCaptor.forClass(PolicyGrouping.class);
        when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
            .thenThrow(new GroupResolutionException("error!"));

        policyManager.applyPolicies(topologyGraph, groupResolver);

        verify(groupResolver, atLeast(2)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertThat(groupArguments.getAllValues(), hasItems(group3, group4));
    }

    /**
     * This class mocks backend PolicyService RPC calls. An instance is injected into the
     * instance under test.
     */
    private class TestPolicyRpcService extends PolicyServiceGrpc.PolicyServiceImplBase {
        public void getAllPolicies(PolicyRequest request,
                                   StreamObserver<PolicyResponse> responseObserver) {
            responseObserver.onNext(PolicyResponse.newBuilder()
                .setPolicy(policy12)
                .build());

            responseObserver.onNext(PolicyResponse.newBuilder()
                .setPolicy(policy34)
                .build());

            responseObserver.onCompleted();
        }
    }

    private static Policy bindToGroup(@Nonnull final PolicyGrouping consumerGroup,
                                      @Nonnull final PolicyGrouping providerGroup, final Long id) {
        PolicyGroupingID.Builder builder = PolicyGroupingID.newBuilder();
        PolicyGroupingID consumerGroupID = consumerGroup.hasGroup() ?
                builder.setGroupId(consumerGroup.getGroup().getId()).build() :
                builder.setGroupId(consumerGroup.getCluster().getId()).build();
        PolicyGroupingID providerGroupID = providerGroup.hasGroup() ?
                builder.setGroupId(providerGroup.getGroup().getId()).build() :
                builder.setGroupId(providerGroup.getCluster().getId()).build();
        return Policy.newBuilder()
            .setId(id)
            .setBindToGroup(
            BindToGroupPolicy.newBuilder()
                .setConsumerGroupId(consumerGroupID)
                .setProviderGroupId(providerGroupID)
        ).build();
    }
}
