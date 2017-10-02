package com.vmturbo.topology.processor.group.policy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
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

    private final TopologyGraph topologyGraph = Mockito.mock(TopologyGraph.class);

    private final PolicyGrouping group1 = PolicyGroupingHelper.policyGrouping(
        "Group 1", EntityType.PHYSICAL_MACHINE_VALUE, 1L);

    private final PolicyGrouping group2 = PolicyGroupingHelper.policyGrouping(
        "Group 2", EntityType.STORAGE_VALUE, 2L);

    private final PolicyGrouping group3 = PolicyGroupingHelper.policyGrouping(
        "Group 3", EntityType.VIRTUAL_MACHINE_VALUE, 3L);

    private final PolicyGrouping group4 = PolicyGroupingHelper.policyGrouping(
        "Group 4", EntityType.APPLICATION_SERVER_VALUE, 4L);

    private GrpcTestServer grpcServer;

    private com.vmturbo.topology.processor.group.policy.PolicyManager policyManager;

    @Before
    public void setup() throws IOException {
        when(filterFactory.newGroupResolver()).thenReturn(groupResolver);

        // set up a mock Actions RPC server
        grpcServer = GrpcTestServer.withServices(policyServiceBackend);
        final PolicyServiceGrpc.PolicyServiceBlockingStub policyRpcService =
            PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the ActionsService to test
        policyManager = new com.vmturbo.topology.processor.group.policy.PolicyManager(
            policyRpcService, filterFactory, new PolicyFactory());
    }

    @After
    public void teardown() {
        grpcServer.close();
    }

    @Test
    public void testApplyPoliciesResolvesGroups() throws Exception {
        ArgumentCaptor<PolicyGrouping> groupArguments = ArgumentCaptor.forClass(PolicyGrouping.class);
        policyManager.applyPolicies(topologyGraph);

        verify(groupResolver, times(4)).resolve(groupArguments.capture(), eq(topologyGraph));
        assertThat(groupArguments.getAllValues(), containsInAnyOrder(group1, group2, group3, group4));
    }

    @Test
    public void testGroupResolutionErrorDoesNotStopProcessing() throws Exception {
        // The first policy will have an exception, but the second one should still be run.
        ArgumentCaptor<PolicyGrouping> groupArguments = ArgumentCaptor.forClass(PolicyGrouping.class);
        when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
            .thenThrow(new GroupResolutionException("error!"));

        policyManager.applyPolicies(topologyGraph);

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
                .setPolicy(bindToGroup(group1, group2))
                .build());

            responseObserver.onNext(PolicyResponse.newBuilder()
                .setPolicy(bindToGroup(group3, group4))
                .build());

            responseObserver.onCompleted();
        }
    }

    private static Policy bindToGroup(@Nonnull final PolicyGrouping consumerGroup,
                                      @Nonnull final PolicyGrouping providerGroup) {
        return Policy.newBuilder()
            .setId(1234L)
            .setBindToGroup(
            BindToGroupPolicy.newBuilder()
                .setConsumerGroup(consumerGroup)
                .setProviderGroup(providerGroup)
        ).build();
    }
}