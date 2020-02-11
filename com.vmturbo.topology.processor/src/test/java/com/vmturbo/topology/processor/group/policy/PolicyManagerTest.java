package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.neverDiscoveredTopologyEntity;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator.Results;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory;

/**
 * Unit tests for {@link PolicyManager}.
 */
public class PolicyManagerTest {

    private final PolicyServiceMole policyServiceMole = Mockito.spy(new PolicyServiceMole());

    private final GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());

    private final ReservationServiceMole reservationServiceMole =
            Mockito.spy(new ReservationServiceMole());

    private final GroupResolver groupResolver = mock(GroupResolver.class);

    private final TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);

    private final PolicyApplicator policyApplicator = mock(PolicyApplicator.class);

    private final ReservationPolicyFactory reservationPolicyFactory =
            mock(ReservationPolicyFactory.class);

    private final long id1 = 1L;
    private final long id2 = 2L;
    private final long id3 = 3L;
    private final long id4 = 4L;
    private final long id5 = 5L;

    private final Grouping group1 = PolicyGroupingHelper.policyGrouping(
        "Group 1", EntityType.PHYSICAL_MACHINE_VALUE, id1);

    private final Grouping group2 = PolicyGroupingHelper.policyGrouping(
        "Group 2", EntityType.STORAGE_VALUE, id2);

    private final Grouping group3 = PolicyGroupingHelper.policyGrouping(
        "Group 3", EntityType.VIRTUAL_MACHINE_VALUE, id3);

    private final Grouping group4 = PolicyGroupingHelper.policyGrouping(
        "Group 4", EntityType.APPLICATION_SERVER_VALUE, id4);

    private final Grouping group5 = PolicyGroupingHelper.policyGrouping(
        "Group 5", EntityType.APPLICATION_SERVER_VALUE, id5);

    private final Policy policy12 = bindToGroup(id1, id2, 12L);
    private final Policy policy34 = bindToGroup(id3, id4, 34L);
    private final Policy planPolicy = bindToGroup(id3, id5, 35L);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyServiceMole, groupServiceMole,
            reservationServiceMole);

    private com.vmturbo.topology.processor.group.policy.PolicyManager policyManager;

    @Before
    public void setup() throws Exception {
        when(groupServiceMole.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
            .addAllId(Arrays.asList(id1, id2, id3, id4)))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())).thenReturn(Arrays.asList(group1, group2, group3, group4));
        when(groupServiceMole.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
            .addAllId(Arrays.asList(id1, id2, id3, id4, id5)))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())).thenReturn(Arrays.asList(group1, group2, group3, group4, group5));
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

        final ReservationServiceBlockingStub reservationServiceStub =
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the GroupService to test
        policyManager = new com.vmturbo.topology.processor.group.policy.PolicyManager(
            policyRpcService, groupServiceStub, new PolicyFactory(), reservationPolicyFactory,
                reservationServiceStub, policyApplicator);
    }

    @Test
    public void testNoPoliciesNoGroupRPC() {
        when(policyServiceMole.getAllPolicies(any())).thenReturn(Collections.emptyList());
        when(topologyGraph.entities()).thenReturn(Stream.empty());
        when(policyApplicator.applyPolicies(any(), eq(groupResolver), eq(topologyGraph)))
            .thenReturn(new Results());
        policyManager.applyPolicies(topologyGraph, groupResolver, Collections.emptyList());

        // There shouldn't be a call to get groups if there are no policies.
        verify(groupServiceMole, never()).getGroups(any());
    }

    /**
     * Test that a user policy that refers to non-existing groups gets ignored.
     */
    @Test
    public void testUserPolicyMissingGroup() {
        when(policyServiceMole.getAllPolicies(any())).thenReturn(Collections.singletonList(PolicyResponse.newBuilder()
            .setPolicy(policy12)
            .build()));
        // Do not configure the group service to return any of the groups.
        when(topologyGraph.entities()).thenReturn(Stream.empty());

        Results expectedResults = mock(Results.class);

        when(policyApplicator.applyPolicies(any(), eq(groupResolver), eq(topologyGraph)))
            .thenReturn(expectedResults);

        final Results results = policyManager.applyPolicies(topologyGraph, groupResolver, Collections.emptyList());
        // We should filter out the invalid policy.
        verify(policyApplicator).applyPolicies(eq(Collections.emptyList()), eq(groupResolver), eq(topologyGraph));
        assertThat(results, is(expectedResults));
    }

    /**
     * Test that a discovered policy that refers to non-existing groups throws an exception.
     */
    @Test(expected = IllegalStateException.class)
    public void testDiscoveredPolicyMissingGroupException() {
        when(policyServiceMole.getAllPolicies(any())).thenReturn(Collections.singletonList(PolicyResponse.newBuilder()
            .setPolicy(policy12.toBuilder()
                .setTargetId(123)
                .build())
            .build()));
        // Do not configure the group service to return any of the groups.
        when(topologyGraph.entities()).thenReturn(Stream.empty());

        policyManager.applyPolicies(topologyGraph, groupResolver, Collections.emptyList());
    }

    @Test
    public void testHandleReservationPolicyConstraints() {
        final List<ScenarioChange> scenarioChanges = Lists.newArrayList(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                        .addInitialPlacementConstraints(ReservationConstraintInfo.newBuilder()
                                .setConstraintId(33)
                                .setType(ReservationConstraintInfo.Type.POLICY)))
                .build());
        Mockito.when(topologyGraph.entities())
                .thenReturn(Stream.of(neverDiscoveredTopologyEntity(5L, EntityType.VIRTUAL_MACHINE).build()));
        Mockito.when(reservationServiceMole.getAllReservations(
                GetAllReservationsRequest.newBuilder().build()))
                .thenReturn(Lists.newArrayList(Reservation.newBuilder()
                        .setStatus(ReservationStatus.RESERVED)
                        .setId(44)
                        .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                                .addReservationTemplate(ReservationTemplate.newBuilder()
                                        .setTemplateId(77)
                                        .setCount(1)
                                        .addReservationInstance(ReservationInstance.newBuilder()
                                                .setEntityId(66L))))
                        .setConstraintInfoCollection(ConstraintInfoCollection.newBuilder()
                                .addReservationConstraintInfo(ReservationConstraintInfo.newBuilder()
                                        .setType(ReservationConstraintInfo.Type.POLICY)
                                        .setConstraintId(34)))
                        .build()));
        final Map<Long, Set<Long>> policyConstraintMap =
                policyManager.handleReservationConstraints(topologyGraph,
                        scenarioChanges, Collections.emptyList());
        assertEquals(2L, policyConstraintMap.keySet().size());
        final Set<Long> reservationIds = policyConstraintMap.get(34L);
        final Set<Long> initialPlacementIds = policyConstraintMap.get(33L);
        assertEquals(Sets.newHashSet(66L), reservationIds);
        assertEquals(Sets.newHashSet(5L), initialPlacementIds);
    }


    private List<ScenarioChange> changes() {
        return Lists.newArrayList(
            ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder()
                .setPolicyChange(PolicyChange.newBuilder()
                    .setPlanOnlyPolicy(planPolicy)
                    .build())
                .build())
            .build());
    }

    private static Policy bindToGroup(@Nonnull final long consumerId,
                                      @Nonnull final long providerId,
                                      final long id) {
        return Policy.newBuilder()
            .setId(id)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setConsumerGroupId(consumerId)
                    .setProviderGroupId(providerId)))
            .build();
    }
}
