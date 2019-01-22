package com.vmturbo.action.orchestrator.stats.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.ImmutableSingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.SingleActionSnapshotFactory.SingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.aggregator.ClusterActionAggregator.ClusterActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChain.MultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChain.MultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

public class ClusterActionAggregatorTest {

    private static final ActionDTO.Action SAVINGS_ACTION = ActionDTO.Action.newBuilder()
            .setId(1)
            .setInfo(ActionInfo.getDefaultInstance())
            .setImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .setSavingsPerHour(CurrencyAmount.newBuilder()
                    .setAmount(1.0))
            .build();

    private static final LocalDateTime TIME = LocalDateTime.MAX;

    private GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());

    private SupplyChainServiceMole supplyChainServiceMole = Mockito.spy(new SupplyChainServiceMole());

    @Rule
    public GrpcTestServer grpcServer =
            GrpcTestServer.newServer(groupServiceMole, supplyChainServiceMole);

    private ClusterActionAggregatorFactory aggregatorFactory;

    private static final ActionEntity CLUSTER_1_PM_1 = ActionEntity.newBuilder()
            .setId(1)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

    private static final ActionEntity CLUSTER_1_PM_2 = ActionEntity.newBuilder()
            .setId(11)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

    private static final ActionEntity CLUSTER_1_VM_1 = ActionEntity.newBuilder()
            .setId(71)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

    private static final ActionEntity CLUSTER_1_VM_2 = ActionEntity.newBuilder()
            .setId(711)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

    private static final ActionEntity CLUSTER_2_PM = ActionEntity.newBuilder()
            .setId(2)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

    private static final ActionEntity CLUSTER_2_VM = ActionEntity.newBuilder()
            .setId(72)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

    private static final Group CLUSTER_1 = Group.newBuilder()
        .setId(123)
        .setType(Type.CLUSTER)
        .setCluster(ClusterInfo.newBuilder()
            .setMembers(StaticGroupMembers.newBuilder()
                .addStaticMemberOids(CLUSTER_1_PM_1.getId())
                .addStaticMemberOids(CLUSTER_1_PM_2.getId())))
        .build();

    private static final Group CLUSTER_2 = Group.newBuilder()
        .setId(456)
        .setType(Type.CLUSTER)
        .setCluster(ClusterInfo.newBuilder()
            .setMembers(StaticGroupMembers.newBuilder()
                .addStaticMemberOids(CLUSTER_2_PM.getId())))
        .build();

    private static final MgmtUnitSubgroup CLUSTER_1_GLOBAL_SUBGROUP = ImmutableMgmtUnitSubgroup.builder()
            .id(12345)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(CLUSTER_1.getId())
                .build())
            .build();

    private static final MgmtUnitSubgroup CLUSTER_2_GLOBAL_SUBGROUP = ImmutableMgmtUnitSubgroup.builder()
        .id(6789)
        .key(ImmutableMgmtUnitSubgroupKey.builder()
            .mgmtUnitId(CLUSTER_2.getId())
            .build())
        .build();

    private static final MgmtUnitSubgroup CLUSTER_1_PM_SUBGROUP = ImmutableMgmtUnitSubgroup.builder()
            .id(321)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                    .entityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .mgmtUnitId(CLUSTER_1.getId())
                    .build())
            .build();

    private static final MgmtUnitSubgroup CLUSTER_2_PM_SUBGROUP = ImmutableMgmtUnitSubgroup.builder()
            .id(432)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                    .entityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .mgmtUnitId(CLUSTER_2.getId())
                    .build())
            .build();

    private static final MgmtUnitSubgroup CLUSTER_1_VM_SUBGROUP = ImmutableMgmtUnitSubgroup.builder()
            .id(3210)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                    .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .mgmtUnitId(CLUSTER_1.getId())
                    .build())
            .build();

    private static final MgmtUnitSubgroup CLUSTER_2_VM_SUBGROUP = ImmutableMgmtUnitSubgroup.builder()
            .id(4320)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                    .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .mgmtUnitId(CLUSTER_2.getId())
                    .build())
            .build();

    private static final ActionGroupKey ACTION_GROUP_KEY = mock(ActionGroupKey.class);

    private static final ActionGroup ACTION_GROUP = ImmutableActionGroup.builder()
            .id(888)
            .key(ACTION_GROUP_KEY)
            .build();

    @Before
    public void setup() {
        aggregatorFactory =
            new ClusterActionAggregatorFactory(grpcServer.getChannel(), grpcServer.getChannel());
    }

    @Test
    public void testAggregateClusterEntities() {
        final ClusterActionAggregator clusterActionAggregator = aggregatorFactory.newAggregator(TIME);
        final GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setTypeFilter(Type.CLUSTER)
                .build();
        when(groupServiceMole.getGroups(expectedRequest))
            .thenReturn(Arrays.asList(CLUSTER_1, CLUSTER_2));

        clusterActionAggregator.start();

        verify(groupServiceMole).getGroups(expectedRequest);

        // Process an action snapshot involving both PMs in cluster 1.
        clusterActionAggregator.processAction(fakeSnapshot(CLUSTER_1_PM_1, CLUSTER_1_PM_2));

        // Process two action snapshots involving the PM in cluster 2.
        clusterActionAggregator.processAction(fakeSnapshot(CLUSTER_2_PM));
        clusterActionAggregator.processAction(fakeSnapshot(CLUSTER_2_PM));

        final Map<Integer, ActionStatsLatestRecord> recordsByMgtmtUnitSubgroup =
            clusterActionAggregator.createRecords(ImmutableMap.of(
                        CLUSTER_1_PM_SUBGROUP.key(), CLUSTER_1_PM_SUBGROUP,
                        CLUSTER_2_PM_SUBGROUP.key(), CLUSTER_2_PM_SUBGROUP,
                        CLUSTER_1_GLOBAL_SUBGROUP.key(), CLUSTER_1_GLOBAL_SUBGROUP,
                        CLUSTER_2_GLOBAL_SUBGROUP.key(), CLUSTER_2_GLOBAL_SUBGROUP),
                    ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
            .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));
        assertThat(recordsByMgtmtUnitSubgroup.keySet(),
                containsInAnyOrder(CLUSTER_1_PM_SUBGROUP.id(), CLUSTER_2_PM_SUBGROUP.id(),
                    CLUSTER_1_GLOBAL_SUBGROUP.id(), CLUSTER_2_GLOBAL_SUBGROUP.id()));

        final ActionStatsLatestRecord cluster1Record =
                recordsByMgtmtUnitSubgroup.get(CLUSTER_1_PM_SUBGROUP.id());
        assertThat(cluster1Record.getTotalEntityCount(), is(2));
        assertThat(cluster1Record.getTotalActionCount(), is(1));

        final ActionStatsLatestRecord cluster2Record =
                recordsByMgtmtUnitSubgroup.get(CLUSTER_2_PM_SUBGROUP.id());
        assertThat(cluster2Record.getTotalEntityCount(), is(1));
        assertThat(cluster2Record.getTotalActionCount(), is(2));

        final ActionStatsLatestRecord cluster1GlobalRecord =
            recordsByMgtmtUnitSubgroup.get(CLUSTER_1_GLOBAL_SUBGROUP.id());
        assertThat(cluster1GlobalRecord.getTotalEntityCount(), is(2));
        assertThat(cluster1GlobalRecord.getTotalActionCount(), is(1));

        final ActionStatsLatestRecord cluster2GlobalRecord =
            recordsByMgtmtUnitSubgroup.get(CLUSTER_2_GLOBAL_SUBGROUP.id());
        assertThat(cluster2GlobalRecord.getTotalEntityCount(), is(1));
        assertThat(cluster2GlobalRecord.getTotalActionCount(), is(2));
    }

    @Test
    public void testAggregateClusterNoRelatedActionsNoRecord() {
        final ClusterActionAggregator clusterActionAggregator = aggregatorFactory.newAggregator(TIME);
        final GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setTypeFilter(Type.CLUSTER)
                .build();
        when(groupServiceMole.getGroups(expectedRequest))
                .thenReturn(Arrays.asList(CLUSTER_1, CLUSTER_2));

        clusterActionAggregator.start();

        verify(groupServiceMole).getGroups(expectedRequest);

        // Process an action snapshot involving unrelated entities.
        clusterActionAggregator.processAction(fakeSnapshot(ActionEntity.newBuilder()
                .setId(127737)
                .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                .build()));

        assertThat(clusterActionAggregator.createRecords(
                Collections.emptyMap(), Collections.emptyMap()).count(), is(0L));
    }

    @Test
    public void testAggregateClusterRelatedVMs() {
        final ClusterActionAggregator clusterActionAggregator = aggregatorFactory.newAggregator(TIME);
        final GetGroupsRequest expectedRequest = GetGroupsRequest.newBuilder()
                .setTypeFilter(Type.CLUSTER)
                .build();
        when(groupServiceMole.getGroups(expectedRequest))
                .thenReturn(Arrays.asList(CLUSTER_1, CLUSTER_2));

        final MultiSupplyChainsRequest expectedSupplyChainRequest = MultiSupplyChainsRequest.newBuilder()
                .addSeeds(SupplyChainSeed.newBuilder()
                        .setSeedOid(CLUSTER_1.getId())
                        .addEntityTypesToInclude("VirtualMachine")
                        .addStartingEntityOid(CLUSTER_1_PM_1.getId())
                        .addStartingEntityOid(CLUSTER_1_PM_2.getId()))
                .addSeeds(SupplyChainSeed.newBuilder()
                        .setSeedOid(CLUSTER_2.getId())
                        .addEntityTypesToInclude("VirtualMachine")
                        .addStartingEntityOid(CLUSTER_2_PM.getId()))
                .build();
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
            .thenReturn(Arrays.asList(
                MultiSupplyChainsResponse.newBuilder()
                    .setSeedOid(CLUSTER_1.getId())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                            .setEntityType("VirtualMachine")
                            .putAllMembersByState(ImmutableMap.of(EntityState.POWERED_ON_VALUE,
                                    MemberList.newBuilder()
                                        .addMemberOids(CLUSTER_1_VM_1.getId())
                                        .addMemberOids(CLUSTER_1_VM_2.getId())
                                        .build())))
                    .build(),
                MultiSupplyChainsResponse.newBuilder()
                    .setSeedOid(CLUSTER_2.getId())
                    .addSupplyChainNodes(SupplyChainNode.newBuilder()
                        .setEntityType("VirtualMachine")
                        .putAllMembersByState(ImmutableMap.of(EntityState.POWERED_ON_VALUE,
                            MemberList.newBuilder()
                                .addMemberOids(CLUSTER_2_VM.getId())
                                .build())))
                    .build()));

        clusterActionAggregator.start();

        verify(groupServiceMole).getGroups(expectedRequest);

        // Because the seeds get added in random order (iterating hashmap), and there are no
        // order-insensitive protobuf comparators, capture the request and check it's contents.
        ArgumentCaptor<MultiSupplyChainsRequest> supplyChainRequestCaptor =
                ArgumentCaptor.forClass(MultiSupplyChainsRequest.class);
        verify(supplyChainServiceMole).getMultiSupplyChains(supplyChainRequestCaptor.capture());
        final MultiSupplyChainsRequest gotRequest = supplyChainRequestCaptor.getValue();
        assertThat(gotRequest.getSeedsList(),
                containsInAnyOrder(expectedSupplyChainRequest.getSeedsList().toArray()));

        // Process action snapshots involving a VM in cluster 1, as well as a random VM not
        // in the cluster.
        clusterActionAggregator.processAction(fakeSnapshot(CLUSTER_1_VM_1));
        clusterActionAggregator.processAction(fakeSnapshot(CLUSTER_1_VM_2));
        clusterActionAggregator.processAction(fakeSnapshot(ActionEntity.newBuilder()
                .setId(1823)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build()));

        // Process an action snapshot involving the VM in cluster 2.
        clusterActionAggregator.processAction(fakeSnapshot(CLUSTER_2_VM));

        final Map<Integer, ActionStatsLatestRecord> recordsByMgtmtUnitSubgroup =
            clusterActionAggregator.createRecords(ImmutableMap.of(
                    CLUSTER_1_VM_SUBGROUP.key(), CLUSTER_1_VM_SUBGROUP,
                    CLUSTER_2_VM_SUBGROUP.key(), CLUSTER_2_VM_SUBGROUP),
                ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
            .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));
        assertThat(recordsByMgtmtUnitSubgroup.keySet(),
                containsInAnyOrder(CLUSTER_1_VM_SUBGROUP.id(), CLUSTER_2_VM_SUBGROUP.id()));

        final ActionStatsLatestRecord cluster1Record = recordsByMgtmtUnitSubgroup.get(CLUSTER_1_VM_SUBGROUP.id());
        assertThat(cluster1Record.getTotalEntityCount(), is(2));
        assertThat(cluster1Record.getTotalActionCount(), is(2));

        final ActionStatsLatestRecord cluster2Record = recordsByMgtmtUnitSubgroup.get(CLUSTER_2_VM_SUBGROUP.id());
        assertThat(cluster2Record.getTotalEntityCount(), is(1));
        assertThat(cluster2Record.getTotalActionCount(), is(1));
    }

    private SingleActionSnapshot fakeSnapshot(@Nonnull final ActionEntity... involvedEntities) {
        final ImmutableSingleActionSnapshot.Builder actionSnapshotBuilder = ImmutableSingleActionSnapshot.builder()
                .actionGroupKey(ACTION_GROUP_KEY)
                .recommendation(SAVINGS_ACTION);
        actionSnapshotBuilder.addInvolvedEntities(involvedEntities);
        return actionSnapshotBuilder.build();
    }
}
