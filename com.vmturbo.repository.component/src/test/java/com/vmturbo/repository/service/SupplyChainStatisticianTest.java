package com.vmturbo.repository.service;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.EntityScope;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsResponse.SingleResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat.StatGroup;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.service.SupplyChainStatistician.SupplementaryData;
import com.vmturbo.repository.service.SupplyChainStatistician.SupplementaryDataFactory;
import com.vmturbo.repository.service.SupplyChainStatistician.TopologyEntityLookup;

/**
 * Unit tests for {@link SupplyChainStatistician}.
 */
public class SupplyChainStatisticianTest {

    /*
     * Discovered by Target 111:
     *
     * BA11
     *  |
     * VM1
     *  |
     * Tier111
     *
     * Discovered by Target 222:
     *
     * BA11    BA22
     *  |        |
     * VM1     VM2 (powered off)
     *  |        |
     * Tier111 Tier222
     *
     * Note: BA11 and VM1 are discovered by both targets!
     * The business accounts are not in the actual supply chain.
     */

    private static final long VM_OID = 1;

    private static final long DISABLED_VM_OID = 2;

    private static final long TIER_1_OID = 111;

    private static final long TIER_2_OID = 222;

    private static final long BUSINESS_ACCOUNT_1_OID = 11;

    private static final long BUSINESS_ACCOUNT_2_OID = 22;

    private static final long TARGET_1_OID = 111;
    private static final long TARGET_2_OID = 222;

    private static final long DB_OID = 321;

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 777777;

    private static final String BA1_DISPLAY_NAME = "BA1";
    private static final String BA2_DISPLAY_NAME = "BA2";
    private static final String VM_DISPLAY_NAME = "VM1";
    private static final String DISABLED_VM_DISPLAY_NAME = "VM2";
    private static final String TIER1_DISPLAY_NAME = "t2.micro";
    private static final String TIER2_DISPLAY_NAME = "c4.large";
    private static final String DB_DISPLAY_NAME = "DB1";

    private static final long RESOURCE_GROUP_1_OID = 123;
    private static final long RESOURCE_GROUP_2_OID = 321;


    private static final SupplyChain SUPPLY_CHAIN = SupplyChain.newBuilder()
        .addSupplyChainNodes(SupplyChainNode.newBuilder()
            .setEntityType(UIEntityType.COMPUTE_TIER.apiStr())
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                .addMemberOids(TIER_1_OID)
                .addMemberOids(TIER_2_OID)
                .build()))
        .addSupplyChainNodes(SupplyChainNode.newBuilder()
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr())
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                .addMemberOids(VM_OID)
                .addMemberOids(DISABLED_VM_OID)
                .build()))
        .build();

    private static final SupplyChain SUPPLY_CHAIN_VM_N_DB = SupplyChain.newBuilder()
        .addSupplyChainNodes(SupplyChainNode.newBuilder()
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr())
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                .addMemberOids(VM_OID)
                .addMemberOids(DISABLED_VM_OID)
                .build()))
        .addSupplyChainNodes(SupplyChainNode.newBuilder()
            .setEntityType(UIEntityType.DATABASE.apiStr())
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                .addMemberOids(DB_OID)
                .build()))
        .addSupplyChainNodes(SupplyChainNode.newBuilder()
            .setEntityType(UIEntityType.COMPUTE_TIER.apiStr())
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                .addMemberOids(TIER_1_OID)
                .addMemberOids(TIER_2_OID)
                .build()))
        .build();

    private EntitySeverityServiceMole entitySeverityServiceBackend = spy(EntitySeverityServiceMole.class);

    private ActionsServiceMole actionsServiceMole = spy(ActionsServiceMole.class);

    private GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);

    /**
     * Test server to mock out gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(entitySeverityServiceBackend,
            actionsServiceMole, groupServiceMole);


    private SupplementaryDataFactory mockSupplementaryDataFactory = mock(SupplementaryDataFactory.class);

    private SupplementaryData mockSupplementaryData = mock(SupplementaryData.class);

    private SupplyChainStatistician statistician = new SupplyChainStatistician(mockSupplementaryDataFactory);

    private TopologyEntityLookup entityLookup;

    private RepoGraphEntity mockEntity(final long oid,
                                       @Nonnull final UIEntityType type,
                                       @Nonnull final EntityState state,
                                       @Nonnull final Set<Long> discoveringTargetIds,
                                       @Nonnull final String displayName) {
        final RepoGraphEntity entity = mock(RepoGraphEntity.class);
        when(entity.getOid()).thenReturn(oid);
        when(entity.getEntityType()).thenReturn(type.typeNumber());
        when(entity.getEntityState()).thenReturn(state);
        when(entity.getDiscoveringTargetIds()).thenAnswer(invocation -> discoveringTargetIds.stream());
        when(entity.getOwner()).thenReturn(Optional.empty());
        when(entity.getDisplayName()).thenReturn(displayName);
        return entity;
    }

    /**
     * Common setup code before all methods.
     * This sets up the supply chain we use for all the tests.
     */
    @Before
    public void setup() {
        final RepoGraphEntity ba1 = mockEntity(BUSINESS_ACCOUNT_1_OID, UIEntityType.BUSINESS_ACCOUNT,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_1_OID, TARGET_2_OID), BA1_DISPLAY_NAME);
        final RepoGraphEntity ba2 = mockEntity(BUSINESS_ACCOUNT_2_OID, UIEntityType.BUSINESS_ACCOUNT,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_2_OID), BA2_DISPLAY_NAME);

        final RepoGraphEntity tier1 = mockEntity(TIER_1_OID, UIEntityType.COMPUTE_TIER,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_1_OID, TARGET_2_OID),
            TIER1_DISPLAY_NAME);

        final RepoGraphEntity tier2 = mockEntity(TIER_2_OID, UIEntityType.COMPUTE_TIER,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_2_OID), TIER2_DISPLAY_NAME);

        final RepoGraphEntity vm = mockEntity(VM_OID, UIEntityType.VIRTUAL_MACHINE,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_1_OID, TARGET_2_OID), VM_DISPLAY_NAME);
        when(vm.getOwner()).thenReturn(Optional.of(ba1));
        final RepoGraphEntity poweredOffVm = mockEntity(DISABLED_VM_OID, UIEntityType.VIRTUAL_MACHINE,
            EntityState.POWERED_OFF, Sets.newHashSet(TARGET_2_OID), DISABLED_VM_DISPLAY_NAME);
        when(poweredOffVm.getOwner()).thenReturn(Optional.of(ba2));

        final RepoGraphEntity db = mockEntity(DB_OID, UIEntityType.DATABASE,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_1_OID), DB_DISPLAY_NAME);

        final Map<Long, RepoGraphEntity> entityMap = Stream.of(tier1, tier2, vm, poweredOffVm, db)
            .collect(Collectors.toMap(RepoGraphEntity::getOid, Function.identity()));
        entityLookup = (oid) -> Optional.ofNullable(entityMap.get(oid));


        when(mockSupplementaryDataFactory.newSupplementaryData(any(), any(), anyLong()))
            .thenReturn(mockSupplementaryData);
    }

    /**
     * Test calculating stats with no group-by criteria.
     */
    @Test
    public void testNoGroupBy() {
        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN, Collections.emptyList(), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, contains(SupplyChainStat.newBuilder()
            .setStatGroup(StatGroup.getDefaultInstance())
            .setNumEntities(4)
            .build()));
    }

    /**
     * Test calculating stats, grouping by business account.
     */
    @Test
    public void testGroupByBusinessAccount() {
        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN,
                Collections.singletonList(SupplyChainGroupBy.BUSINESS_ACCOUNT_ID), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder())
                .setNumEntities(2)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setAccountId(BUSINESS_ACCOUNT_1_OID))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setAccountId(BUSINESS_ACCOUNT_2_OID))
                .setNumEntities(1)
                .build()));
    }

    /**
     * Test calculating stats, grouping by resource group.
     */
    @Test
    public void testGroupByResourceGroup() {
        when(mockSupplementaryData.getResourceGroupId(VM_OID)).thenReturn(Optional.of(RESOURCE_GROUP_1_OID));
        when(mockSupplementaryData.getResourceGroupId(DISABLED_VM_OID)).thenReturn(Optional.of(RESOURCE_GROUP_1_OID));
        when(mockSupplementaryData.getResourceGroupId(DB_OID)).thenReturn(Optional.of(RESOURCE_GROUP_1_OID));
        when(mockSupplementaryData.getResourceGroupId(TIER_1_OID)).thenReturn(Optional.empty());
        when(mockSupplementaryData.getResourceGroupId(TIER_2_OID)).thenReturn(Optional.empty());
        final List<SupplyChainStat> stats = statistician.calculateStats(SUPPLY_CHAIN_VM_N_DB,
                Collections.singletonList(SupplyChainGroupBy.RESOURCE_GROUP), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
                SupplyChainStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder())
                        .setNumEntities(2)
                        .build(),
                SupplyChainStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder()
                                .setResourceGroupId(RESOURCE_GROUP_1_OID))
                        .setNumEntities(3)
                        .build()));
    }

    /**
     * Test calculating stats, grouping by resource group. When entities belong to different
     * resource groups or don't belong to any resource group at all.
     */
    @Test
    public void testGroupByResourceGroupWithAnyGroups() {
        when(mockSupplementaryData.getResourceGroupId(VM_OID)).thenReturn(Optional.of(RESOURCE_GROUP_1_OID));
        when(mockSupplementaryData.getResourceGroupId(DISABLED_VM_OID)).thenReturn(Optional.of(RESOURCE_GROUP_1_OID));
        when(mockSupplementaryData.getResourceGroupId(DB_OID)).thenReturn(Optional.of(RESOURCE_GROUP_2_OID));
        when(mockSupplementaryData.getResourceGroupId(TIER_1_OID)).thenReturn(Optional.empty());
        when(mockSupplementaryData.getResourceGroupId(TIER_2_OID)).thenReturn(Optional.empty());
        final List<SupplyChainStat> stats = statistician.calculateStats(SUPPLY_CHAIN_VM_N_DB,
                Collections.singletonList(SupplyChainGroupBy.RESOURCE_GROUP), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
                SupplyChainStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder())
                        .setNumEntities(2)
                        .build(),
                SupplyChainStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder()
                                .setResourceGroupId(RESOURCE_GROUP_2_OID))
                        .setNumEntities(1)
                        .build(),
                SupplyChainStat.newBuilder()
                        .setStatGroup(StatGroup.newBuilder()
                                .setResourceGroupId(RESOURCE_GROUP_1_OID))
                        .setNumEntities(2)
                        .build()));
    }

    /**
     * Test calculating stats, grouping by discovering target id.
     */
    @Test
    public void testGroupByTarget() {
        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN,
                Collections.singletonList(SupplyChainGroupBy.TARGET), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetId(TARGET_1_OID))
                .setNumEntities(2)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetId(TARGET_2_OID))
                .setNumEntities(4)
                .build()));
    }

    /**
     * Test calculating stats, grouping by entity type.
     */
    @Test
    public void testGroupByEntityType() {
        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN,
                Collections.singletonList(SupplyChainGroupBy.ENTITY_TYPE), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                .setNumEntities(2)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setEntityType(UIEntityType.COMPUTE_TIER.typeNumber()))
                .setNumEntities(2)
                .build()));
    }

    /**
     * Test calculating stats, grouping by entity state.
     */
    @Test
    public void testGroupByEntityState() {
        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN,
                Collections.singletonList(SupplyChainGroupBy.ENTITY_STATE), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setEntityState(EntityState.POWERED_ON))
                .setNumEntities(3)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setEntityState(EntityState.POWERED_OFF))
                .setNumEntities(1)
                .build()));
    }

    /**
     * Test calculating stats, grouping by severity.
     */
    @Test
    public void testGroupBySeverity() {
        when(mockSupplementaryData.getSeverity(anyLong())).thenReturn(Severity.NORMAL);
        when(mockSupplementaryData.getSeverity(VM_OID)).thenReturn(Severity.CRITICAL);

        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN,
                Collections.singletonList(SupplyChainGroupBy.SEVERITY), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setSeverity(Severity.NORMAL))
                .setNumEntities(3)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setSeverity(Severity.CRITICAL))
                .setNumEntities(1)
                .build()));
    }

    /**
     * Test calculating stats, grouping by action category.
     */
    @Test
    public void testGroupByCategory() {
        when(mockSupplementaryData.getCategories(anyLong())).thenReturn(Collections.emptySet());
        when(mockSupplementaryData.getCategories(VM_OID)).thenReturn(Sets.newHashSet(
            ActionCategory.PERFORMANCE_ASSURANCE,
            ActionCategory.EFFICIENCY_IMPROVEMENT));

        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN,
                Collections.singletonList(SupplyChainGroupBy.ACTION_CATEGORY), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                // Unset because 3 entities have no categories.
                .setStatGroup(StatGroup.newBuilder())
                .setNumEntities(3)
                .build()));
    }

    /**
     * Test calculating stats, grouping by target, entity type, and action category.
     */
    @Test
    public void testByTargetAndEntityTypeAndCategories() {
        when(mockSupplementaryData.getCategories(anyLong())).thenReturn(Collections.emptySet());
        when(mockSupplementaryData.getCategories(VM_OID)).thenReturn(Sets.newHashSet(
            ActionCategory.PERFORMANCE_ASSURANCE));

        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN,
                Arrays.asList(SupplyChainGroupBy.TARGET, SupplyChainGroupBy.ENTITY_TYPE,
                        SupplyChainGroupBy.ACTION_CATEGORY), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetId(TARGET_1_OID)
                    .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                    .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetId(TARGET_2_OID)
                    // 1 VM with no action category
                    .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetId(TARGET_2_OID)
                    // 1 VM with PERFORMANCE_ASSURANCE
                    .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
                    .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetId(TARGET_1_OID)
                    .setEntityType(UIEntityType.COMPUTE_TIER.typeNumber()))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTargetId(TARGET_2_OID)
                    .setEntityType(UIEntityType.COMPUTE_TIER.typeNumber()))
                .setNumEntities(2)
                .build()));
    }

    /**
     * Test the group by functionality for supply chain when we are grouping by template.
     */
    @Test
    public void testGroupByTemplate() {
        // ARRANGE
        final RepoGraphEntity vm1 = entityLookup.getEntity(VM_OID).get();
        final RepoGraphEntity vm2 = entityLookup.getEntity(DISABLED_VM_OID).get();
        final RepoGraphEntity db = entityLookup.getEntity(DB_OID).get();
        final RepoGraphEntity tier = entityLookup.getEntity(TIER_1_OID).get();
        final RepoGraphEntity storageTier = mockEntity(11L, UIEntityType.STORAGE_TIER,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_2_OID), "SMALL");
        final RepoGraphEntity dbTier = mockEntity(15L, UIEntityType.DATABASE_TIER,
            EntityState.POWERED_ON, Sets.newHashSet(TARGET_2_OID), "db.t2.micro");

        when(vm1.getProviders()).thenReturn(ImmutableList.of(storageTier, tier));
        when(vm2.getProviders()).thenReturn(Collections.emptyList());
        when(db.getProviders()).thenReturn(ImmutableList.of(dbTier));

        // ACT
        final List<SupplyChainStat> stats =
            statistician.calculateStats(SUPPLY_CHAIN_VM_N_DB,
                Arrays.asList(SupplyChainGroupBy.TEMPLATE), entityLookup, REALTIME_TOPOLOGY_CONTEXT_ID);

        // ASSERT
        assertThat(stats.size(), is(3));
        assertThat(stats, containsInAnyOrder(
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTemplate("t2.micro"))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                .setStatGroup(StatGroup.newBuilder()
                    .setTemplate("db.t2.micro"))
                .setNumEntities(1)
                .build(),
            SupplyChainStat.newBuilder()
                // Unset because 3 entities have no template.
                .setStatGroup(StatGroup.newBuilder())
                .setNumEntities(3)
                .build()));
    }

    /**
     * Test that the {@link SupplementaryDataFactory} requests and parses action category
     * information for supply chain entities.
     */
    @Test
    public void testSupplementaryFactoryActionCategories() {
        final SupplementaryDataFactory factory = new SupplementaryDataFactory(
            EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel())
        );

        final List<Long> entities = Arrays.asList(1L, 2L);

        when(actionsServiceMole.getCurrentActionStats(any()))
            .thenReturn(GetCurrentActionStatsResponse.newBuilder()
                .addResponses(SingleResponse.newBuilder()
                    .setQueryId(1L)
                    .addActionStats(CurrentActionStat.newBuilder()
                        .setStatGroup(CurrentActionStat.StatGroup.newBuilder()
                            .setTargetEntityId(1L)
                            .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE))
                        .setActionCount(1))
                    .addActionStats(CurrentActionStat.newBuilder()
                        .setStatGroup(CurrentActionStat.StatGroup.newBuilder()
                            .setTargetEntityId(1L)
                            .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT))
                        .setActionCount(1)))
                    .build());

        final SupplementaryData suppData = factory.newSupplementaryData(
            entities, Collections.singletonList(SupplyChainGroupBy.ACTION_CATEGORY), REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(suppData.getCategories(1L),
            containsInAnyOrder(ActionCategory.PERFORMANCE_ASSURANCE, ActionCategory.EFFICIENCY_IMPROVEMENT));
        assertThat(suppData.getCategories(2L),
            is(Collections.emptySet()));

        verify(actionsServiceMole).getCurrentActionStats(GetCurrentActionStatsRequest.newBuilder()
            .addQueries(SingleQuery.newBuilder()
                .setQueryId(1L)
                .setQuery(CurrentActionStatsQuery.newBuilder()
                    .setScopeFilter(ScopeFilter.newBuilder()
                        .setEntityList(EntityScope.newBuilder()
                            .addAllOids(entities)))
                    .addGroupBy(GroupBy.TARGET_ENTITY_ID)
                    .addGroupBy(GroupBy.ACTION_CATEGORY)))
            .build());
    }

    /**
     * Test that the {@link SupplementaryDataFactory} requests and parses entity severities
     * for supply chain entities.
     */
    @Test
    public void testSupplementaryFactorySeverities() {
        final SupplementaryDataFactory factory = new SupplementaryDataFactory(
            EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        final List<Long> entities = Arrays.asList(1L, 2L, 3L);

        when(entitySeverityServiceBackend.getEntitySeverities(any()))
            .thenReturn(Collections.singletonList(EntitySeveritiesResponse.newBuilder()
                .setEntitySeverity(EntitySeveritiesChunk.newBuilder().addEntitySeverity(EntitySeverity.newBuilder()
                    .setEntityId(1L)
                    .setSeverity(Severity.CRITICAL))
                    .addEntitySeverity(EntitySeverity.newBuilder()
                        .setEntityId(2L)
                        .setSeverity(Severity.MAJOR))
                    .build()).build()));


        final SupplementaryData suppData = factory.newSupplementaryData(
            entities, Collections.singletonList(SupplyChainGroupBy.SEVERITY), REALTIME_TOPOLOGY_CONTEXT_ID);
        assertThat(suppData.getSeverity(1L), is(Severity.CRITICAL));
        assertThat(suppData.getSeverity(2L), is(Severity.MAJOR));
        // Normal by default
        assertThat(suppData.getSeverity(3L), is(Severity.NORMAL));

        verify(entitySeverityServiceBackend).getEntitySeverities(MultiEntityRequest.newBuilder()
            .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
            .addAllEntityIds(entities)
            .build());
    }

    /**
     * Test resourceGroups population related to entities.
     */
    @Test
    public void testSupplementaryDataGroupByResourceGroup() {
        final long entityId1 = 1L;
        final long entityId2 = 2L;
        final long resourceGroupId1 = 12L;
        final SupplementaryDataFactory factory = new SupplementaryDataFactory(
                EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        final List<Long> entities = Arrays.asList(entityId1, entityId2);

        final Map<Long, Groupings> entityToGroupsMap = new HashMap<>();
        entityToGroupsMap.put(1L, Groupings.newBuilder().addGroupId(resourceGroupId1).build());

        final GetGroupsForEntitiesResponse getGroupsForEntitiesResponse =
                GetGroupsForEntitiesResponse.newBuilder()
                        .putAllEntityGroup(entityToGroupsMap)
                        .build();
        when(groupServiceMole.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                .addGroupType(GroupType.RESOURCE)
                .addAllEntityId(entities)
                .build())).thenReturn(getGroupsForEntitiesResponse);

        final SupplementaryData supplementaryData = factory.newSupplementaryData(entities,
                Collections.singletonList(SupplyChainGroupBy.RESOURCE_GROUP), REALTIME_TOPOLOGY_CONTEXT_ID);
        assertTrue(supplementaryData.getResourceGroupId(entityId1).isPresent());
        assertEquals(Long.valueOf(resourceGroupId1), supplementaryData.getResourceGroupId(entityId1).get());
        assertFalse(supplementaryData.getResourceGroupId(entityId2).isPresent());
    }

}
