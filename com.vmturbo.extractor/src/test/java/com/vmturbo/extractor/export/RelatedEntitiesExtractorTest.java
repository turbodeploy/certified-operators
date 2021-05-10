package com.vmturbo.extractor.export;

import static com.vmturbo.extractor.schema.enums.EntityType.BILLING_FAMILY;
import static com.vmturbo.extractor.schema.enums.EntityType.BUSINESS_ACCOUNT;
import static com.vmturbo.extractor.schema.enums.EntityType.COMPUTE_CLUSTER;
import static com.vmturbo.extractor.schema.enums.EntityType.DATABASE;
import static com.vmturbo.extractor.schema.enums.EntityType.GROUP;
import static com.vmturbo.extractor.schema.enums.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.extractor.schema.enums.EntityType.RESOURCE_GROUP;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE_CLUSTER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.User;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.json.export.RelatedEntity;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.GroupFetcher.GroupData;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Test for {@link RelatedEntitiesExtractor}.
 */
public class RelatedEntitiesExtractorTest {

    private static final long vmId = 121;
    private static final long stId1 = 221;
    private static final long stId2 = 222;
    private static final long pmId = 321;
    private static final long dbId = 421;
    // master account
    private static final long accountId1 = 521;
    // sub account
    private static final long accountId2 = 522;
    private static final Grouping CLUSTER_1 = Grouping.newBuilder()
            .setId(1234)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.COMPUTE_HOST_CLUSTER)
                    .setDisplayName("cluster1"))
            .build();
    private static final Grouping STORAGE_CLUSTER_1 = Grouping.newBuilder()
            .setId(1235)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.STORAGE_CLUSTER)
                    .setDisplayName("storageCluster1"))
            .build();
    private static final Grouping USER_VM_GROUP = Grouping.newBuilder()
            .setId(1236)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.REGULAR)
                    .setDisplayName("userVMGroup1"))
            .setOrigin(Origin.newBuilder().setUser(User.getDefaultInstance()))
            .build();
    private static final Grouping RESOURCE_GROUP_1 = Grouping.newBuilder()
            .setId(1237)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.RESOURCE)
                    .setDisplayName("rg1"))
            .build();
    private static final Grouping BILLING_FAMILY_1 = Grouping.newBuilder()
            .setId(1238)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.BILLING_FAMILY)
                    .setDisplayName("bf1"))
            .build();

    private final SupplyChain supplyChain = mock(SupplyChain.class);
    private final GroupData groupData = mock(GroupData.class);
    private final TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);
    private RelatedEntitiesExtractor relatedEntitiesExtractor;

    // mock topology
    private final SupplyChainEntity vmEntity = mockEntity(vmId, EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE);
    private final SupplyChainEntity dbEntity = mockEntity(dbId, EntityDTO.EntityType.DATABASE_VALUE);
    private final SupplyChainEntity stEntity1 = mockEntity(stId1, EntityDTO.EntityType.STORAGE_VALUE);
    private final SupplyChainEntity stEntity2 = mockEntity(stId2, EntityDTO.EntityType.STORAGE_VALUE);
    private final SupplyChainEntity pmEntity = mockEntity(pmId, EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE);
    private final SupplyChainEntity account1 = mockEntity(accountId1, EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE);
    private final SupplyChainEntity account2 = mockEntity(accountId2, EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE);

    /**
     * Setup before each test.
     */
    @Before
    public void setUp() {
        // mock supply chain
        doReturn(ImmutableMap.of(
                EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber(), ImmutableSet.of(vmId),
                EntityDTO.EntityType.DATABASE.getNumber(), ImmutableSet.of(dbId),
                EntityDTO.EntityType.STORAGE.getNumber(), ImmutableSet.of(stId1, stId2),
                EntityDTO.EntityType.PHYSICAL_MACHINE.getNumber(), ImmutableSet.of(pmId))
        ).when(supplyChain).getRelatedEntities(vmId);

        // mock groups
        doReturn(ImmutableList.of(USER_VM_GROUP)).when(groupData).getGroupsForEntity(vmId);
        doReturn(ImmutableList.of(CLUSTER_1)).when(groupData).getGroupsForEntity(pmId);
        doReturn(ImmutableList.of(STORAGE_CLUSTER_1)).when(groupData).getGroupsForEntity(stId1);
        doReturn(ImmutableList.of(STORAGE_CLUSTER_1)).when(groupData).getGroupsForEntity(stId2);
        doReturn(ImmutableList.of(RESOURCE_GROUP_1)).when(groupData).getGroupsForEntity(dbId);
        doReturn(ImmutableList.of(BILLING_FAMILY_1)).when(groupData).getGroupsForEntity(accountId1);
        doReturn(ImmutableList.of(BILLING_FAMILY_1)).when(groupData).getGroupsForEntity(accountId2);

        relatedEntitiesExtractor = new RelatedEntitiesExtractor(topologyGraph, supplyChain, groupData);
    }

    /**
     * Test that related entities and groups are extracted as expected.
     */
    @Test
    public void testRelatedEntitiesAndGroups() {
        final Map<String, List<RelatedEntity>> relatedEntities =
                relatedEntitiesExtractor.extractRelatedEntities(vmId);

        // verify
        assertThat(relatedEntities.size(), is(7));
        assertThat(relatedEntities.keySet(), containsInAnyOrder(DATABASE.getLiteral(),
                PHYSICAL_MACHINE.getLiteral(), STORAGE.getLiteral(), GROUP.getLiteral(),
                STORAGE_CLUSTER.getLiteral(), COMPUTE_CLUSTER.getLiteral(), RESOURCE_GROUP.getLiteral()));
        // related entity
        assertThat(getRelatedEntityIds(relatedEntities, DATABASE), containsInAnyOrder(dbId));
        assertThat(getRelatedEntityIds(relatedEntities, PHYSICAL_MACHINE), containsInAnyOrder(pmId));
        assertThat(getRelatedEntityIds(relatedEntities, STORAGE), containsInAnyOrder(stId1, stId2));
        // related group
        assertThat(getRelatedEntityIds(relatedEntities, GROUP), containsInAnyOrder(USER_VM_GROUP.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER_1.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, COMPUTE_CLUSTER), containsInAnyOrder(CLUSTER_1.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, RESOURCE_GROUP), containsInAnyOrder(RESOURCE_GROUP_1.getId()));
    }

    /**
     * Test that related accounts are extracted correctly. If an entity is owned by sub account,
     * sub account is owned by master account, then only sub account should be included in related
     * accounts.
     */
    @Test
    public void testRelatedAccount() {
        // case1: vm is owned by account2, account2 is owned by account1
        doReturn(Optional.of(account2)).when(vmEntity).getOwner();
        doReturn(Optional.of(account1)).when(account2).getOwner();
        doReturn(ImmutableMap.of(
                EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber(), ImmutableSet.of(vmId),
                EntityDTO.EntityType.BUSINESS_ACCOUNT.getNumber(), ImmutableSet.of(accountId1, accountId2))
        ).when(supplyChain).getRelatedEntities(vmId);
        Map<String, List<RelatedEntity>> relatedEntities = relatedEntitiesExtractor.extractRelatedEntities(vmId);
        // verify that master account is not included
        assertThat(relatedEntities.size(), is(3));
        assertThat(relatedEntities.keySet(), containsInAnyOrder(
                BUSINESS_ACCOUNT.getLiteral(), BILLING_FAMILY.getLiteral(), GROUP.getLiteral()));
        assertThat(getRelatedEntityIds(relatedEntities, BUSINESS_ACCOUNT), containsInAnyOrder(accountId2));
        assertThat(getRelatedEntityIds(relatedEntities, BILLING_FAMILY), containsInAnyOrder(BILLING_FAMILY_1.getId()));

        // case2: vm is owned by master account1
        doReturn(Optional.of(account1)).when(vmEntity).getOwner();
        doReturn(ImmutableMap.of(
                EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber(), ImmutableSet.of(vmId),
                EntityDTO.EntityType.BUSINESS_ACCOUNT.getNumber(), ImmutableSet.of(accountId1))
        ).when(supplyChain).getRelatedEntities(vmId);
        relatedEntities = relatedEntitiesExtractor.extractRelatedEntities(vmId);
        // verify that master account is included
        assertThat(relatedEntities.size(), is(3));
        assertThat(relatedEntities.keySet(), containsInAnyOrder(
                BUSINESS_ACCOUNT.getLiteral(), BILLING_FAMILY.getLiteral(), GROUP.getLiteral()));
        assertThat(getRelatedEntityIds(relatedEntities, BUSINESS_ACCOUNT), containsInAnyOrder(accountId1));
        assertThat(getRelatedEntityIds(relatedEntities, BILLING_FAMILY), containsInAnyOrder(BILLING_FAMILY_1.getId()));
    }

    /**
     * Mock a {@link SupplyChainEntity}.
     *
     * @param entityId   Given entity ID.
     * @param entityType Given entity Type.
     * @return mocked entity
     */
    private SupplyChainEntity mockEntity(long entityId, int entityType) {
        SupplyChainEntity entity = mock(SupplyChainEntity.class);
        when(entity.getOid()).thenReturn(entityId);
        when(entity.getEntityType()).thenReturn(entityType);
        when(entity.getDisplayName()).thenReturn(String.valueOf(entityId));
        doReturn(Optional.of(entity)).when(topologyGraph).getEntity(entityId);
        return entity;
    }

    /**
     * Get ids of related entities from given map.
     *
     * @param relatedEntities related entities by type
     * @param entityType type of related entity
     * @return list of related entity ids
     */
    private static List<Long> getRelatedEntityIds(Map<String, List<RelatedEntity>> relatedEntities,
            EntityType entityType) {
        return relatedEntities.get(entityType.getLiteral()).stream()
                .map(RelatedEntity::getOid)
                .collect(Collectors.toList());
    }
}