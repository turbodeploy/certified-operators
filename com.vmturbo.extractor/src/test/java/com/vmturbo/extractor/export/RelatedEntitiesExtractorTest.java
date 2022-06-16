package com.vmturbo.extractor.export;

import static com.vmturbo.extractor.schema.enums.EntityType.BILLING_FAMILY;
import static com.vmturbo.extractor.schema.enums.EntityType.BUSINESS_ACCOUNT;
import static com.vmturbo.extractor.schema.enums.EntityType.COMPUTE_CLUSTER;
import static com.vmturbo.extractor.schema.enums.EntityType.DATABASE;
import static com.vmturbo.extractor.schema.enums.EntityType.DATACENTER;
import static com.vmturbo.extractor.schema.enums.EntityType.DISK_ARRAY;
import static com.vmturbo.extractor.schema.enums.EntityType.GROUP;
import static com.vmturbo.extractor.schema.enums.EntityType.NETWORK;
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
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.action.ActionConverter;
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
    private static final long dcId = 621;
    private static final long daId = 721;
    private static final long nwId = 821;

    private static final long missingEntityOid = 1234567;

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
    private ExtractorFeatureFlags featureFlags;
    private RelatedEntitiesExtractor relatedEntitiesExtractor;

    // mock topology
    private final SupplyChainEntity vmEntity = mockEntity(vmId, EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE);
    private final SupplyChainEntity dbEntity = mockEntity(dbId, EntityDTO.EntityType.DATABASE_VALUE);
    private final SupplyChainEntity stEntity1 = mockEntity(stId1, EntityDTO.EntityType.STORAGE_VALUE);
    private final SupplyChainEntity stEntity2 = mockEntity(stId2, EntityDTO.EntityType.STORAGE_VALUE);
    private final SupplyChainEntity pmEntity = mockEntity(pmId, EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE);
    private final SupplyChainEntity account1 = mockEntity(accountId1, EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE);
    private final SupplyChainEntity account2 = mockEntity(accountId2, EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE);
    private final SupplyChainEntity dcEntity = mockEntity(dcId, EntityDTO.EntityType.DATACENTER_VALUE);
    private final SupplyChainEntity daEntity = mockEntity(daId, EntityDTO.EntityType.DISK_ARRAY_VALUE);
    private final SupplyChainEntity nwEntity = mockEntity(nwId, EntityDTO.EntityType.NETWORK_VALUE);

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
                EntityDTO.EntityType.PHYSICAL_MACHINE.getNumber(), ImmutableSet.of(pmId),
                EntityDTO.EntityType.NETWORK.getNumber(), ImmutableSet.of(nwId))
        ).when(supplyChain).getRelatedEntities(vmId);

        // mock groups
        doReturn(ImmutableList.of(USER_VM_GROUP)).when(groupData).getGroupsForEntity(vmId);
        doReturn(ImmutableList.of(CLUSTER_1)).when(groupData).getGroupsForEntity(pmId);
        doReturn(ImmutableList.of(STORAGE_CLUSTER_1)).when(groupData).getGroupsForEntity(stId1);
        doReturn(ImmutableList.of(STORAGE_CLUSTER_1)).when(groupData).getGroupsForEntity(stId2);
        doReturn(ImmutableList.of(RESOURCE_GROUP_1)).when(groupData).getGroupsForEntity(dbId);
        doReturn(ImmutableList.of(BILLING_FAMILY_1)).when(groupData).getGroupsForEntity(accountId1);
        doReturn(ImmutableList.of(BILLING_FAMILY_1)).when(groupData).getGroupsForEntity(accountId2);

        featureFlags = mock(ExtractorFeatureFlags.class);

        when(topologyGraph.getEntity(missingEntityOid)).thenReturn(Optional.empty());

        relatedEntitiesExtractor = new RelatedEntitiesExtractor(topologyGraph, supplyChain, groupData,
                featureFlags);
    }

    /**
     * Test that related entities and groups are extracted as expected.
     */
    @Test
    public void testRelatedEntitiesAndGroups() {
        final Map<String, List<RelatedEntity>> relatedEntities =
                relatedEntitiesExtractor.extractRelatedEntities(vmId);

        // verify
        assertThat(relatedEntities.size(), is(8));
        assertThat(relatedEntities.keySet(), containsInAnyOrder(DATABASE.getLiteral(),
                PHYSICAL_MACHINE.getLiteral(), STORAGE.getLiteral(), GROUP.getLiteral(),
                STORAGE_CLUSTER.getLiteral(), COMPUTE_CLUSTER.getLiteral(), RESOURCE_GROUP.getLiteral(),
                NETWORK.getLiteral()));
        // related entity
        assertThat(getRelatedEntityIds(relatedEntities, DATABASE), containsInAnyOrder(dbId));
        assertThat(getRelatedEntityIds(relatedEntities, PHYSICAL_MACHINE), containsInAnyOrder(pmId));
        assertThat(getRelatedEntityIds(relatedEntities, STORAGE), containsInAnyOrder(stId1, stId2));
        assertThat(getRelatedEntityIds(relatedEntities, NETWORK), containsInAnyOrder(nwId));
        // related group
        assertThat(getRelatedEntityIds(relatedEntities, GROUP), containsInAnyOrder(USER_VM_GROUP.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER_1.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, COMPUTE_CLUSTER), containsInAnyOrder(CLUSTER_1.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, RESOURCE_GROUP), containsInAnyOrder(RESOURCE_GROUP_1.getId()));
    }

    /**
     * Enables the entity relationship whitelist and tests that it works.
     */
    @Test
    public void testRelatedEntitiesWhitelist() {

        // ARRANGE - enable tje whitelist
        when(featureFlags.isEntityRelationWhitelistEnabled()).thenReturn(true);

        // ACT
        Map<String, List<RelatedEntity>> relatedEntities =
                relatedEntitiesExtractor.extractRelatedEntities(vmId);

        // ASSERT - Here we filter out the DATABASE and NETWORK entities. This also results in
        // losing the RESOURCE_GROUP relationship because it was brought in by the related DATABASE.
        assertThat(relatedEntities.size(), is(5));
        assertThat(relatedEntities.keySet(), containsInAnyOrder(PHYSICAL_MACHINE.getLiteral(),
                STORAGE.getLiteral(), GROUP.getLiteral(), STORAGE_CLUSTER.getLiteral(),
                COMPUTE_CLUSTER.getLiteral()));
        // related entity
        assertThat(getRelatedEntityIds(relatedEntities, PHYSICAL_MACHINE), containsInAnyOrder(pmId));
        assertThat(getRelatedEntityIds(relatedEntities, STORAGE), containsInAnyOrder(stId1, stId2));
        // related group
        assertThat(getRelatedEntityIds(relatedEntities, GROUP), containsInAnyOrder(USER_VM_GROUP.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER_1.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, COMPUTE_CLUSTER), containsInAnyOrder(CLUSTER_1.getId()));
    }

    /**
     * Enables the entity relationship whitelist and tests that it works.
     */
    @Test
    public void testRelatedEntitiesWhitelistWithEntityThatIsMissingFromTopologyGraph() {

        // ARRANGE - enable the whitelist
        when(featureFlags.isEntityRelationWhitelistEnabled()).thenReturn(true);

        // ACT
        Map<String, List<RelatedEntity>> relatedEntities =
                relatedEntitiesExtractor.extractRelatedEntities(1234567L);

        // ASSERT
        assertThat(
                "Expecting related entities to be null when the entity is missing from the topology graph",
                relatedEntities == null);
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
     * Test that related entities are filtered correctly.
     */
    @Test
    public void testRelatedEntitiesWithFilter() {
        // mock supply chain
        doReturn(ImmutableMap.of(
                EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber(), ImmutableSet.of(vmId),
                EntityDTO.EntityType.STORAGE.getNumber(), ImmutableSet.of(stId1),
                EntityDTO.EntityType.DISK_ARRAY.getNumber(), ImmutableSet.of(daId),
                EntityDTO.EntityType.PHYSICAL_MACHINE.getNumber(), ImmutableSet.of(pmId),
                EntityDTO.EntityType.DATACENTER.getNumber(), ImmutableSet.of(dcId))
        ).when(supplyChain).getRelatedEntities(stId1);

        final Map<String, List<RelatedEntity>> relatedEntities =
                relatedEntitiesExtractor.extractRelatedEntities(stId1,
                        ActionConverter.RELATED_ENTITY_FILTER_FOR_ACTION_TARGET_ENTITY.get(
                                EntityDTO.EntityType.STORAGE_VALUE));
        // verify
        assertThat(relatedEntities.size(), is(5));
        assertThat(relatedEntities.keySet(), containsInAnyOrder(DISK_ARRAY.getLiteral(),
                DATACENTER.getLiteral(), PHYSICAL_MACHINE.getLiteral(),
                STORAGE_CLUSTER.getLiteral(), COMPUTE_CLUSTER.getLiteral()));
        // related entity
        assertThat(getRelatedEntityIds(relatedEntities, DISK_ARRAY), containsInAnyOrder(daId));
        assertThat(getRelatedEntityIds(relatedEntities, DATACENTER), containsInAnyOrder(dcId));
        assertThat(getRelatedEntityIds(relatedEntities, PHYSICAL_MACHINE), containsInAnyOrder(pmId));
        // related group
        assertThat(getRelatedEntityIds(relatedEntities, STORAGE_CLUSTER), containsInAnyOrder(STORAGE_CLUSTER_1.getId()));
        assertThat(getRelatedEntityIds(relatedEntities, COMPUTE_CLUSTER), containsInAnyOrder(CLUSTER_1.getId()));
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