package com.vmturbo.topology.processor.topology;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
import static com.vmturbo.topology.processor.topology.CloudMigrationPlanHelper.COMMODITIES_TO_SKIP;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.loadTopologyBuilderDTO;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.loadTopologyInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.topology.CloudMigrationPlanHelper.CloudMigrationSettingsPolicyEditor;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * Test coverage for cloud migration stage helper.
 */
public class CloudMigrationPlanHelperTest {
    private static final String IRRELEVANT_SETTING_POLICY = "Irrelevant";

    /**
     * Single helper instance.
     */
    private final CloudMigrationPlanHelper cloudMigrationPlanHelper;

    /**
     * Pipeline context containing source entities etc.
     */
    private final TopologyPipelineContext context;

    /**
     * Group service dependency.
     */
    private final GroupServiceBlockingStub groupServiceClient;

    /**
     * Stats History service dependency.
     */
    private final StatsHistoryServiceBlockingStub statsHistoryServiceBlockingStub;

    /**
     * On-prem source VM DTO data read from file.
     */
    private TopologyEntityDTO.Builder vm1OnPrem;

    /**
     * Azure source VM DTO data read from file.
     */
    private TopologyEntityDTO.Builder vm1Azure;

    /**
     * AWS source VM DTO data read from file.
     */
    private TopologyEntityDTO.Builder vm1Aws;

    /**
     * Allocation plan topology info read from file.
     */
    private TopologyInfo allocationTopologyInfo;

    /**
     * Consumption plan topology info read from file.
     */
    private TopologyInfo consumptionTopologyInfo;

    private static final long EXISTING_SETTING_POLICY_GROUP_OID = 100;
    private static final Set<Long> DISCOVERED_EXCLUDED_TIER_OIDS = Sets.newHashSet(101L, 102L, 103L);
    private static final Set<Long> EXISTING_CLOUD_VM_OIDS = Sets.newHashSet(1L, 2L, 3L);
    private static final Set<Long> MIGRATING_VM_OIDS = Sets.newHashSet(4L, 5L, 6L);

    /**
     * Creates new instance with dependencies.
     */
    public CloudMigrationPlanHelperTest() {
        context = mock(TopologyPipelineContext.class);
        groupServiceClient = mock(GroupConfig.class).groupServiceBlockingStub();
        statsHistoryServiceBlockingStub = mock(TopologyConfig.class).historyClient();
        cloudMigrationPlanHelper = new CloudMigrationPlanHelper(groupServiceClient, statsHistoryServiceBlockingStub);
    }

    /**
     * Sets up topology data after reading from data files.
     */
    @Before
    public void setup() {
        vm1OnPrem = loadTopologyBuilderDTO("cloud-migration-vm-1-onprem.json");
        allocationTopologyInfo = loadTopologyInfo("cloud-migration-topo-info-allocation.json");
        consumptionTopologyInfo = loadTopologyInfo("cloud-migration-topo-info-consumption.json");
        vm1Azure = loadTopologyBuilderDTO("cloud-migration-vm-1-azure.json");
        vm1Aws = loadTopologyBuilderDTO("cloud-migration-vm-1-aws.json");

        IdentityGenerator.initPrefix(1L);
    }

    /**
     * Helper settings class to help verify before and after commBought settings values.
     */
    private static class CommBoughtExpectedTestSettings {
        /**
         * ID of host or compute tier for source entity being migrated.
         */
        long hostProviderId = 0;

        /**
         * For attached disk 1, storage or Volume provider id for source entity being migrated.
         */
        long storageProviderId1 = 0;

        /**
         * Optional 2nd attached disk storage/volume provider id.
         */
        @Nullable Long storageProviderId2 = null;

        /**
         * Volume id, used for on-prem storage.
         */
        long volumeId = 0;

        /**
         * Expected commBoughtDTO per provider, used for count verifications before and after.
         */
        final Map<Long, Long> countsByProvider = new HashMap<>();

        /**
         * Total commodities that were skipped/ignored.
         */
        int totalSkipped = 0;

        /**
         * Total access commodities currently.
         */
        int totalAccess = 0;

        /**
         * Total commodities that are marked as inactive currently.
         */
        int totalInactive = 0;

        /**
         * Whether commBoughtGrouping is movable or not. If not, movable assert check is ignored.
         */
        @Nullable Boolean movable = null;
    }

    /**
     * Checks if bought commodities are being setup correctly for on-prem source entities.
     * Methods covered:
     *      prepareBoughtCommodities()
     *      getUpdatedCommBought()
     */
    @Test
    public void prepareBoughtCommoditiesOnPremSource() {
        assertNotNull(vm1OnPrem);
        assertNotNull(allocationTopologyInfo);

        CommBoughtExpectedTestSettings settings = new CommBoughtExpectedTestSettings();
        settings.hostProviderId = 73433887033680L;
        settings.storageProviderId1 = 73433887031974L;
        settings.volumeId = 73433887060893L;

        settings.movable = false;
        settings.totalSkipped = 5;
        settings.totalAccess = 6;
        settings.totalInactive = 0;
        settings.countsByProvider.put(settings.hostProviderId, 13L);
        settings.countsByProvider.put(settings.storageProviderId1, 6L);

        // Check counts before.
        verifyCommBoughtCounts(vm1OnPrem, settings);

        // Inactive commodities are only being done for allocation plan, rest of the behavior
        // of this method is the same across plans, so pass in allocation topology info.
        cloudMigrationPlanHelper.prepareBoughtCommodities(vm1OnPrem, allocationTopologyInfo, Collections.emptyMap(), true);

        settings.movable = true;
        settings.totalSkipped = 0;
        settings.totalAccess = 0;
        settings.totalInactive = 3;
        settings.countsByProvider.put(settings.hostProviderId, 4L);
        settings.countsByProvider.put(settings.storageProviderId1, 4L);

        // Check counts after.
        verifyCommBoughtCounts(vm1OnPrem, settings);

        // Check storage commBought values.
        verifyStorageCommBought(vm1OnPrem, settings);
    }

    /**
     * Checks if bought commodities are being setup correctly for Azure source entities.
     * Methods covered:
     *      prepareBoughtCommodities()
     *      getUpdatedCommBought()
     */
    @Test
    public void prepareBoughtCommoditiesAzureSource() {
        assertNotNull(vm1Azure);
        assertNotNull(allocationTopologyInfo);

        CommBoughtExpectedTestSettings settings = new CommBoughtExpectedTestSettings();
        settings.hostProviderId = 73320334249387L;
        settings.storageProviderId1 = 73320335294658L;
        settings.storageProviderId2 = 73320335294657L;
        settings.volumeId = 0;

        settings.movable = false;
        settings.totalSkipped = 3;
        settings.totalAccess = 4;
        settings.totalInactive = 2;
        settings.countsByProvider.put(settings.hostProviderId, 8L);
        settings.countsByProvider.put(settings.storageProviderId1, 6L);
        settings.countsByProvider.put(settings.storageProviderId2, 6L);

        // Check counts before.
        verifyCommBoughtCounts(vm1Azure, settings);

        // Inactive commodities are only being done for allocation plan, rest of the behavior
        // of this method is the same across plans, so pass in allocation topology info.
        cloudMigrationPlanHelper.prepareBoughtCommodities(vm1Azure, allocationTopologyInfo, Collections.emptyMap(), true);

        settings.movable = true;
        settings.totalSkipped = 0;
        settings.totalAccess = 0;
        settings.totalInactive = 6;
        settings.countsByProvider.put(settings.hostProviderId, 3L);
        settings.countsByProvider.put(settings.storageProviderId1, 5L);
        settings.countsByProvider.put(settings.storageProviderId2, 5L);

        // Check counts after.
        verifyCommBoughtCounts(vm1Azure, settings);

        // Check storage commBought values.
        verifyStorageCommBought(vm1Azure, settings);
    }

    /**
     * Checks if bought commodities are being setup correctly for AWS source entities.
     * Methods covered:
     *      prepareBoughtCommodities()
     *      getUpdatedCommBought()
     */
    @Test
    public void prepareBoughtCommoditiesAwsSource() {
        assertNotNull(vm1Aws);
        assertNotNull(allocationTopologyInfo);

        CommBoughtExpectedTestSettings settings = new CommBoughtExpectedTestSettings();
        settings.hostProviderId = 73320835644202L;
        settings.storageProviderId1 = 73320835644316L;
        settings.volumeId = 0;

        settings.movable = false;
        settings.totalSkipped = 2;
        settings.totalAccess = 3;
        settings.totalInactive = 0;
        settings.countsByProvider.put(settings.hostProviderId, 9L);
        settings.countsByProvider.put(settings.storageProviderId1, 6L);

        // Check counts before.
        verifyCommBoughtCounts(vm1Aws, settings);

        // Inactive commodities are only being done for allocation plan, rest of the behavior
        // of this method is the same across plans, so pass in allocation topology info.
        cloudMigrationPlanHelper.prepareBoughtCommodities(vm1Aws, allocationTopologyInfo, Collections.emptyMap(), true);

        settings.movable = true;
        settings.totalSkipped = 0;
        settings.totalAccess = 0;
        settings.totalInactive = 4;
        settings.countsByProvider.put(settings.hostProviderId, 5L);
        settings.countsByProvider.put(settings.storageProviderId1, 5L);

        // Check counts after.
        verifyCommBoughtCounts(vm1Aws, settings);

        // Check storage commBought values.
        verifyStorageCommBought(vm1Aws, settings);
    }

    /**
     * Checks if storage commBought commodity settings are correct, whether volume id is set
     * correctly, and commBoughtGrouping is movable (should be).
     *
     * @param dtoBuilder DTO builder for entity being migrated.
     * @param settings Expected test settings.
     */
    private void verifyStorageCommBought(@Nonnull final TopologyEntityDTO.Builder dtoBuilder,
                                         @Nonnull final CommBoughtExpectedTestSettings settings) {
        // Verify storage commBoughtGrouping is correct, with volumeId still there.
        Optional<CommoditiesBoughtFromProvider> optGrouping = dtoBuilder
                .getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(commBoughtGrouping -> commBoughtGrouping.getProviderId()
                        == settings.storageProviderId1)
                .findFirst();

        assertTrue(optGrouping.isPresent());
        CommoditiesBoughtFromProvider storageCommBought = optGrouping.get();
        // Make sure commBoughtGrouping is now movable.
        assertTrue(storageCommBought.getMovable());
        assertEquals(settings.volumeId, storageCommBought.getVolumeId());
    }

    /**
     * Checks if commBought counts per provider are as expected, before and after test.
     *
     * @param dtoBuilder DTO builder for entity being migrated.
     * @param settings Expected test settings.
     */
    private void verifyCommBoughtCounts(@Nonnull final TopologyEntityDTO.Builder dtoBuilder,
                                        @Nonnull final CommBoughtExpectedTestSettings settings) {
        final AtomicInteger actualSkipCount = new AtomicInteger();
        final AtomicInteger actualAccessCount = new AtomicInteger();
        final AtomicInteger actualInactiveCount = new AtomicInteger();
        // Verify it has at least enough commBoughtGroupings that we are expecting.
        assertTrue(dtoBuilder.getCommoditiesBoughtFromProvidersCount()
                >= settings.countsByProvider.size());
        dtoBuilder.getCommoditiesBoughtFromProvidersList()
                .forEach(commBoughtGrouping -> {
                    if (!settings.countsByProvider.containsKey(commBoughtGrouping
                            .getProviderId())) {
                        // We are not checking this provider, don't care.
                        return;
                    }
                    long count = settings.countsByProvider.get(commBoughtGrouping.getProviderId());
                    assertEquals(count, Integer.valueOf(commBoughtGrouping
                            .getCommodityBoughtCount()).longValue());
                    // Check movable if it is not null (i.e should not be ignored).
                    if (settings.movable != null) {
                        assertEquals(settings.movable, commBoughtGrouping.getMovable());
                    }
                    commBoughtGrouping.getCommodityBoughtList()
                            .forEach(boughtDto -> {
                                if (COMMODITIES_TO_SKIP.contains(CommodityType.forNumber(boughtDto
                                        .getCommodityType().getType()))) {
                                    actualSkipCount.getAndIncrement();
                                } else if (boughtDto.getCommodityType().hasKey()) {
                                    actualAccessCount.getAndIncrement();
                                } else if (!boughtDto.getActive()) {
                                    actualInactiveCount.getAndIncrement();
                                }
                            });
                });
        assertEquals(settings.totalAccess, actualAccessCount.get());
        assertEquals(settings.totalSkipped, actualSkipCount.get());
        assertEquals(settings.totalInactive, actualInactiveCount.get());
    }

    /**
     * Check that the CloudMigrationSettingsPolicyEditor adds a group of migrating VMs
     * to certain discovered policies (and only those policies).
     */
    @Test
    public void testSettingsPolicyEditor() {
        TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getSourceEntities()).thenReturn(MIGRATING_VM_OIDS);

        Grouping existingCloudVMsGrouping = PolicyManager.generateStaticGroup(
            EXISTING_CLOUD_VM_OIDS,
            VIRTUAL_MACHINE_VALUE,
            "Existing Cloud VMs with an exclusion policy");

        ResolvedGroup existingCloudVMsResolvedGroup = new ResolvedGroup(
            existingCloudVMsGrouping,
            Collections.singletonMap(ApiEntityType.VIRTUAL_MACHINE, EXISTING_CLOUD_VM_OIDS));

        Map<Long, ResolvedGroup> groups = new HashMap<>();
        groups.put(existingCloudVMsGrouping.getId(), existingCloudVMsResolvedGroup);

        List<SettingPolicy> settingPolicies = new ArrayList<>();

        settingPolicies.add(
            createExclusionSettingsPolicy(IRRELEVANT_SETTING_POLICY,
                existingCloudVMsGrouping.getId()));

        settingPolicies.add(
            createExclusionSettingsPolicy("blahblah:Cloud Compute Tier AWS:standard:blahblah",
                existingCloudVMsGrouping.getId()));

        settingPolicies.add(
            createExclusionSettingsPolicy("blahblah:Cloud Compute Tier Azure:standard:blahblah",
                existingCloudVMsGrouping.getId()));

        CloudMigrationSettingsPolicyEditor editor = new CloudMigrationSettingsPolicyEditor(
            MIGRATING_VM_OIDS);

        List<SettingPolicy> updatedSettingPolicies = editor.applyEdits(settingPolicies, groups);
        assertEquals(settingPolicies.size(), updatedSettingPolicies.size());

        // Find groups newly added to the map
        List<Long> addedGroupOids = groups.keySet().stream()
            .filter(oid -> oid != existingCloudVMsGrouping.getId())
            .collect(Collectors.toList());

        // There should be one added group consisting of the migrating VMs.
        assertEquals(1, addedGroupOids.size());
        long addedGroupOid = addedGroupOids.get(0);

        ResolvedGroup addedGroup = groups.get(addedGroupOid);
        Set<Long> vmOids = addedGroup.getEntitiesOfType(ApiEntityType.VIRTUAL_MACHINE);
        assertEquals(MIGRATING_VM_OIDS, vmOids);

        // The group should now be included in the scope of the two policies that are
        // discovered exclusion polices for enforcing standard tiers, but not to the
        // "Irrelevant" settings policy.

        assertEquals(2, updatedSettingPolicies.stream()
            .filter(sp -> !IRRELEVANT_SETTING_POLICY.equals(sp.getInfo().getName()))
            .filter(sp -> sp.getInfo().getScope().getGroupsList().contains(addedGroupOid))
            .count());
    }

    private SettingPolicy createExclusionSettingsPolicy(@Nonnull final String name,
                                                        final Long scopeGroupOid) {
        Scope scope = Scope.newBuilder().addGroups(scopeGroupOid).build();

        SortedSetOfOidSettingValue oids = SortedSetOfOidSettingValue.newBuilder()
            .addAllOids(DISCOVERED_EXCLUDED_TIER_OIDS)
            .build();

        Setting setting = Setting.newBuilder()
            .setSettingSpecName(EntitySettingSpecs.ExcludedTemplates.getSettingName())
            .setSortedSetOfOidSettingValue(oids)
            .build();

        SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
            .setName(name)
            .setScope(scope)
            .addSettings(setting).build();

        return SettingPolicy.newBuilder().setId(100L)
            .setSettingPolicyType(Type.DISCOVERED)
            .setInfo(info)
            .build();
    }

    /**
     * Test that removeInactiveEntities correctly handles all entity states, returning
     * only entities whose state is POWERED_ON.
     */
    @Test
    public void testRemoveInactiveEntities() {
        final long activeVmOid = 1L;

        final TopologyGraphCreator<TopologyEntity.Builder, TopologyEntity> graphCreator =
            new TopologyGraphCreator<>(EntityState.values().length);

        // Create an active entity (POWERED_ON)
        graphCreator.addEntity(TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setEntityType(VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setOid(activeVmOid)));

        // Create entities in every other state
        for (EntityState state : EntityState.values()) {
            if (state != EntityState.POWERED_ON) {
                graphCreator.addEntity(TopologyEntity.newBuilder(
                    TopologyEntityDTO.newBuilder()
                        .setEntityType(VIRTUAL_MACHINE_VALUE)
                        .setEntityState(state)
                        .setOid(state.getNumber() + 1000L)));
            }
        }

        TopologyGraph<TopologyEntity> graph = graphCreator.build();
        Set<Long> entityOids = graph.entitiesOfType(VIRTUAL_MACHINE_VALUE)
            .map(TopologyEntity::getOid).collect(Collectors.toSet());

        assertEquals(EntityState.values().length, entityOids.size());

        final Set<Long> activeOids = cloudMigrationPlanHelper
            .removeInactiveEntities(graph, entityOids);

        // Only the active entity's oid should be returned

        assertTrue(activeOids.equals(Collections.singleton(activeVmOid)));
    }

    /**
     * Test removal of volume redundancy type.
     *
     * @throws Exception exception
     */
    @Test
    public void testRemoveRedundancyType() throws Exception {
        TopologyEntityDTO.Builder topologyEntityBuilder = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                                .setRedundancyType(RedundancyType.LRS).build()));

        CloudMigrationPlanHelper.removeRedundancyType(topologyEntityBuilder);
        assertTrue(topologyEntityBuilder.hasTypeSpecificInfo());
        assertTrue(topologyEntityBuilder.getTypeSpecificInfo().hasVirtualVolume());
        assertFalse(topologyEntityBuilder.getTypeSpecificInfo().getVirtualVolume().hasRedundancyType());
    }
}

