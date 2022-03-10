package com.vmturbo.topology.processor.topology;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.ConnectedEntityImpl;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.topology.CloudMigrationPlanHelper.CloudMigrationSettingsPolicyEditor;
import com.vmturbo.topology.processor.topology.CloudMigrationPlanHelper.CloudMigrationStageException;
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
    private TopologyEntityImpl vm1OnPrem;

    /**
     * On-prem source VM DTO data read from file.
     * This VM has 2 storages - volume1OnPrem and volume2OnPrem
     */
    private TopologyEntityImpl vm2OnPrem;

    /**
     * Azure source VM DTO data read from file.
     */
    private TopologyEntityImpl vm1Azure;

    /**
     * AWS source VM DTO data read from file.
     */
    private TopologyEntityImpl vm1Aws;

    /**
     * On-prem source Host PM DTO data read from file.
     */
    private TopologyEntityImpl host1OnPrem;

    /**
     * On-prem source Storage DTO data read from file.
     */
    private TopologyEntityImpl storage1OnPrem;

    /**
     * On-prem source Storage DTO data read from file. This storage has state equals UNKNOWN.
     */
    private TopologyEntityImpl storage2OnPrem;

    /**
     * On-prem volume 1, connected to storage 1.
     */
    private TopologyEntityImpl volume1OnPrem;

    /**
     * On-prem volume 2, connected to storage 2.
     */
    private TopologyEntityImpl volume2OnPrem;

    /**
     * Allocation plan topology info read from file.
     */
    private TopologyInfo allocationTopologyInfo;

    /**
     * Consumption plan topology info read from file.
     */
    private TopologyInfo consumptionTopologyInfo;

    private static final Set<Long> DISCOVERED_EXCLUDED_TIER_OIDS = Sets.newHashSet(101L, 102L, 103L);
    private static final Set<Long> EXISTING_CLOUD_VM_OIDS = Sets.newHashSet(1L, 2L, 3L);
    private static final Set<Long> MIGRATING_VM_OIDS = Sets.newHashSet(4L, 5L, 6L);

    /**
     * Creates new instance with dependencies.
     */
    public CloudMigrationPlanHelperTest() {
        groupServiceClient = mock(GroupConfig.class).groupServiceBlockingStub();
        statsHistoryServiceBlockingStub = mock(TopologyConfig.class).historyClient();
        cloudMigrationPlanHelper = new CloudMigrationPlanHelper(groupServiceClient, statsHistoryServiceBlockingStub);
    }

    /**
     * Sets up topology data after reading from data files.
     */
    @Before
    public void setup() {
        vm1OnPrem = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-vm-1-onprem.json"));
        vm2OnPrem = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-vm-2-onprem.json"));
        allocationTopologyInfo = loadTopologyInfo("cloud-migration-topo-info-allocation.json");
        consumptionTopologyInfo = loadTopologyInfo("cloud-migration-topo-info-consumption.json");
        vm1Azure = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-vm-1-azure.json"));
        vm1Aws = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-vm-1-aws.json"));
        host1OnPrem = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-pm-1-onprem.json"));
        storage1OnPrem = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-storage-1-onprem.json"));
        storage2OnPrem = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-storage-2-onprem.json"));
        volume1OnPrem = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-volume-1-onprem.json"));
        volume2OnPrem = TopologyEntityImpl.fromProto(loadTopologyBuilderDTO("cloud-migration-volume-2-onprem.json"));
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
     * Test that cloud entities with AHUB pricing are reset to normal LICENSE_INCLUDED
     * pricing before migration, to ensure correct OS cost calculation at the destination.
     *
     * @throws PipelineStageException should not happen in this test and indicates failure
     */
    @Test
    public void prepareEntitiesAHUB() throws PipelineStageException {
        assertNotNull(vm1Azure);
        assertNotNull(allocationTopologyInfo);

        TopologyEntity.Builder azureVm = TopologyEntity.newBuilder(vm1Azure);

        // Verify that the VM starts with AHUB licensing

        assertEquals(LicenseModel.AHUB,
            vm1Azure.getTypeSpecificInfo().getVirtualMachine().getLicenseModel());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(
            azureVm);

        TopologyMigration migration = TopologyMigration.getDefaultInstance();

        TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(consumptionTopologyInfo);

        cloudMigrationPlanHelper.prepareEntities(context, graph, migration, Collections.EMPTY_MAP,
            Collections.singleton(azureVm.getOid()), true);

        TopologyEntity resultVm = graph.getEntity(azureVm.getOid()).orElse(null);
        assertNotNull(resultVm);

        // VM should have been updated to LICENSE_INCLUDED to ensure correct pricing
        // calculation on the destination
        assertEquals(LicenseModel.LICENSE_INCLUDED,
            resultVm.getTypeSpecificInfo().getVirtualMachine().getLicenseModel());
    }

    /**
     * Given an on prem vm that consumes on a volume, and the vm is not movable and not scalable.
     * After prepareEntities(), it would be set to movable and scalable. The environment type would
     * become CLOUD.
     *
     * @throws PipelineStageException should not happen in this test
     */
    @Test
    public void testPrepareEntitiesVMConsumesFromVolume()  throws PipelineStageException {
        long volumeId = 2222L;
        long vmId = 1111L;
        // Create a vm that is not movable and not scalable.
        TopologyEntityImpl vm = new TopologyEntityImpl().addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl().setProviderId(volumeId).setMovable(false)
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setScalable(false)
                        .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                        new CommodityTypeImpl().setType(CommodityType.STORAGE_AMOUNT_VALUE))))
                .setOid(vmId).setEntityType(VIRTUAL_MACHINE_VALUE).setEnvironmentType(EnvironmentType.ON_PREM)
                .setAnalysisSettings(new AnalysisSettingsImpl().setControllable(false));
        TopologyEntityImpl volume = new TopologyEntityImpl().addCommoditySoldList(
                new CommoditySoldImpl().setCommodityType(new CommodityTypeImpl()
                        .setType(CommodityType.STORAGE_AMOUNT_VALUE)))
                .setEnvironmentType(EnvironmentType.ON_PREM).setOid(volumeId)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(
                TopologyEntity.newBuilder(vm), TopologyEntity.newBuilder(volume));
        TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(allocationTopologyInfo);
        cloudMigrationPlanHelper.prepareEntities(context, graph, TopologyMigration.getDefaultInstance(),
                Collections.EMPTY_MAP, Collections.singleton(vm.getOid()), true);

        TopologyEntity resultVolume = graph.getEntity(volumeId).orElse(null);
        assertNotNull(resultVolume);
        assertEquals(EnvironmentType.CLOUD, resultVolume.getEnvironmentType());
        TopologyEntity resultVm = graph.getEntity(vmId).orElse(null);
        assertNotNull(resultVm);
        assertEquals(EnvironmentType.CLOUD, resultVm.getEnvironmentType());
        assertTrue(resultVm.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList()
                .stream().allMatch(sl -> sl.getMovable() == true && sl.getScalable() == true));

    }

    /**
     * If an on-prem storage has state UNKNOWN, it will changed to POWERED_ON by prepareEntities.
     *
     * @throws PipelineStageException should not happen in this test
     */
    @Test
    public void prepareEntitiesStorageUnknownState() throws PipelineStageException {
        assertNotNull(storage2OnPrem);
        assertNotNull(vm1OnPrem);
        assertNotNull(allocationTopologyInfo);

        TopologyEntity.Builder onpremVm = TopologyEntity.newBuilder(vm1OnPrem);
        TopologyEntity.Builder storageOnPrem = TopologyEntity.newBuilder(storage2OnPrem);

        assertEquals(EntityState.UNKNOWN_VALUE,
                storage2OnPrem.getEntityState().getNumber());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(
                storageOnPrem, onpremVm);

        TopologyMigration migration = TopologyMigration.getDefaultInstance();

        TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(allocationTopologyInfo);

        cloudMigrationPlanHelper.prepareEntities(context, graph, migration, Collections.EMPTY_MAP,
            Collections.singleton(vm1OnPrem.getOid()), true);

        TopologyEntity resultStorage = graph.getEntity(storage2OnPrem.getOid()).orElse(null);
        assertNotNull(resultStorage);

        assertEquals(EntityState.POWERED_ON_VALUE, resultStorage.getEntityState().getNumber());
    }

    /**
     * Confirms that intra-cloud migrations are prevented, and that legal cloud-to-cloud migrations
     * are supported.
     *
     * @throws CloudMigrationStageException Thrown if a cloud migration plan the destination does
     * not have a valid cloudType, or when an intra-plan migration in attempted in such plans
     */
    @Test
    public void testPreventionOfIntraCloudMigrations() throws CloudMigrationStageException {
        final long regionId = 1L;
        final long azureServiceProviderId = 2L;
        final long awsServiceProviderId = 3L;
        final long awsVmId = 4L;
        final long awsBusinessAccountId = 5L;
        final long azureBusinessAccountId = 6L;
        final long vmId = 7L;

        final TopologyEntity.Builder azureBusinessAccount = TopologyEntityUtils.topologyEntity(azureBusinessAccountId, 0, 0, "BUSINESS_ACCOUNT", EntityType.BUSINESS_ACCOUNT);
        azureBusinessAccount.getTopologyEntityImpl().addAllConnectedEntityList(Lists.newArrayList(
                new ConnectedEntityImpl()
                        .setConnectedEntityId(vmId)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION),
                new ConnectedEntityImpl()
                        .setConnectedEntityId(azureServiceProviderId)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)));

        final TopologyEntity.Builder azureServiceProviderEntity =  TopologyEntityUtils.topologyEntity(azureServiceProviderId, 0, 0, "Azure", EntityType.SERVICE_PROVIDER);
        azureServiceProviderEntity.getTopologyEntityImpl()
                .addConnectedEntityList(
                        new ConnectedEntityImpl()
                                .setConnectedEntityId(regionId)
                                .setConnectionType(ConnectionType.OWNS_CONNECTION));

        final TopologyEntity.Builder azureVmEntity = TopologyEntityUtils
                .topologyEntity(vmId, 0, 0, "VM", EntityType.VIRTUAL_MACHINE);
        final TopologyEntity.Builder regionEntity = TopologyEntityUtils
                .topologyEntity(regionId, 0, 0, "REGION", EntityType.REGION);

        regionEntity.getTopologyEntityImpl().addConnectedEntityList(
                new ConnectedEntityImpl()
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION));

        final TopologyEntity.Builder awsVmEntity = TopologyEntityUtils
                .topologyEntity(awsVmId, 0, 0, "AWS_VM", EntityType.VIRTUAL_MACHINE);
        final TopologyEntity.Builder awsBusinessAccount = TopologyEntityUtils.topologyEntity(
                awsBusinessAccountId, 0, 0, "BUSINESS_ACCOUNT", EntityType.BUSINESS_ACCOUNT);
        final TopologyEntity.Builder awsServiceProvider = TopologyEntityUtils.topologyEntity(
                awsServiceProviderId, 0, 0, "Aws", EntityType.SERVICE_PROVIDER);

        awsServiceProvider.getTopologyEntityImpl().addConnectedEntityList(
                new ConnectedEntityImpl()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityId(awsBusinessAccountId));

        awsBusinessAccount.getTopologyEntityImpl().addAllConnectedEntityList(Lists.newArrayList(
                new ConnectedEntityImpl()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityId(awsVmId),
                new ConnectedEntityImpl()
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .setConnectedEntityId(awsServiceProviderId)));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(
                azureVmEntity,
                awsVmEntity,
                regionEntity,
                azureBusinessAccount,
                azureServiceProviderEntity,
                awsBusinessAccount,
                awsServiceProvider);

        final boolean isAzureToAzureIntraCloudMigration = cloudMigrationPlanHelper.isIntraCloudMigration(
                graph,
                Sets.newHashSet(vmId),
                Sets.newHashSet(regionId));

        assertTrue(isAzureToAzureIntraCloudMigration);

        final boolean isAwsToAzureIntraCloudMigration = cloudMigrationPlanHelper.isIntraCloudMigration(
                graph,
                Sets.newHashSet(awsVmId),
                Sets.newHashSet(regionId));

        assertFalse(isAwsToAzureIntraCloudMigration);
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

        settings.movable = false;
        settings.totalSkipped = 5;
        settings.totalAccess = 6;
        settings.totalInactive = 0;
        settings.countsByProvider.put(settings.hostProviderId, 13L);
        settings.countsByProvider.put(settings.storageProviderId1, 6L);

        // Check counts before.
        verifyCommBoughtCounts(vm1OnPrem, settings);

        // vm1OnPrem has 1 storage, hence 1 comm bought list that has a storage provider.
        verifyNumOfStorageCommBoughtLists(1, vm1OnPrem);

        // Inactive commodities are only being done for allocation plan, rest of the behavior
        // of this method is the same across plans, so pass in allocation topology info.
        TopologyGraph graph = TopologyEntityTopologyGraphCreator.newGraph(new HashMap());
        cloudMigrationPlanHelper.prepareBoughtCommodities(createVmEntity(vm1OnPrem),
                allocationTopologyInfo,
                Collections.emptyMap(), true, true, graph);

        settings.movable = true;
        settings.totalSkipped = 0;
        settings.totalAccess = 0;
        settings.totalInactive = 2;
        settings.countsByProvider.put(settings.hostProviderId, 4L);
        settings.countsByProvider.put(settings.storageProviderId1, 4L);

        // Check counts after.
        verifyCommBoughtCounts(vm1OnPrem, settings);

        // Check storage commBought values.
        verifyStorageCommBought(vm1OnPrem, settings);

        // vm1OnPrem has 1 storage, hence 1 comm bought list that has a storage provider.
        verifyNumOfStorageCommBoughtLists(1, vm1OnPrem);
    }

    private TopologyEntity createVmEntity(TopologyEntityImpl vmEntityDto) {
        TopologyEntity.Builder vmEntity = TopologyEntity.newBuilder(vmEntityDto);
        TopologyEntity.Builder volumeEntity = TopologyEntity.newBuilder(volume1OnPrem);
        vmEntity.addOutboundAssociation(volumeEntity);
        TopologyEntity.Builder storageEntity = TopologyEntity.newBuilder(storage1OnPrem);
        storageEntity.addInboundAssociation(volumeEntity);
        vmEntity.addProvider(storageEntity);
        return vmEntity.build();
    }

    private void verifyNumOfStorageCommBoughtLists(int expectedNumber, TopologyEntityImpl vmEntityDto) {
        long numOfStorageCommBoughtLists = vmEntityDto.getCommoditiesBoughtFromProvidersImplList().stream()
                .filter(list -> list.getProviderEntityType() == STORAGE_VALUE)
                .count();
        assertEquals(expectedNumber, numOfStorageCommBoughtLists);
    }

    /**
     * Test the logic that excludes commodity bought list that buys from a storage that does not
     * have a volume. It happens when the storage is modelling an ISO NFS share.
     */
    @Test
    public void excludeStorageWithNoVolumes() {
        // vm2OnPrem is connected to 2 storages, hence 2 comm bought list that has a storage provider.
        verifyNumOfStorageCommBoughtLists(2, vm2OnPrem);

        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(new HashMap<>());
        cloudMigrationPlanHelper.prepareBoughtCommodities(createVmEntity2Storages1Volume(false),
                allocationTopologyInfo, Collections.emptyMap(), true, true, graph);

        // 1 of the storages does not have a volume. (e.g. ISO NFSShare) This storage comm bought
        // list should be excluded from migration.
        verifyNumOfStorageCommBoughtLists(1, vm1OnPrem);
    }

    /**
     * Test the logic that excludes commodity bought list that buys from a storage that does not
     * have a volume that connects to the VM. This situation happens when migrating a cluster or
     * a group of VMs to the cloud. The storage can be used by volumes of any VMs in scope.
     */
    @Test
    public void excludeStorageWithUnrelatedVolumes() {
        // vm2OnPrem is connected to 2 storages, hence 2 comm bought list that has a storage provider.
        verifyNumOfStorageCommBoughtLists(2, vm2OnPrem);

        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(new HashMap<>());
        cloudMigrationPlanHelper.prepareBoughtCommodities(createVmEntity2Storages1Volume(true),
                allocationTopologyInfo, Collections.emptyMap(), true, true, graph);

        // One of the storages does not have a volume that is connected to the VM.
        // This storage comm bought list should be excluded from migration.
        verifyNumOfStorageCommBoughtLists(1, vm1OnPrem);
    }

    /**
     * Creates the TopologyEntity of a VM that has 2 storages.
     * If the parameter pass in a flag that is true, the second storage is connected to a volume
     * that is not connected to the VM.
     *
     * @param storageHasUnrelatedVolume a flag that prompts the connection of an unrelated volume to
     *                                  a storage.
     * @return Topology entity of the VM
     */
    private TopologyEntity createVmEntity2Storages1Volume(boolean storageHasUnrelatedVolume) {
        TopologyEntity.Builder vmEntity = TopologyEntity.newBuilder(vm2OnPrem);
        TopologyEntity.Builder volume1Entity = TopologyEntity.newBuilder(volume1OnPrem);
        vmEntity.addOutboundAssociation(volume1Entity);
        TopologyEntity.Builder storage1Entity = TopologyEntity.newBuilder(storage1OnPrem);
        storage1Entity.addInboundAssociation(volume1Entity);
        vmEntity.addProvider(storage1Entity);
        TopologyEntity.Builder storage2Entity = TopologyEntity.newBuilder(storage2OnPrem);
        vmEntity.addProvider(storage2Entity);

        if (storageHasUnrelatedVolume) {
            TopologyEntity.Builder volume2Entity = TopologyEntity.newBuilder(volume2OnPrem);
            storage2Entity.addInboundAssociation(volume2Entity);
        }
        return vmEntity.build();
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
        TopologyGraph graph = TopologyEntityTopologyGraphCreator.newGraph(new HashMap());
        TopologyEntity vm1AzureEntity = TopologyEntity.newBuilder(vm1Azure).build();
        cloudMigrationPlanHelper.prepareBoughtCommodities(vm1AzureEntity, allocationTopologyInfo,
                Collections.emptyMap(), true, true, graph);

        settings.movable = true;
        settings.totalSkipped = 0;
        settings.totalAccess = 0;
        settings.totalInactive = 4;
        settings.countsByProvider.put(settings.hostProviderId, 3L);
        settings.countsByProvider.put(settings.storageProviderId1, 5L);
        settings.countsByProvider.put(settings.storageProviderId2, 5L);

        // Check counts after.
        verifyCommBoughtCounts(vm1Azure, settings);

        // Check storage commBought values.
        verifyStorageCommBought(vm1Azure, settings);
    }

    /**
     * Make sure IOPS commodity value is not 0.
     */
    @Test
    public void testPrepareSoldCommodities() {
        Map<Long, Double> providerToMaxStorageAccessMap = new HashMap<>();
        providerToMaxStorageAccessMap.put(100L, 0d);

        TopologyEntityImpl volumeDtoBuild = new TopologyEntityImpl();
        volumeDtoBuild
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setOid(100L)
                .addCommoditySoldList(new CommoditySoldImpl()
                        .setCommodityType(new CommodityTypeImpl()
                                .setType(CommodityType.STORAGE_ACCESS_VALUE)));

        cloudMigrationPlanHelper.prepareSoldCommodities(volumeDtoBuild, providerToMaxStorageAccessMap);
        double iopsValue = volumeDtoBuild.getCommoditySoldListImplList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                .findFirst().map(c -> c.getUsed()).orElse(-1d);
        assertTrue(iopsValue > 0);
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
        TopologyGraph graph = TopologyEntityTopologyGraphCreator.newGraph(new HashMap());
        TopologyEntity vm1AwsEntity = TopologyEntity.newBuilder(vm1Aws).build();
        cloudMigrationPlanHelper.prepareBoughtCommodities(vm1AwsEntity, allocationTopologyInfo,
                Collections.emptyMap(), true, true, graph);

        settings.movable = true;
        settings.totalSkipped = 0;
        settings.totalAccess = 0;
        settings.totalInactive = 3;
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
     * @param entityImpl Entity pojo being migrated.
     * @param settings Expected test settings.
     */
    private void verifyStorageCommBought(@Nonnull final TopologyEntityImpl entityImpl,
                                         @Nonnull final CommBoughtExpectedTestSettings settings) {
        // Verify storage commBoughtGrouping is correct.
        Optional<CommoditiesBoughtFromProviderView> optGrouping = entityImpl
                .getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(commBoughtGrouping -> commBoughtGrouping.getProviderId()
                        == settings.storageProviderId1)
                .findFirst();

        assertTrue(optGrouping.isPresent());
        CommoditiesBoughtFromProviderView storageCommBought = optGrouping.get();
        // Make sure commBoughtGrouping is now movable.
        assertTrue(storageCommBought.getMovable());
    }

    /**
     * Checks if commBought counts per provider are as expected, before and after test.
     *
     * @param entityImpl Entity pojo being migrated.
     * @param settings Expected test settings.
     */
    private void verifyCommBoughtCounts(@Nonnull final TopologyEntityImpl entityImpl,
                                        @Nonnull final CommBoughtExpectedTestSettings settings) {
        final AtomicInteger actualSkipCount = new AtomicInteger();
        final AtomicInteger actualAccessCount = new AtomicInteger();
        final AtomicInteger actualInactiveCount = new AtomicInteger();
        // Verify it has at least enough commBoughtGroupings that we are expecting.
        assertTrue(entityImpl.getCommoditiesBoughtFromProvidersCount()
                >= settings.countsByProvider.size());
        entityImpl.getCommoditiesBoughtFromProvidersList()
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
     * to certain discovered policies (and only those policies). Also verify that exclusion
     * policy for Windows SQL server VMs being migrated, is correctly updated.
     */
    @Test
    public void testSettingsPolicyEditor() {
        Grouping existingCloudVMsGrouping = PolicyManager.generateStaticGroup(
            EXISTING_CLOUD_VM_OIDS,
            VIRTUAL_MACHINE_VALUE,
            "Existing Cloud VMs with an exclusion policy");

        ResolvedGroup existingCloudVMsResolvedGroup = new ResolvedGroup(
            existingCloudVMsGrouping,
            Collections.singletonMap(ApiEntityType.VIRTUAL_MACHINE, EXISTING_CLOUD_VM_OIDS));

        Map<Long, ResolvedGroup> groups = new HashMap<>();
        groups.put(existingCloudVMsGrouping.getId(), existingCloudVMsResolvedGroup);

        final Grouping windowsVmGrouping = PolicyManager.generateStaticGroup(
                EXISTING_CLOUD_VM_OIDS,
                VIRTUAL_MACHINE_VALUE,
                "Existing Cloud VMs with Windows SQL exclusion policy");
        final ResolvedGroup windowsVmResolvedGroup = new ResolvedGroup(
                windowsVmGrouping,
                Collections.singletonMap(ApiEntityType.VIRTUAL_MACHINE, EXISTING_CLOUD_VM_OIDS));
        groups.put(windowsVmGrouping.getId(), windowsVmResolvedGroup);

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

        settingPolicies.add(
                createExclusionSettingsPolicy(String.format("vmturbodev %s policy",
                        CloudMigrationSettingsPolicyEditor.WINDOWS_SQL_SERVER_POLICY),
                        windowsVmGrouping.getId()));

        final long windowsSqlServerVmId = 4L;
        final String windowsSqlServerVmName = "SQLServerTestVM";
        final TopologyEntity.Builder windowsSqlServerVm = TopologyEntityUtils
                .topologyEntity(windowsSqlServerVmId, 0, 0,
                        windowsSqlServerVmName, EntityType.VIRTUAL_MACHINE);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.pojoGraphOf(
                windowsSqlServerVm);

        CloudMigrationSettingsPolicyEditor editor = new CloudMigrationSettingsPolicyEditor(
            MIGRATING_VM_OIDS, graph);

        List<SettingPolicy> updatedSettingPolicies = editor.applyEdits(settingPolicies, groups);
        assertEquals(settingPolicies.size(), updatedSettingPolicies.size());

        // Find groups newly added to the map
        List<Long> addedGroupOids = groups.keySet().stream()
            .filter(oid -> oid != existingCloudVMsGrouping.getId()
                    && oid != windowsVmGrouping.getId())
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

        // Verify that the Windows SQL server policy is correctly updated with the right VM group id.
        assertEquals(1, updatedSettingPolicies.stream()
                .filter(sp -> sp.getInfo().getName().contains(CloudMigrationSettingsPolicyEditor
                        .WINDOWS_SQL_SERVER_POLICY))
                .filter(sp -> sp.getInfo().getScope().getGroupsList().contains(windowsVmGrouping
                        .getId()))
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
            new TopologyEntityImpl()
                .setEntityType(VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setOid(activeVmOid)));

        // Create entities in every other state
        for (EntityState state : EntityState.values()) {
            if (state != EntityState.POWERED_ON) {
                graphCreator.addEntity(TopologyEntity.newBuilder(
                    new TopologyEntityImpl()
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
     * Verifies that movable and scalable flags for onPrem hosts and storage providers are set
     * to false, so that we don't see actions for those in MCP plan output.
     */
    @Test
    public void onPremHostAndStorageNonMovable() {
        assertNotNull(host1OnPrem);
        assertFalse(host1OnPrem.getCommoditiesBoughtFromProvidersList().isEmpty());
        assertNotNull(storage1OnPrem);
        assertFalse(storage1OnPrem.getCommoditiesBoughtFromProvidersList().isEmpty());
        assertNotNull(allocationTopologyInfo);

        long storageProviderOfHost = 73490062265697L;
        verifyMovableScalable(host1OnPrem, storageProviderOfHost, true);
        long diskArrayProviderOfStorage = 73433887031971L;
        verifyMovableScalable(storage1OnPrem, diskArrayProviderOfStorage, true);

        TopologyEntity host1OnPremEntity = TopologyEntity.newBuilder(host1OnPrem).build();
        cloudMigrationPlanHelper.prepareBoughtCommodities(host1OnPremEntity, allocationTopologyInfo,
                Collections.emptyMap(), true, false, mock(TopologyGraph.class));
        TopologyEntity storage1OnPremEntity = TopologyEntity.newBuilder(storage1OnPrem).build();
        cloudMigrationPlanHelper.prepareBoughtCommodities(storage1OnPremEntity, allocationTopologyInfo,
                Collections.emptyMap(), true, false, mock(TopologyGraph.class));

        verifyMovableScalable(host1OnPrem, storageProviderOfHost, false);
        verifyMovableScalable(storage1OnPrem, diskArrayProviderOfStorage, false);
    }

    /**
     * Convenience method to check status of commBoughtGrouping movable and scalable flags.
     *
     * @param entity Entity (host/storage) for which setting needs to be checked.
     * @param providerId Provider that host/storage is buying from.
     * @param expectedValue Value to check.
     */
    private void verifyMovableScalable(@Nonnull final TopologyEntityImpl entity,
            long providerId, boolean expectedValue) {
        Optional<CommoditiesBoughtFromProviderView> commBoughtGrouping =
                entity.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(commBought -> commBought.getProviderId() == providerId)
                        .findAny();
        assertTrue(commBoughtGrouping.isPresent());
        assertEquals(commBoughtGrouping.get().getMovable(), expectedValue);
        assertEquals(commBoughtGrouping.get().getScalable(), expectedValue);
    }

    /**
     * Test that we create a numDisk commodity for On Prem or AWS VMs that migrate to Azure.
     *
     * @throws PipelineStageException should not happen in this test and indicates failure
     */
    @Test
    public void testAddNumVMsCommodity() throws PipelineStageException {
        assertNotNull(vm1Aws);
        assertNotNull(vm1OnPrem);
        assertNotNull(allocationTopologyInfo);

        TopologyEntity.Builder awsVm = TopologyEntity.newBuilder(vm1Aws);
        TopologyEntity.Builder onPremVm = TopologyEntity.newBuilder(vm1OnPrem);

        final TopologyGraph<TopologyEntity> awsGraph = TopologyEntityUtils.pojoGraphOf(awsVm);
        final TopologyGraph<TopologyEntity> onPremGraph = TopologyEntityUtils.pojoGraphOf(onPremVm);

        TopologyMigration migration = TopologyMigration.getDefaultInstance();

        TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(consumptionTopologyInfo);

        // AWS to Azure
        cloudMigrationPlanHelper.prepareEntities(context, awsGraph, migration, Collections.EMPTY_MAP,
            Collections.singleton(awsVm.getOid()), false);
        assertNumDiskCommodity(awsGraph, awsVm.getOid(), EntityType.COMPUTE_TIER_VALUE, 1);

        // OnPrem to Azure
        cloudMigrationPlanHelper.prepareEntities(context, onPremGraph, migration, Collections.EMPTY_MAP,
            Collections.singleton(onPremVm.getOid()), false);
        assertNumDiskCommodity(onPremGraph, onPremVm.getOid(), EntityType.PHYSICAL_MACHINE_VALUE, 4);
    }

    private void assertNumDiskCommodity(@Nonnull final TopologyGraph<TopologyEntity> graph, long oid,
                                        int providerType, double expectedNumDiskUsed) {
        TopologyEntity resultVm = graph.getEntity(oid).orElse(null);
        assertNotNull(resultVm);
        CommoditiesBoughtFromProviderView commBoughtFromPr = resultVm.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList().stream()
                .filter(pr -> providerType == pr.getProviderEntityType())
                .findFirst().orElse(null);
        assertNotNull(commBoughtFromPr);
        CommodityBoughtView numDiskComm = commBoughtFromPr.getCommodityBoughtList().stream()
                .filter(comm -> comm.getCommodityType().getType() == CommodityType.NUM_DISK_VALUE)
                .findFirst().orElse(null);
        assertNotNull(numDiskComm);
        assertEquals(expectedNumDiskUsed, numDiskComm.getUsed(), 0);
    }

    /**
     * Check a coupon commodity IS ADDED to PM provider when the VM has such provider but no
     * existing COUPON commodity.  The coupon commodity should only added to PM provider of a VM.
     */
    @Test
    public void addCouponCommodityWhenVmIsProvidedByPMAndNoCoupon() {
        TopologyEntityImpl vm = new TopologyEntityImpl();
        final long pmProviderId = 101L;
        final long diskProviderId = 301L;
        final long vmId = 201L;
        vm.setOid(vmId);
        vm.setEntityType(VIRTUAL_MACHINE_VALUE);
        vm.addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setProviderId(pmProviderId)
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl().setType(CommodityType.CPU_VALUE)))
                .addCommodityBought(new CommodityBoughtImpl()
                   .setCommodityType(new CommodityTypeImpl().setType(CommodityType.MEM_VALUE))));
        vm.addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
            .setProviderEntityType(EntityType.DISK_ARRAY_VALUE)
            .setProviderId(diskProviderId)
            .addCommodityBought(new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl().setType(CommodityType.STORAGE_AMOUNT_VALUE))));
        cloudMigrationPlanHelper.addCouponCommodity(TopologyEntity.newBuilder(vm).build());

        assertEquals(2, vm.getCommoditiesBoughtFromProvidersImplList().size());

        // Verify PM Provider
        List<CommoditiesBoughtFromProviderImpl> pmProviders = vm.getCommoditiesBoughtFromProvidersImplList().stream().filter(provider ->
            EntityType.PHYSICAL_MACHINE_VALUE == provider.getProviderEntityType()).collect(Collectors.toList());
        assertEquals(1, pmProviders.size());
        final CommoditiesBoughtFromProviderImpl pmProvider = pmProviders.get(0);
        assertEquals(pmProviderId, pmProvider.getProviderId());
        assertEquals(3, pmProvider.getCommodityBoughtCount());
        assertTrue(pmProvider.getCommodityBoughtImplList().stream().anyMatch(commodityBoughtBuilder -> CommodityType.COUPON_VALUE == commodityBoughtBuilder.getCommodityType().getType()));


        // Verify non PM Provider, no coupon should be added
        List<CommoditiesBoughtFromProviderImpl> nonPmProviders = vm.getCommoditiesBoughtFromProvidersImplList().stream().filter(provider ->
            EntityType.PHYSICAL_MACHINE_VALUE != provider.getProviderEntityType()).collect(Collectors.toList());
        assertEquals(1, nonPmProviders.size());
        final CommoditiesBoughtFromProviderImpl nonPmProvider = nonPmProviders.get(0);
        assertEquals(diskProviderId, nonPmProvider.getProviderId());
        assertEquals(1, nonPmProvider.getCommodityBoughtCount());
        assertFalse(nonPmProvider.getCommodityBoughtImplList().stream().anyMatch(commodityBoughtBuilder -> CommodityType.COUPON_VALUE == commodityBoughtBuilder.getCommodityType().getType()));
    }

    /**
     * Check coupon commodity IS NOT CHANGED to PM provider when the VM has such provider has
     * existing COUPON commodity.
     */
    @Test
    public void addCouponCommodityWhenVmIsProvidedByPMAndHasCoupon() {
        TopologyEntityImpl vm = new TopologyEntityImpl();
        final long pmProviderId = 101L;
        final long vmId = 201L;
        final double couponUsed = 3.0d;
        final double couponPeak = 5.0d;
        final double delta = 0.000001d;
        vm.setOid(vmId);
        vm.setEntityType(VIRTUAL_MACHINE_VALUE);
        vm.addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setProviderId(pmProviderId)
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl().setType(CommodityType.CPU_VALUE)))
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl().setType(CommodityType.MEM_VALUE)))
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl().setType(CommodityType.COUPON_VALUE))
                    .setUsed(couponUsed)
                    .setPeak(couponPeak)));
        vm.addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
            .setProviderEntityType(EntityType.DISK_ARRAY_VALUE)
            .setProviderId(301L)
            .addCommodityBought(new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl().setType(CommodityType.STORAGE_AMOUNT_VALUE))));
        cloudMigrationPlanHelper.addCouponCommodity(TopologyEntity.newBuilder(vm).build());

        // Verify
        assertEquals(2, vm.getCommoditiesBoughtFromProvidersImplList().size());
        List<CommoditiesBoughtFromProviderImpl> pmProviders = vm.getCommoditiesBoughtFromProvidersImplList().stream()
            .filter(provider -> EntityType.PHYSICAL_MACHINE_VALUE == provider.getProviderEntityType())
            .collect(Collectors.toList());
        assertEquals(1, pmProviders.size());
        final CommoditiesBoughtFromProviderImpl pmProvider = pmProviders.get(0);
        assertEquals(pmProviderId, pmProvider.getProviderId());
        assertEquals(3, pmProvider.getCommodityBoughtCount());

        // Check the coupon values are not changed by the method
        List<CommodityBoughtImpl> couponCommodities = pmProvider.getCommodityBoughtImplList().stream()
            .filter(commodity -> CommodityType.COUPON_VALUE == commodity.getCommodityType().getType())
            .collect(Collectors.toList());
        assertEquals(1, couponCommodities.size());
        final CommodityBoughtImpl couponCommodity = couponCommodities.get(0);
        assertEquals(couponPeak, couponCommodity.getPeak(), delta);
        assertEquals(couponUsed, couponCommodity.getUsed(), delta);
    }

    /**
     * When the entity is not a VM, the commodity list should not be touched
     * by the addCouponCommodity() method, regardless of the provider type.
     */
    @Test
    public void addCouponCommodityWhenEntityIsNonVm() {
        TopologyEntityImpl vm = new TopologyEntityImpl();
        final long pmProviderId = 101L;
        final long diskProviderId = 301L;
        final long vmId = 201L;
        vm.setOid(vmId);
        vm.setEntityType(STORAGE_VALUE);
        vm.addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setProviderId(pmProviderId)
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl().setType(CommodityType.CPU_VALUE)))
                .addCommodityBought(new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl().setType(CommodityType.MEM_VALUE))));
        vm.addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
            .setProviderEntityType(EntityType.DISK_ARRAY_VALUE)
            .setProviderId(diskProviderId)
            .addCommodityBought(new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl().setType(CommodityType.STORAGE_AMOUNT_VALUE))));
        cloudMigrationPlanHelper.addCouponCommodity(TopologyEntity.newBuilder(vm).build());

        assertEquals(2, vm.getCommoditiesBoughtFromProvidersImplList().size());

        // Verify PM Provider
        List<CommoditiesBoughtFromProviderImpl> pmProviders = vm.getCommoditiesBoughtFromProvidersImplList().stream().filter(provider ->
            EntityType.PHYSICAL_MACHINE_VALUE == provider.getProviderEntityType()).collect(Collectors.toList());
        assertEquals(1, pmProviders.size());
        final CommoditiesBoughtFromProviderImpl pmProvider = pmProviders.get(0);
        assertEquals(pmProviderId, pmProvider.getProviderId());
        assertEquals(2, pmProvider.getCommodityBoughtCount());
        assertFalse(pmProvider.getCommodityBoughtImplList().stream().anyMatch(commodityBoughtBuilder -> CommodityType.COUPON_VALUE == commodityBoughtBuilder.getCommodityType().getType()));


        // Verify non PM Provider, no coupon should be added
        List<CommoditiesBoughtFromProviderImpl> nonPmProviders = vm.getCommoditiesBoughtFromProvidersImplList().stream().filter(provider ->
            EntityType.PHYSICAL_MACHINE_VALUE != provider.getProviderEntityType()).collect(Collectors.toList());
        assertEquals(1, nonPmProviders.size());
        final CommoditiesBoughtFromProviderImpl nonPmProvider = nonPmProviders.get(0);
        assertEquals(diskProviderId, nonPmProvider.getProviderId());
        assertEquals(1, nonPmProvider.getCommodityBoughtCount());
        assertFalse(nonPmProvider.getCommodityBoughtImplList().stream().anyMatch(commodityBoughtBuilder -> CommodityType.COUPON_VALUE == commodityBoughtBuilder.getCommodityType().getType()));
    }

    /**
     * Checks if commodities are getting skipped correctly when doing on-prem to cloud migration.
     * Certain on-prem commodities don't apply to cloud and are thus not sold by cloud tiers,
     * so those are being removed from TopologyEntity, before they can be migrated to cloud.
     * Note: This test was moved here from CommoditiesEditorTest as
     */
    @Test
    public void skipCommoditiesOnMigration() {
        TopologyEntityImpl entity = new TopologyEntityImpl();
        long pmProviderId = 101L;
        long storageVolumeProviderId = 102L;
        List<CommoditiesBoughtFromProviderView> commoditiesByProvider =
                getCommoditiesBoughtFromProviders(pmProviderId, storageVolumeProviderId);

        // First check whether skip works for PM provider commodities.
        CommoditiesBoughtFromProviderView checkPmProvider = commoditiesByProvider.get(0);
        assertEquals(pmProviderId, checkPmProvider.getProviderId());
        assertEquals(3, checkPmProvider.getCommodityBoughtCount());
        final List<CommodityBoughtView> remainingBoughtPm = cloudMigrationPlanHelper
                .getUpdatedCommBought(checkPmProvider, consumptionTopologyInfo,
                        entity, Collections.emptyMap(), true, true);
        assertEquals(2, remainingBoughtPm.size());

        // These 2 commodities (cpu & mem) should not be getting filtered out, only Q2_VCPU should.
        boolean hasCpu = false;
        boolean hasMem = false;
        for (CommodityBoughtView boughtDTO : remainingBoughtPm) {
            if (boughtDTO.getCommodityType().getType() == CommodityDTO.CommodityType.CPU_VALUE) {
                hasCpu = true;
            }
            if (boughtDTO.getCommodityType().getType() == CommodityDTO.CommodityType.MEM_VALUE) {
                hasMem = true;
            }
        }
        assertTrue(hasCpu);
        assertTrue(hasMem);

        // Now check skip for Storage provider commodities.
        CommoditiesBoughtFromProviderView checkStorageProvider = commoditiesByProvider.get(1);
        assertEquals(storageVolumeProviderId, checkStorageProvider.getProviderId());
        assertEquals(3, checkStorageProvider.getCommodityBoughtCount());
        final List<CommodityBoughtView> remainingBoughtStorage = cloudMigrationPlanHelper
                .getUpdatedCommBought(checkStorageProvider, consumptionTopologyInfo,
                        entity, Collections.emptyMap(), true, true);
        // There should not be any instance store commodities after the skip.
        assertEquals(0, remainingBoughtStorage.size());
    }

    /**
     * Test skip a configuration volume that associates with an on prem VM.
     * The configuration volume's CommmoditiesBoughtFromProvider list will be cleared. The VM to
     * configuration volume's CommmoditiesBoughtFromProvider will be skipped as well.
     */
    @Test
    public void skipConfigurationVolumeOnMigration() {

        long configVolumeProviderId = 101L;
        TopologyEntityImpl configVV = new TopologyEntityImpl()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setOid(configVolumeProviderId)
                .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(
                                CommodityType.STORAGE_AMOUNT.getNumber())))
                .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(
                                CommodityType.STORAGE_PROVISIONED.getNumber())));
        long diskVolumeProviderId = 102L;
        TopologyEntityImpl diskVV = new TopologyEntityImpl()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setOid(diskVolumeProviderId)
                .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(
                                CommodityType.STORAGE_ACCESS.getNumber())))
                .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(
                                CommodityType.STORAGE_AMOUNT.getNumber())))
                .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(
                                CommodityType.STORAGE_LATENCY.getNumber())))
                .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(
                        new CommodityTypeImpl().setType(
                                CommodityType.STORAGE_PROVISIONED.getNumber())));
        long vmId = 1234567L;
            TopologyEntityImpl vm = new TopologyEntityImpl().setOid(vmId)
                .setEntityType(VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                        .setProviderId(diskVolumeProviderId).setMovable(true)
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .addCommodityBought(new CommodityBoughtImpl().setActive(true)
                                .setCommodityType(new CommodityTypeImpl()
                                        .setType(CommodityType.STORAGE_ACCESS.getNumber())))
                        .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                                new CommodityTypeImpl().setType(
                                        CommodityType.STORAGE_AMOUNT.getNumber())))
                        .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                                new CommodityTypeImpl().setType(
                                        CommodityType.STORAGE_LATENCY.getNumber())))
                        .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                                new CommodityTypeImpl().setType(
                                        CommodityType.STORAGE_PROVISIONED.getNumber()))))
                .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                        .setProviderId(configVolumeProviderId)
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .addCommodityBought(new CommodityBoughtImpl().setActive(true)
                                .setCommodityType(new CommodityTypeImpl()
                                        .setType(CommodityType.STORAGE_AMOUNT.getNumber())))
                        .addCommodityBought(new CommodityBoughtImpl().setActive(true)
                                .setCommodityType(new CommodityTypeImpl()
                                        .setType(CommodityType.STORAGE_PROVISIONED.getNumber()))));

        TopologyGraph graph = TopologyEntityTopologyGraphCreator.newGraph(new HashMap() {{
            put(vmId, TopologyEntity.newBuilder(vm));
            put(configVolumeProviderId, TopologyEntity.newBuilder(configVV));
            put(diskVolumeProviderId, TopologyEntity.newBuilder(diskVV));
        }});
        TopologyEntity vmEntity = TopologyEntity.newBuilder(vm).build();
        cloudMigrationPlanHelper.prepareBoughtCommodities(vmEntity, consumptionTopologyInfo,
                new HashMap<>(), false, true, graph);
        // Verify that the vm has no CommoditiesBoughtFromProviders to config volume
        assertTrue(vm.getCommoditiesBoughtFromProvidersImplList().stream()
                .noneMatch(c -> c.getProviderId() == configVolumeProviderId));
        assertEquals(1, vm.getCommoditiesBoughtFromProvidersImplList().size());

        TopologyEntity vvEntity = TopologyEntity.newBuilder(configVV).build();
        cloudMigrationPlanHelper.prepareBoughtCommodities(vvEntity, consumptionTopologyInfo,
                new HashMap<>(), false, false, graph);
        // Verify that the configVV has no CommoditiesBoughtFromProviders
        assertEquals(0, configVV.getCommoditiesBoughtFromProvidersCount());
    }

    /**
     * Check that we correctly generate a Map to migrate the Volumes to Cloud.
     * If the Volume is connected to the same Storage the input entity is also connected.
     */
    @Test
    public void testGetStorageProviders() {
        long stId = 4444L;
        long st2Id = 3333L;
        long volumeId = 2222L;
        long vmId = 1111L;
        TopologyEntityImpl vmEntity = new TopologyEntityImpl().addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl().setProviderId(volumeId).setMovable(false)
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setScalable(false)
                        .addCommodityBought(new CommodityBoughtImpl().setCommodityType(
                                new CommodityTypeImpl().setType(CommodityType.STORAGE_AMOUNT_VALUE))))
                .setOid(vmId).setEntityType(VIRTUAL_MACHINE_VALUE).setEnvironmentType(EnvironmentType.ON_PREM);

        TopologyEntity.Builder stEntityImpl = TopologyEntity.newBuilder(new TopologyEntityImpl()
                .setOid(stId)
                .setEntityType(STORAGE_VALUE));

        TopologyEntity.Builder st2EntityImpl = TopologyEntity.newBuilder(new TopologyEntityImpl()
                .setOid(st2Id)
                .setEntityType(STORAGE_VALUE));

        TopologyEntity.Builder vvEntityImpl = TopologyEntity.newBuilder(new TopologyEntityImpl()
                .setOid(volumeId)
                .setEntityType(VIRTUAL_VOLUME_VALUE))
                .addOutboundAssociation(stEntityImpl);

        TopologyEntity.Builder vmEntityImpl = TopologyEntity.newBuilder(vmEntity)
                .addProvider(stEntityImpl)
                .addOutboundAssociation(vvEntityImpl);

        // Assert that the migration Map is populated because VM and Volume are consuming from the same Storage
        Map<EntityType, List<TopologyMigration.MigrationReference>> migrationMap = cloudMigrationPlanHelper
                .getStorageProviders(vmEntityImpl.build());

        assertFalse(migrationMap.isEmpty());

        // Assert that the migration Map is empty because VM and Volume are consuming from the different Storage
        TopologyEntity.Builder vmEntityWithDifferentSt = TopologyEntity.newBuilder(vmEntity)
                .addProvider(st2EntityImpl)
                .addOutboundAssociation(vvEntityImpl);

        migrationMap = cloudMigrationPlanHelper
                .getStorageProviders(vmEntityWithDifferentSt.build());

        assertTrue(migrationMap.isEmpty());
    }

    /**
     * Gets a list of CommoditiesBoughtFromProvider - one for PM and other for storage volume.
     *
     * @param pmProviderId Id of PM provider.
     * @param storageVolumeProviderId Id of storage volume provider.
     * @return List of CommoditiesBoughtFromProvider with dummy commodities added.
     */
    @Nonnull
    private List<CommoditiesBoughtFromProviderView> getCommoditiesBoughtFromProviders(
            long pmProviderId, long storageVolumeProviderId) {
        List<CommoditiesBoughtFromProviderView> commoditiesByProvider = new ArrayList<>();
        commoditiesByProvider.add(
                new CommoditiesBoughtFromProviderImpl()
                        .setProviderId(pmProviderId)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommodityBought(
                                new CommodityBoughtImpl()
                                        .setActive(true)
                                        .setCommodityType(
                                                new CommodityTypeImpl()
                                                        .setType(CommodityDTO.CommodityType
                                                                .Q2_VCPU.getNumber())))
                        .addCommodityBought(
                                new CommodityBoughtImpl()
                                        .setActive(true)
                                        .setCommodityType(
                                                new CommodityTypeImpl()
                                                        .setType(CommodityDTO.CommodityType
                                                                .CPU.getNumber())))
                        .addCommodityBought(
                                new CommodityBoughtImpl()
                                        .setActive(true)
                                        .setCommodityType(
                                                new CommodityTypeImpl()
                                                        .setType(CommodityDTO.CommodityType
                                                                .MEM.getNumber()))));

        commoditiesByProvider.add(
                new CommoditiesBoughtFromProviderImpl()
                        .setProviderId(storageVolumeProviderId)
                        .setProviderEntityType(EntityType.STORAGE_VOLUME_VALUE)
                        .addCommodityBought(
                                new CommodityBoughtImpl()
                                        .setActive(true)
                                        .setCommodityType(
                                                new CommodityTypeImpl()
                                                        .setType(CommodityDTO.CommodityType
                                                                .INSTANCE_DISK_SIZE.getNumber())))
                        .addCommodityBought(
                                new CommodityBoughtImpl()
                                        .setActive(true)
                                        .setCommodityType(
                                                new CommodityTypeImpl()
                                                        .setType(CommodityDTO.CommodityType
                                                                .INSTANCE_DISK_TYPE.getNumber())))
                        .addCommodityBought(
                                new CommodityBoughtImpl()
                                        .setActive(true)
                                        .setCommodityType(
                                                new CommodityTypeImpl()
                                                        .setType(CommodityDTO.CommodityType
                                                                .INSTANCE_DISK_COUNT.getNumber()))));

        return commoditiesByProvider;
    }
}