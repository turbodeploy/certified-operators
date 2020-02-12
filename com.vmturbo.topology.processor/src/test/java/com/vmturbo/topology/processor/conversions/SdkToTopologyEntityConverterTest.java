package com.vmturbo.topology.processor.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ConsumerPolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ProviderPolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualDatacenterData;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.TagValues;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * Unit test for {@link SdkToTopologyEntityConverter}.
 */
public class SdkToTopologyEntityConverterTest {

    private static final long PM_POWEREDON_OID = 102L;
    private static final long PM_MAINTENANCE_OID = 103L;
    private static final long PM_FAILOVER_OID = 104L;
    private static final long VM_OID = 100L;
    private static final long DS_OID = 205L;
    private static final long POD_OID = 105L;
    private static final long APPLICATION_OID = 305L;

    private static final double DELTA = 1e-8;

    /**
     * Convert entities test.
     *
     * @throws IOException
     *      reading from file exception
     */
    @Test
    public void testConverter() throws IOException {
        CommonDTO.EntityDTO vmProbeDTO = messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        CommonDTO.EntityDTO pmPoweredonProbeDTO = messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        CommonDTO.EntityDTO pmMaintenanceProbeDTO = messageFromJsonFile("protobuf/messages/pm-2-maintenance.dto.json");
        CommonDTO.EntityDTO pmFailoverProbeDTO = messageFromJsonFile("protobuf/messages/pm-3-failover.dto.json");
        CommonDTO.EntityDTO dsProbeDTO = messageFromJsonFile("protobuf/messages/ds-1.dto.json");
        Map<Long, CommonDTO.EntityDTO> probeDTOs = Maps.newLinkedHashMap(); // preserve the order
        // The entities are placed in the map so that there are forward references (from the VM to the other two)
        probeDTOs.put(VM_OID, vmProbeDTO);
        probeDTOs.put(PM_POWEREDON_OID, pmPoweredonProbeDTO);
        probeDTOs.put(PM_MAINTENANCE_OID, pmMaintenanceProbeDTO);
        probeDTOs.put(PM_FAILOVER_OID, pmFailoverProbeDTO);
        probeDTOs.put(DS_OID, dsProbeDTO);
        final List<TopologyEntityDTO> topologyDTOs =
                SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(probeDTOs).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toList());
        assertEquals(5, topologyDTOs.size());
        // OIDs match
        TopologyEntityDTO vmTopologyDTO = findEntity(topologyDTOs, VM_OID);
        assertEquals(vmProbeDTO.getDisplayName(), vmTopologyDTO.getDisplayName());
        assertEquals(3, vmTopologyDTO.getCommoditySoldListCount()); // 2xVCPU, 1xVMem
        assertEquals(2, vmTopologyDTO.getCommoditiesBoughtFromProvidersCount()); // buying from two providers

        assertFalse(vmTopologyDTO.getAnalysisSettings().hasSuspendable());
        assertFalse(vmTopologyDTO.getAnalysisSettings().hasCloneable());

        for (CommoditiesBoughtFromProvider bought : vmTopologyDTO.getCommoditiesBoughtFromProvidersList()) {
            assertFalse(bought.hasMovable());
        }

        // check that vcpu P2 sold has the effective capacity limited
        Optional<CommoditySoldDTO> P2VCPUCommoditySold = vmTopologyDTO.getCommoditySoldListList().stream()
                .filter(commoditySoldDTO -> "P2".equals(commoditySoldDTO.getCommodityType().getKey()))
                .findFirst();
        assertTrue(P2VCPUCommoditySold.isPresent());
        // vcpu p2 effective capacity % should be 50%
        assertEquals(50.0, P2VCPUCommoditySold.get().getEffectiveCapacityPercentage(), DELTA);

        // check tags of the VM
        final Map<String, TagValuesDTO> vmTags = vmTopologyDTO.getTags().getTagsMap();
        assertEquals(3, vmTags.size());
        final List<String> valuesForKey1 = vmTags.get("key1").getValuesList();
        final List<String> valuesForKey2 = vmTags.get("key2").getValuesList();
        final List<String> valuesForKey3 = vmTags.get("key3").getValuesList();
        assertEquals(4, valuesForKey1.size());
        for (int i = 1; i <= 4; i++) {
            assertTrue(valuesForKey1.contains("value" + i));
        }
        assertEquals(1, valuesForKey2.size());
        assertEquals("value3", valuesForKey2.get(0));
        assertEquals(1, valuesForKey3.size());
        assertEquals("value5", valuesForKey3.get(0));

        CommoditiesBoughtFromProvider vmCommBoughtGrouping = vmTopologyDTO.getCommoditiesBoughtFromProvidersList().stream()
            .filter(commodityBoughtGrouping -> commodityBoughtGrouping.getProviderId() == PM_POWEREDON_OID)
            .findFirst()
            .get();

        assertNotNull(vmCommBoughtGrouping);
        assertEquals(3, vmCommBoughtGrouping.getCommodityBoughtCount()); // Mem, CPU, Ballooning
        assertTrue(isActive(vmCommBoughtGrouping.getCommodityBoughtList(), CommodityType.CPU_VALUE));
        assertFalse(isActive(vmCommBoughtGrouping.getCommodityBoughtList(), CommodityType.BALLOONING_VALUE));
        TypeSpecificInfo typeSpecificInfo = vmTopologyDTO.getTypeSpecificInfo();
        assertNotNull(typeSpecificInfo);
        assertTrue(typeSpecificInfo.hasVirtualMachine());
        VirtualMachineInfo vmInfo = typeSpecificInfo.getVirtualMachine();
        assertNotNull(vmInfo);
        assertEquals(Tenancy.DEFAULT, vmInfo.getTenancy());
        assertEquals(OSType.LINUX, vmInfo.getGuestOsInfo().getGuestOsType());
        assertEquals(OSType.LINUX.name(), vmInfo.getGuestOsInfo().getGuestOsName());
        List<IpAddress> ipAddress = vmInfo.getIpAddressesList();
        assertEquals(1, ipAddress.size());
        assertEquals("10.0.1.15", ipAddress.get(0).getIpAddress());
        assertFalse(ipAddress.get(0).getIsElastic());

        // check powered on pm
        TopologyEntityDTO pmPoweredOnTopologyDTO = findEntity(topologyDTOs, PM_POWEREDON_OID);
        assertTrue(isActive(pmPoweredOnTopologyDTO, CommodityType.CPU_VALUE));
        assertFalse(isActive(pmPoweredOnTopologyDTO, CommodityType.BALLOONING_VALUE));
        assertTrue(pmPoweredOnTopologyDTO.getEntityState() == EntityState.POWERED_ON);

        // check maintenance pm
        TopologyEntityDTO pmMaintenanceTopologyDTO = findEntity(topologyDTOs, PM_MAINTENANCE_OID);
        assertTrue(pmMaintenanceTopologyDTO.getEntityState() == EntityState.MAINTENANCE);

        // check failover pm
        TopologyEntityDTO pmFailoverTopologyDTO = findEntity(topologyDTOs, PM_FAILOVER_OID);
        assertTrue(pmFailoverTopologyDTO.getEntityState() == EntityState.FAILOVER);

        // check for st capacity constraint
        TopologyEntityDTO stTopologyDTO = findEntity(topologyDTOs, DS_OID);
        stTopologyDTO.getCommoditySoldListList().forEach(c -> {
            if (c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE) {
                assertEquals(10000, c.getMaxAmountForConsumer(), DELTA);
                assertEquals(100, c.getMinAmountForConsumer(), DELTA);
                assertEquals(3, c.getRatioDependency().getRatio(), DELTA);
            }
        });
    }

    private TopologyEntityDTO findEntity(List<TopologyEntityDTO> dtos, long oid) {
        return dtos.stream().filter(entity -> entity.getOid() == oid).findFirst().get();
    }

    private boolean isActive(List<CommodityBoughtDTO> list, int commodityType) {
        return list.stream()
            .filter(comm -> comm.getCommodityType().getType() == commodityType)
            .findFirst().get()
            .getActive();
    }

    private boolean isActive(TopologyEntityDTO dto, int commSoldType) {
        return dto.getCommoditySoldListList().stream()
                        .filter(comm -> comm.getCommodityType().getType() == commSoldType)
                        .findFirst().get()
                        .getActive();
    }

    private static final long VDC_OID = 100L;

    @Test
    public void testVDC() throws IOException {
        CommonDTO.EntityDTO vdcProbeDTO = messageFromJsonFile("protobuf/messages/vdc-1.dto.json");
        Map<Long, CommonDTO.EntityDTO> probeDTOs = Maps.newLinkedHashMap(); // preserve the order
        probeDTOs.put(VDC_OID, vdcProbeDTO);
        final List<TopologyEntityDTO> topologyDTOs =
                SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(probeDTOs).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toList());
        assertEquals(1, topologyDTOs.size());
        TopologyEntityDTO vdcTopologyDTO = topologyDTOs.get(0);
        assertEquals(EntityType.VIRTUAL_DATACENTER_VALUE, vdcTopologyDTO.getEntityType());
        assertEquals(CommodityType.MEM_ALLOCATION_VALUE,
                vdcTopologyDTO.getCommoditySoldList(0).getCommodityType().getType());
        // property map contains related field entries
        Map<String, String> vdcPropertiesMap = vdcTopologyDTO.getEntityPropertyMap();
        // Probe DTO properties map copied to topology DTO properties map
        for (EntityProperty property : vdcProbeDTO.getEntityPropertiesList()) {
            assertEquals(vdcPropertiesMap.get(property.getName()), property.getValue());
        }
        // In case someone changes the test file
        assertEquals("A Value", vdcPropertiesMap.get("A Key"));
        assertFalse(vdcTopologyDTO.getAnalysisSettings().getIsAvailableAsProvider());
        assertTrue(vdcTopologyDTO.getAnalysisSettings().getShopTogether());
    }

    @Test
    public void testDuplicateEntityPropertiesDoesNotThrowException() {
        final EntityDTO entityDTO = EntityBuilders.virtualMachine("foo")
            .property(EntityBuilders.entityProperty().named("duplicateProperty").withValue("value"))
            .property(EntityBuilders.entityProperty().named("duplicateProperty").withValue("value"))
            .build();

        final Map<Long, EntityDTO> probeDTOs = ImmutableMap.of(VM_OID, entityDTO);
        // This should generate warning messages in the log about duplicate properties.
        SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(probeDTOs);
    }

    /**
     * Load a json file into a DTO.
     * @param fileName the name of the file to load
     * @return The entity DTO represented by the file
     * @throws IOException when the file is not found
     */
    public static CommonDTO.EntityDTO messageFromJsonFile(String fileName) throws IOException {
        URL fileUrl = SdkToTopologyEntityConverterTest.class.getClassLoader().getResources(fileName)
                .nextElement();
        CommonDTO.EntityDTO.Builder builder = CommonDTO.EntityDTO.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(fileUrl.openStream()), builder);
        CommonDTO.EntityDTO message = builder.build();
        return message;
    }

    /**
     * Load a small topology with one of each: VM, PM, Storage, Datacenter and verify that the
     * accesses property is set properly when needed and not set when not needed.
     * @throws IOException if the test file can't be loaded properly
     */
    @Test
    public void testAccesses() throws IOException {
        CommonDTO.EntityDTO vm = messageFromJsonFile("protobuf/messages/accesses-vm.json");
        CommonDTO.EntityDTO pm = messageFromJsonFile("protobuf/messages/accesses-pm.json");
        CommonDTO.EntityDTO dc = messageFromJsonFile("protobuf/messages/accesses-dc.json");
        CommonDTO.EntityDTO st = messageFromJsonFile("protobuf/messages/accesses-st.json");
        Map<Long, CommonDTO.EntityDTO> probeDTOs = Maps.newHashMap();
        long VM_ID = 10;
        long PM_ID = 20;
        long DC_ID = 30;
        long ST_ID = 40;
        probeDTOs.put(VM_ID, vm);
        probeDTOs.put(PM_ID, pm);
        probeDTOs.put(DC_ID, dc);
        probeDTOs.put(ST_ID, st);
        final List<TopologyEntityDTO> topologyDTOs =
                SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(probeDTOs).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toList());

        // Assert that for all commodities sold that are not DSPM_ACCESS or DATASTORE
        // the accesses property is not set
        List<CommoditySoldDTO> commsSold = topologyDTOs.stream()
                        .map(TopologyEntityDTO::getCommoditySoldListList)
                        .flatMap(List::stream)
                        .filter(SdkToTopologyEntityConverterTest::isNotAccessCommodity)
                        .filter(CommoditySoldDTO::hasAccesses)
                        .collect(Collectors.toList());
        assertTrue(commsSold.isEmpty());

        // This is the accesses property of the DATASTORE commodity that the PM sells
        long pmAccesses = topologyDTOs.stream().filter(dto -> dto.getOid() == PM_ID).findFirst().get()
                        .getCommoditySoldListList().stream()
                        .filter(SdkToTopologyEntityConverterTest::isAccessCommodity)
                        .findFirst().get()
                        .getAccesses();
        assertEquals(ST_ID, pmAccesses);

        // This is the accesses property of the DSPM_ACCESS commodity that the ST sells
        long stAccesses = topologyDTOs.stream().filter(dto -> dto.getOid() == ST_ID).findFirst().get()
                        .getCommoditySoldListList().stream()
                        .filter(SdkToTopologyEntityConverterTest::isAccessCommodity)
                        .findFirst().get()
                        .getAccesses();
        assertEquals(PM_ID, stAccesses);
    }

    @Test
    public void testDiscoveredEntitySuspendability() {
        assertEquals(Optional.empty(), SdkToTopologyEntityConverter.calculateSuspendability(EntityDTO.newBuilder()
            .setId("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setOrigin(EntityOrigin.DISCOVERED)));
    }

    @Test
    public void testEntitySuspendabiligyWithStitchingEntity() {
        final EntityDTO.Builder pmEntityDto = EntityDTO.newBuilder()
                .setId("foo")
                .setEntityType(EntityType.PHYSICAL_MACHINE);
        final StitchingEntityData pmEntity = StitchingEntityData.newBuilder(pmEntityDto)
                .build();
        TopologyStitchingEntity pmStitchingEntity = new TopologyStitchingEntity(pmEntity);
        final EntityDTO.Builder storageEntityDto = EntityDTO.newBuilder()
                .setId("bar")
                .setEntityType(EntityType.STORAGE)
                .setProviderPolicy(ProviderPolicy.newBuilder()
                        .setLocalSupported(true));
        final StitchingEntityData storageEntity = StitchingEntityData.newBuilder(storageEntityDto)
                .build();
        TopologyStitchingEntity storageStitchingEntity = new TopologyStitchingEntity(storageEntity);
        storageStitchingEntity.addConsumer(pmStitchingEntity);
        assertEquals(Optional.of(false), SdkToTopologyEntityConverter.calculateSuspendabilityWithStitchingEntity(
                storageStitchingEntity));
    }

    @Test
    public void testProxyEntitySuspendability() {
        assertEquals(Optional.of(false), SdkToTopologyEntityConverter.calculateSuspendability(EntityDTO.newBuilder()
            .setId("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setOrigin(EntityOrigin.PROXY)));
    }

    @Test
    public void testReplacableEntitySuspendability() {
        assertEquals(Optional.of(false), SdkToTopologyEntityConverter.calculateSuspendability(EntityDTO.newBuilder()
            .setId("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setOrigin(EntityOrigin.REPLACEABLE)));
    }

    @Test
    public void testTopologyDTOCommodityKey() {
        final String testKey = "abc";
        final EntityDTO.Builder pmEntityDto = EntityDTO.newBuilder()
                .setId("foo")
                .setEntityType(EntityType.PHYSICAL_MACHINE)
                .addCommoditiesSold(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.MEM_PROVISIONED)
                        .setCapacity(100))
                .addCommoditiesSold(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.CLUSTER)
                        .setKey(testKey));
        final StitchingEntityData pmEntity = StitchingEntityData.newBuilder(pmEntityDto)
                .build();
        TopologyStitchingEntity pmStitchingEntity = new TopologyStitchingEntity(pmEntity);
        pmEntityDto.getCommoditiesSoldList().stream()
                .map(CommodityDTO::toBuilder)
                .forEach(commodity -> pmStitchingEntity.addCommoditySold(commodity, Optional.empty()));
        final TopologyEntityDTO.Builder pmBuilder = SdkToTopologyEntityConverter.newTopologyEntityDTO(pmStitchingEntity);
        assertEquals(2L, pmBuilder.getCommoditySoldListCount());
        assertFalse(pmBuilder.getCommoditySoldListList().stream()
                .filter(commoditySold ->
                        commoditySold.getCommodityType().getType() == CommodityType.MEM_PROVISIONED_VALUE)
                .allMatch(commoditySold -> commoditySold.getCommodityType().hasKey()));
        assertEquals(testKey, pmBuilder.getCommoditySoldListList().stream()
                .filter(commoditySold ->
                        commoditySold.getCommodityType().getType() == CommodityType.CLUSTER_VALUE)
                .map(commoditySold -> commoditySold.getCommodityType().getKey())
                .findFirst()
                .get());
    }

    // // Either monitored or controllable is false, set controllable to false.
    @Test
    public void testIsControllable() throws IOException {
        assertTrue(SdkToTopologyEntityConverter.isControllable(true, true));
        assertFalse(SdkToTopologyEntityConverter.isControllable(true, false));
        assertFalse(SdkToTopologyEntityConverter.isControllable(false, true));
        assertFalse(SdkToTopologyEntityConverter.isControllable(false, false));
    }

    /**
     * Test {@link SdkToTopologyEntityConverter#entityPropertyFilter(EntityProperty)}.
     */
    @Test
    public void testEntityPropertyFilter() {
        final String nameSpace = "default";
        final String name = "name";
        final String value = "value";

        EntityDTO.Builder builder = EntityDTO.newBuilder()
            .setEntityType(EntityType.UNKNOWN)
            .setId("id")
            .addEntityProperties(EntityProperty.newBuilder()
                .setName(StringConstants.CORE_QUOTA_PREFIX + "::subscriptionId::family")
                .setValue("10").setNamespace(nameSpace))
            .addEntityProperties(EntityProperty.newBuilder()
                .setName("LocalName")
                .setValue("supplyChainValue").setNamespace(nameSpace))
            .addEntityProperties(EntityProperty.newBuilder()
                .setName(name)
                .setValue(value).setNamespace(nameSpace));

        TopologyStitchingEntity stitchingEntity = new TopologyStitchingEntity(
            StitchingEntityData.newBuilder(builder).oid(1).build());
        final TopologyEntityDTO.Builder topologyDTO =
            SdkToTopologyEntityConverter.newTopologyEntityDTO(stitchingEntity);

        assertEquals(1, topologyDTO.getEntityPropertyMapMap().size());
        assertEquals(value, topologyDTO.getEntityPropertyMapMap().get(name));
    }

    @Test
    public void testCommodityUsedPercentage() {
        final long appOid = 1L;
        final long vmOid = 2L;
        final double appVcpuUsed = 10; // percentage
        final double appVmemUsed = 1024;
        final double appVStorageUsed = 40; // percentage
        final double appVcpuPeak = 20; // percentage
        final double appVmemPeak = 1600;
        final double appVStoragePeak = 5000;

        final double vmVcpuUsed = 30; // percentage
        final double vmVmemUsed = 1500;
        final double vmVStorageUsed = 5000;
        final double vmVcpuPeak = 50; // percentage
        final double vmVmemPeak = 1800;
        final double vmVStoragePeak = 70; // percentage
        final double vmVcpuCapacity = 2000;
        final double vmVmemCapacity = 2048;
        final double vmVStorageCapacity = 10000;

        EntityDTO.Builder appBuilder = EntityDTO.newBuilder()
            .setEntityType(EntityType.APPLICATION)
            .setId("app1")
            .addCommoditiesBought(CommodityBought.newBuilder()
                .setProviderId("vm1")
                .addBought(CommodityDTO.newBuilder()
                    .setCommodityType(CommodityType.VCPU)
                    .setUsed(appVcpuUsed)
                    .setPeak(appVcpuPeak)
                    // both used and peak are percentage
                    .setIsUsedPct(true)
                    .setIsPeakPct(true))
                .addBought(CommodityDTO.newBuilder()
                    .setCommodityType(CommodityType.VMEM)
                    .setUsed(appVmemUsed)
                    .setPeak(appVmemPeak))
                .addBought(CommodityDTO.newBuilder()
                    .setCommodityType(CommodityType.VSTORAGE)
                    .setUsed(appVStorageUsed)
                    .setPeak(appVStoragePeak)
                    .setIsUsedPct(true)));

        EntityDTO.Builder vmBuilder = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId("vm1")
            .addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.VCPU)
                .setUsed(vmVcpuUsed)
                .setPeak(vmVcpuPeak)
                .setCapacity(vmVcpuCapacity)
                // both used and peak are percentage
                .setIsUsedPct(true)
                .setIsPeakPct(true))
            .addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.VMEM)
                .setUsed(vmVmemUsed)
                .setPeak(vmVmemPeak)
                .setCapacity(vmVmemCapacity))
            .addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.VSTORAGE)
                .setUsed(vmVStorageUsed)
                .setPeak(vmVStoragePeak)
                .setCapacity(vmVStorageCapacity)
                // only peak is percentage
                .setIsPeakPct(true)
            );

        TopologyStitchingEntity appStitchingEntity = new TopologyStitchingEntity(
            StitchingEntityData.newBuilder(appBuilder).oid(appOid).build());
        TopologyStitchingEntity vmStitchingEntity = new TopologyStitchingEntity(
            StitchingEntityData.newBuilder(vmBuilder).oid(vmOid).build());

        appStitchingEntity.addProviderCommodityBought(vmStitchingEntity, new CommoditiesBought(
            appBuilder.getCommoditiesBoughtBuilder(0).getBoughtBuilderList()));

        vmBuilder.getCommoditiesSoldBuilderList()
            .forEach(commSold -> vmStitchingEntity.addCommoditySold(commSold, Optional.empty()));

        final TopologyEntityDTO.Builder appTopologyDTO =
            SdkToTopologyEntityConverter.newTopologyEntityDTO(appStitchingEntity);

        Map<Integer, CommodityBoughtDTO> commodityBoughtMap =
            appTopologyDTO.getCommoditiesBoughtFromProvidersList().stream()
                .filter(commoditiesBoughtFromProvider ->
                    commoditiesBoughtFromProvider.getProviderId() == vmOid)
                .flatMap(commoditiesBoughtFromProvider ->
                    commoditiesBoughtFromProvider.getCommodityBoughtList().stream())
                .collect(Collectors.toMap(comm -> comm.getCommodityType().getType(),
                    Function.identity()));

        CommodityBoughtDTO appVMEM = commodityBoughtMap.get(CommodityType.VMEM_VALUE);
        CommodityBoughtDTO appVCPU = commodityBoughtMap.get(CommodityType.VCPU_VALUE);
        CommodityBoughtDTO appVST = commodityBoughtMap.get(CommodityType.VSTORAGE_VALUE);

        // check app bought commodities
        assertEquals(appVmemUsed, appVMEM.getUsed(), DELTA);
        assertEquals(appVcpuUsed * vmVcpuCapacity / 100, appVCPU.getUsed(), DELTA);
        assertEquals(appVStorageUsed * vmVStorageCapacity / 100, appVST.getUsed(), DELTA);
        assertEquals(appVmemPeak, appVMEM.getPeak(), DELTA);
        assertEquals(appVcpuPeak * vmVcpuCapacity / 100, appVCPU.getPeak(), DELTA);
        assertEquals(appVStoragePeak, appVST.getPeak(), DELTA);

        final TopologyEntityDTO.Builder vmTopologyDTO =
            SdkToTopologyEntityConverter.newTopologyEntityDTO(vmStitchingEntity);
        Map<Integer, CommoditySoldDTO> commoditySoldMap =
            vmTopologyDTO.getCommoditySoldListList().stream()
                .collect(Collectors.toMap(comm -> comm.getCommodityType().getType(),
                    Function.identity()));

        CommoditySoldDTO vmVMEM = commoditySoldMap.get(CommodityType.VMEM_VALUE);
        CommoditySoldDTO vmVCPU = commoditySoldMap.get(CommodityType.VCPU_VALUE);
        CommoditySoldDTO vmVST = commoditySoldMap.get(CommodityType.VSTORAGE_VALUE);

        // check vm sold commodities
        assertEquals(vmVmemUsed, vmVMEM.getUsed(), DELTA);
        assertEquals(vmVcpuUsed * vmVcpuCapacity / 100, vmVCPU.getUsed(), DELTA);
        assertEquals(vmVStorageUsed, vmVST.getUsed(), DELTA);
        assertEquals(vmVmemPeak, vmVMEM.getPeak(), DELTA);
        assertEquals(vmVcpuPeak * vmVcpuCapacity / 100, vmVCPU.getPeak(), DELTA);
        assertEquals(vmVStoragePeak * vmVStorageCapacity / 100, vmVST.getPeak(), DELTA);

        // create a vm provider which doesn't sell vcpu commodity
        vmStitchingEntity.getTopologyCommoditiesSold().clear();
        vmStitchingEntity.addCommoditySold(vmBuilder.getCommoditiesSoldBuilder(1),
            Optional.empty());

        final TopologyEntityDTO.Builder appTopologyDTO1 =
            SdkToTopologyEntityConverter.newTopologyEntityDTO(appStitchingEntity);
        CommodityBoughtDTO appVCPU1 = appTopologyDTO1.getCommoditiesBoughtFromProvidersList()
            .stream()
            .filter(commoditiesBoughtFromProvider ->
                commoditiesBoughtFromProvider.getProviderId() == vmOid)
            .flatMap(commoditiesBoughtFromProvider ->
                commoditiesBoughtFromProvider.getCommodityBoughtList().stream())
            .filter(commodityBoughtDTO ->
                commodityBoughtDTO.getCommodityType().getType() == CommodityType.VCPU_VALUE)
            .findFirst()
            .get();
        // check that original percentage used is used if no matching commodity on provider side
        assertEquals(appVcpuUsed, appVCPU1.getUsed(), DELTA);
    }

    /**
     * Tests that {@link SdkToTopologyEntityConverter} preserves
     * the connections between cloud entities correctly.
     */
    @Test
    public void testConnections() {
        final long vmId = 1L;
        final long regId = 2L;

        final EntityDTO.Builder vmBuilder = EntityDTO.newBuilder()
                                                .setEntityType(EntityType.VIRTUAL_MACHINE)
                                                .setId(Long.toString(vmId));
        final EntityDTO.Builder regBuilder = EntityDTO.newBuilder()
                                                .setEntityType(EntityType.REGION)
                                                .setId(Long.toString(regId));
        final TopologyStitchingEntity vm = new TopologyStitchingEntity(
                                            StitchingEntityData.newBuilder(vmBuilder).oid(vmId).build());
        final TopologyStitchingEntity reg = new TopologyStitchingEntity(
                                            StitchingEntityData.newBuilder(regBuilder).oid(regId).build());
        vm.addConnectedTo(ConnectionType.AGGREGATED_BY_CONNECTION, reg);
        vm.addConnectedFrom(ConnectionType.AGGREGATED_BY_CONNECTION, reg);

        final TopologyEntityDTO.Builder vmTopologyEntityBuilder =
                SdkToTopologyEntityConverter.newTopologyEntityDTO(vm);
        final TopologyEntityDTO.Builder regTopologyEntityBuilder =
                SdkToTopologyEntityConverter.newTopologyEntityDTO(reg);

        assertEquals(1, vmTopologyEntityBuilder.getConnectedEntityListCount());
        assertEquals(regId, vmTopologyEntityBuilder.getConnectedEntityList(0).getConnectedEntityId());
        assertEquals(EntityType.REGION_VALUE,
                     vmTopologyEntityBuilder.getConnectedEntityList(0).getConnectedEntityType());
        assertEquals(ConnectionType.AGGREGATED_BY_CONNECTION,
                     vmTopologyEntityBuilder.getConnectedEntityList(0).getConnectionType());
        assertEquals(0, regTopologyEntityBuilder.getConnectedEntityListCount());
    }

    /**
     * Tests that {@link SdkToTopologyEntityConverter} translate the ConsumerPolicy.deletable to
     * AnalysisSetting.deletable when it is false.
     */
    @Test
    public void testVolumeConverterForConsumerPolicyWhenConsumerPolicyDeletableIsFalse() {
        final long vvId = 1L;
        final boolean isVolumeDeletable = false;

        final TopologyStitchingEntity vv = getTopologyStitchingEntityForVirtualVolume(vvId, isVolumeDeletable);

        final TopologyEntityDTO.Builder vvTopologyEntityBuilder =
            SdkToTopologyEntityConverter.newTopologyEntityDTO(vv);

        assertFalse(vvTopologyEntityBuilder.getAnalysisSettings().getDeletable());
    }

    /**
     * Tests that {@link SdkToTopologyEntityConverter} translate the ConsumerPolicy.deletable to
     * AnalysisSetting.deletable when it is true.
     */
    @Test
    public void testVolumeConverterForConsumerPolicyWhenConsumerPolicyDeletableIsTrue() {
        final long vvId = 1L;
        final boolean isVolumeDeletable = true;

        final TopologyStitchingEntity vv = getTopologyStitchingEntityForVirtualVolume(vvId, isVolumeDeletable);

        final TopologyEntityDTO.Builder vvTopologyEntityBuilder =
            SdkToTopologyEntityConverter.newTopologyEntityDTO(vv);

        assertTrue(vvTopologyEntityBuilder.getAnalysisSettings().getDeletable());
    }

    /**
     * Test converter for tags for tag with several values.
     */
    @Test
    public void testTagConverter() {
        final String tagName = "test_tag_name";
        final String tagValue1 = "test_tag_value1";
        final String tagValue2 = "test_tag_value2";
        final Map<String, TagValues> tagsMap = new HashMap<>();
        tagsMap.put(tagName,
                TagValues.newBuilder().addAllValue(Arrays.asList(tagValue1, tagValue2)).build());
        final Tags tags = SdkToTopologyEntityConverter.convertGroupTags(tagsMap);
        Assert.assertEquals(1, tags.getTagsMap().size());
        Assert.assertEquals(Arrays.asList(tagValue1, tagValue2),
                tags.getTagsMap().get(tagName).getValuesList());
    }

    /**
     * Test converter for tags when discovered group has no tags.
     */
    @Test
    public void testTagConverterWithoutInputTags() {
        final Tags tags = SdkToTopologyEntityConverter.convertGroupTags(Collections.emptyMap());
        Assert.assertTrue(tags.getTagsMap().isEmpty());
    }

    /**
     * Helper method to create {@link TopologyStitchingEntity} for Virtual Volume.
     *
     * @param vvId virtual volume id
     * @param deletable  volume's consumer policy of deletable
     * @return {@link TopologyStitchingEntity}
     */
    private static TopologyStitchingEntity getTopologyStitchingEntityForVirtualVolume(final long vvId,
                                                                                      final boolean deletable) {
        final EntityDTO.Builder vvNotDeletableBuilder = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME)
            .setId(Long.toString(vvId))
            .setConsumerPolicy(ConsumerPolicy.newBuilder()
                .setDeletable(deletable)
                .build());
        return new TopologyStitchingEntity(
            StitchingEntityData.newBuilder(vvNotDeletableBuilder).oid(vvId).build());
    }



    private static boolean isAccessCommodity(CommoditySoldDTO comm) {
        int type = comm.getCommodityType().getType();
        return type == CommodityType.DSPM_ACCESS_VALUE || type == CommodityType.DATASTORE_VALUE;
    }

    private static boolean isNotAccessCommodity(CommoditySoldDTO comm) {
        return !isAccessCommodity(comm);
    }

    /**
     * Test that the action eligibility settings from the SDK DTO
     * are transferred to the TopologyEntity DTO.
     * @throws IOException
     *      reading from file exception
     */
    @Test
    public void testNodeAndPodWithActionEligibility() throws IOException {
        // VM DTO containing suspendable = false
        CommonDTO.EntityDTO vmProbeDTO = messageFromJsonFile("protobuf/messages/kube-master-node-1.dto.json");
        // Pod DTO containing suspendable = false, cloneable = false, movable across providers = false
        CommonDTO.EntityDTO podProbeDTO = messageFromJsonFile("protobuf/messages/kube-daemon-pod-1.dto.json");

        Map<Long, CommonDTO.EntityDTO> probeDTOs = Maps.newLinkedHashMap(); // preserve the order
        // The entities are placed in the map so that there are forward references (from the VM to the other two)
        probeDTOs.put(VM_OID, vmProbeDTO);
        probeDTOs.put(POD_OID, podProbeDTO);
        final List<TopologyEntityDTO> topologyDTOs =
                SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(probeDTOs).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toList());

        // VM
        TopologyEntityDTO vmTopologyDTO = findEntity(topologyDTOs, VM_OID);
        // has suspendable setting and is disabled
        assertTrue(vmTopologyDTO.getAnalysisSettings().hasSuspendable());
        assertFalse(vmTopologyDTO.getAnalysisSettings().getSuspendable());

        // Pod
        TopologyEntityDTO podTopologyDTO = findEntity(topologyDTOs, POD_OID);
        // has suspendable setting and is disabled
        assertTrue(podTopologyDTO.getAnalysisSettings().hasSuspendable());
        assertFalse(podTopologyDTO.getAnalysisSettings().getSuspendable());
        // has cloneable setting and is disabled
        assertTrue(podTopologyDTO.getAnalysisSettings().hasCloneable());
        assertFalse(podTopologyDTO.getAnalysisSettings().getCloneable());

        // has movable setting for each provider and is disabled
          for (CommoditiesBoughtFromProvider bought : podTopologyDTO.getCommoditiesBoughtFromProvidersList()) {
              if (bought.getProviderEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                  assertTrue(bought.hasMovable());
                  assertFalse(bought.getMovable());
              }
              if (bought.getProviderEntityType() == EntityType.VIRTUAL_DATACENTER_VALUE) {
                  assertTrue(bought.hasMovable());
                  assertFalse(bought.getMovable());
              }
          }
    }

    /**
     * Test when application DTO does not provided action eligibility, the suspendable flag should
     * be respected to the definition of `checkAppSuspendability`.
     * @throws IOException
     *      reading from file exception
     */
    @Test
    public void testApplicationSuspendWithoutActionEligibility() throws IOException {
        // Application DTO containing no info
        CommonDTO.EntityDTO applicationProbeDTO = messageFromJsonFile("protobuf/messages/aws_engineering_entity_application.dto.json");

        Map<Long, CommonDTO.EntityDTO> probeDTOs = Maps.newLinkedHashMap();
        probeDTOs.put(APPLICATION_OID, applicationProbeDTO);
        final List<TopologyEntityDTO> topologyDTOs =
            SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(probeDTOs).stream()
                .map(TopologyEntityDTO.Builder::build)
                .collect(Collectors.toList());

        // APPICATION
        TopologyEntityDTO applicationTopologyDTO = findEntity(topologyDTOs, APPLICATION_OID);
        // has suspendable setting and is disabled
        assertTrue(applicationTopologyDTO.getAnalysisSettings().hasSuspendable());
        assertFalse(applicationTopologyDTO.getAnalysisSettings().getSuspendable());
    }
}
