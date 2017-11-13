package com.vmturbo.topology.processor.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualDatacenterData;

/**
 * Unit test for {@link Converter}.
 */
public class ConverterTest {

    private static final long PM_OID = 102L;
    private static final long VM_OID = 100L;
    private static final long DS_OID = 205L;

    @Test
    public void testConverter() throws IOException {
        CommonDTO.EntityDTO vmProbeDTO = messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        CommonDTO.EntityDTO pmProbeDTO = messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        CommonDTO.EntityDTO dsProbeDTO = messageFromJsonFile("protobuf/messages/ds-1.dto.json");
        Map<Long, CommonDTO.EntityDTO> probeDTOs = Maps.newLinkedHashMap(); // preserve the order
        // The entities are placed in the map so that there are forward references (from the VM to the other two)
        probeDTOs.put(VM_OID, vmProbeDTO);
        probeDTOs.put(PM_OID, pmProbeDTO);
        probeDTOs.put(DS_OID, dsProbeDTO);
        final List<TopologyEntityDTO> topologyDTOs = Converter.convert(probeDTOs).stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toList());
        assertEquals(3, topologyDTOs.size());
        // OIDs match
        TopologyEntityDTO vmTopologyDTO = findEntity(topologyDTOs, EntityType.VIRTUAL_MACHINE_VALUE);
        assertEquals(vmProbeDTO.getDisplayName(), vmTopologyDTO.getDisplayName());
        assertEquals(3, vmTopologyDTO.getCommoditySoldListCount()); // 2xVCPU, 1xVMem
        assertEquals(2, vmTopologyDTO.getCommoditiesBoughtFromProvidersCount()); // buying from two providers

        CommoditiesBoughtFromProvider vmCommBoughtGrouping = vmTopologyDTO.getCommoditiesBoughtFromProvidersList().stream()
            .filter(commodityBoughtGrouping -> commodityBoughtGrouping.getProviderId() == PM_OID)
            .findFirst()
            .get();

        assertNotNull(vmCommBoughtGrouping);
        assertEquals(3, vmCommBoughtGrouping.getCommodityBoughtCount()); // Mem, CPU, Ballooning
        assertTrue(isActive(vmCommBoughtGrouping.getCommodityBoughtList(), CommodityType.CPU_VALUE));
        assertFalse(isActive(vmCommBoughtGrouping.getCommodityBoughtList(), CommodityType.BALLOONING_VALUE));

        TopologyEntityDTO pmTopologyDTO = findEntity(topologyDTOs, EntityType.PHYSICAL_MACHINE_VALUE);
        assertTrue(isActive(pmTopologyDTO, CommodityType.CPU_VALUE));
        assertFalse(isActive(pmTopologyDTO, CommodityType.BALLOONING_VALUE));
    }

    private TopologyEntityDTO findEntity(List<TopologyEntityDTO> dtos, int entityType) {
        return dtos.stream().filter(entity -> entity.getEntityType() == entityType).findFirst().get();
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
        final List<TopologyEntityDTO> topologyDTOs = Converter.convert(probeDTOs).stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toList());
        assertEquals(1, topologyDTOs.size());
        TopologyEntityDTO vdcTopologyDTO = topologyDTOs.get(0);
        assertEquals(EntityType.VIRTUAL_DATACENTER_VALUE, vdcTopologyDTO.getEntityType());
        assertEquals(CommodityType.MEM_ALLOCATION_VALUE,
                vdcTopologyDTO.getCommoditySoldList(0).getCommodityType().getType());
        // property map contains related field entries
        VirtualDatacenterData vdcData = vdcProbeDTO.getVirtualDatacenterData();
        Map<String, String> vdcPropertiesMap = vdcTopologyDTO.getEntityPropertyMap();
        for (Entry<FieldDescriptor, Object> entry : vdcData.getAllFields().entrySet()) {
            assertEquals(entry.getValue().toString(),
                vdcPropertiesMap.get(entry.getKey().toString()));
        }
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
        Converter.convert(probeDTOs); // This should generate warning messages in the log about duplicate properties.
    }

    /**
     * Load a json file into a DTO.
     * @param fileName the name of the file to load
     * @return The entity DTO represented by the file
     * @throws IOException when the file is not found
     */
    public static CommonDTO.EntityDTO messageFromJsonFile(String fileName) throws IOException {
        URL fileUrl = ConverterTest.class.getClassLoader().getResources(fileName).nextElement();
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
        final List<TopologyEntityDTO> topologyDTOs = Converter.convert(probeDTOs).stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toList());

        // Assert that for all commodities sold that are not DSPM_ACCESS or DATASTORE
        // the accesses property is not set
        List<CommoditySoldDTO> commsSold = topologyDTOs.stream()
                        .map(TopologyEntityDTO::getCommoditySoldListList)
                        .flatMap(List::stream)
                        .filter(ConverterTest::isNotAccessCommodity)
                        .filter(CommoditySoldDTO::hasAccesses)
                        .collect(Collectors.toList());
        assertTrue(commsSold.isEmpty());

        // This is the accesses property of the DATASTORE commodity that the PM sells
        long pmAccesses = topologyDTOs.stream().filter(dto -> dto.getOid() == PM_ID).findFirst().get()
                        .getCommoditySoldListList().stream()
                        .filter(ConverterTest::isAccessCommodity)
                        .findFirst().get()
                        .getAccesses();
        assertEquals(ST_ID, pmAccesses);

        // This is the accesses property of the DSPM_ACCESS commodity that the ST sells
        long stAccesses = topologyDTOs.stream().filter(dto -> dto.getOid() == ST_ID).findFirst().get()
                        .getCommoditySoldListList().stream()
                        .filter(ConverterTest::isAccessCommodity)
                        .findFirst().get()
                        .getAccesses();
        assertEquals(PM_ID, stAccesses);
    }

    private static boolean isAccessCommodity(CommoditySoldDTO comm) {
        int type = comm.getCommodityType().getType();
        return type == CommodityType.DSPM_ACCESS_VALUE || type == CommodityType.DATASTORE_VALUE;
    }

    private static boolean isNotAccessCommodity(CommoditySoldDTO comm) {
        return !isAccessCommodity(comm);
    }
}
