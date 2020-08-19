package com.vmturbo.topology.processor.identity.extractor;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.application;
import static com.vmturbo.platform.common.builders.EntityBuilders.diskArray;
import static com.vmturbo.platform.common.builders.EntityBuilders.entityProperty;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.processor;
import static com.vmturbo.platform.common.builders.EntityBuilders.storage;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static com.vmturbo.platform.common.builders.metadata.EntityIdentityMetadataBuilder.entityMetadata;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataBuilder;

/**
 * Test property extraction from DTOs.
 */
public class IdentifyingPropertyExtractorTest {

    /**
     * Assert that the identifying properties are found. Does not check heuristic.
     *
     * @param dto DTO containing the properties
     * @param metadata metadata describing the properties to extract
     * @param expectedPropertyValues The expected values for the properties being extracted
     * @return The descriptor for the entity represented by dto.
     * @throws Exception Any error that may occur during extraction
     */
    private EntityDescriptor testIdentifyingProperties(EntityDTO dto,
                                                       EntityIdentityMetadata metadata,
                                                       List<String> expectedPropertyValues) throws Exception {
        ServiceEntityIdentityMetadata identityMetadata =
            new ServiceEntityIdentityMetadataBuilder(metadata).build();
        EntityDescriptor descriptor =
            IdentifyingPropertyExtractor.extractEntityDescriptor(dto, identityMetadata);

        // Grab the extracted values
        Collection<PropertyDescriptor> properties = descriptor.getIdentifyingProperties(null);
        Collection<String> values = Collections2.transform(properties, PropertyDescriptor::getValue);

        // Compare the extracted values to the expected values
        assertEquals(expectedPropertyValues.size(), values.size());
        assertThat(values, containsInAnyOrder(expectedPropertyValues.toArray()));

        return descriptor;
    }

    @Test
    public void testExtractEntityDescriptionWithNoProperties() throws Exception {
        EntityDTO dto = virtualMachine("vm-1")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Collections.singletonList(dto.getEntityType().name()));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractEntityDescriptionWithOneProperty() throws Exception {
        EntityDTO dto = virtualMachine("vm-1")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .nonVolatileProp("id")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Lists.newArrayList(dto.getEntityType().name(), "vm-1"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractEntityDescriptionWithVolatileProperty() throws Exception {
        EntityDTO dto = virtualMachine("vm-1")
            .displayName("virtual-machine-1")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .volatileProp("displayName")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Lists.newArrayList(dto.getEntityType().name(), "virtual-machine-1"));
        List<PropertyDescriptor> volatileProperties = new ArrayList<>(descriptor.getVolatileProperties(null));

        assertEquals(1, volatileProperties.size());
        assertEquals("virtual-machine-1", volatileProperties.get(0).getValue());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractEntityDescriptionWithHeuristicProperty() throws Exception {
        EntityDTO dto = virtualMachine("vm-1")
            .displayName("virtual-machine-1")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .heuristicProp("displayName")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Collections.singletonList(dto.getEntityType().name()));
        assertEquals(0, descriptor.getVolatileProperties(null).size());

        List<PropertyDescriptor> heuristicProperties = new ArrayList<>(descriptor.getHeuristicProperties(null));
        assertEquals(1, heuristicProperties.size());
        assertEquals("virtual-machine-1", heuristicProperties.get(0).getValue());
    }

    @Test
    public void testExtractEntityDescriptionWithPropertyList() throws Exception {
        EntityDTO dto = storage("st-1")
            .underlying("pm-1")
            .underlying("pm-2")
            .underlying("pm-3")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.STORAGE)
            .nonVolatileProp("underlyingList")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Arrays.asList(dto.getEntityType().name(), "pm-1", "pm-2", "pm-3"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractEntityDescriptionWithManyProperties() throws Exception {
        EntityDTO dto = storage("st-1")
            .displayName("storage-1")
            .underlying("pm-1")
            .underlying("pm-2")
            .underlying("pm-3")
            .build();

        EntityIdentityMetadata metadata = entityMetadata(EntityType.STORAGE)
            .nonVolatileProp("underlyingList")
            .volatileProp("id")
            .heuristicProp("displayName")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Arrays.asList(dto.getEntityType().name(), "pm-1", "pm-2", "pm-3", "st-1"));
        assertEquals(1, descriptor.getVolatileProperties(null).size());
        assertEquals(1, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractionSetsRanks() throws Exception {
        EntityDTO dto = storage("st-1")
            .underlying("pm-1")
            .underlying("pm-2")
            .underlying("pm-3")
            .build();

        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .nonVolatileProp("underlyingList")
            .volatileProp("id")
            .build();

        ServiceEntityIdentityMetadata identityMetadata =
            new ServiceEntityIdentityMetadataBuilder(metadata).build();

        EntityDescriptor descriptor =
            IdentifyingPropertyExtractor.extractEntityDescriptor(dto, identityMetadata);

        Set<Integer> idPropertyRanks = new HashSet<>();
        for (PropertyDescriptor property : descriptor.getIdentifyingProperties(null)) {
            idPropertyRanks.add(property.getPropertyTypeRank());
            assertTrue(property.getPropertyTypeRank() >= 1); // Verify property rank >= 1
        }

        // Verify distinct property rank per property type (not property value!); include EntityType
        assertEquals(3, idPropertyRanks.size());
    }

    @Test
    public void testExtractEntityDescriptionWithApplicationComponentProperty() throws Exception {
        EntityDTO dto = application("app-1")
            .appType("GuestLoad")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.APPLICATION_COMPONENT)
            .nonVolatileProp("applicationData/type")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Lists.newArrayList(dto.getEntityType().name(), "GuestLoad"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractEntityDescriptionWithVirtualMachineProperty() throws Exception {
        EntityDTO dto = virtualMachine("vm-1")
            .ipAddress("10.10.100.20")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .nonVolatileProp("virtualMachineData/ipAddressList")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Lists.newArrayList(dto.getEntityType().name(), "10.10.100.20"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractEntityDescriptionWithDiskArrayProperty() throws Exception {
        EntityDTO dto = diskArray("da-1")
            .storageId("storage-123")
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.DISK_ARRAY)
            .nonVolatileProp("diskArrayData/storageIdList")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Lists.newArrayList(dto.getEntityType().name(), "storage-123"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractPhysicalMachineProcessorCapacities() throws Exception {
        EntityDTO dto = physicalMachine("pm-1")
            .processor(processor("pm-1").displayName("phys-mach1").capacityMhz(100.0))
            .processor(processor("pm-2").displayName("phys-mach1").capacityMhz(250.0))
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.PHYSICAL_MACHINE)
            .nonVolatileProp("physicalMachineRelatedData/processorList/capacity*")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Arrays.asList(dto.getEntityType().name(), "100.0", "250.0"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractCustomProperty() throws Exception {
        EntityDTO dto = virtualMachine("vm-1")
            .property(entityProperty().named("customproperty").withValue("customvalue"))
            .buying(cpuMHz().from("pm-1"))
            .build();

        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .nonVolatileProp("entityPropertiesList(name,customproperty)/value*")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Lists.newArrayList(dto.getEntityType().name(), "customvalue"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }

    @Test
    public void testExtractVirtualMachineCpuProvider() throws Exception {
        EntityDTO dto = virtualMachine("vm-1")
            .buying(cpuMHz().from("pm-1", EntityType.PHYSICAL_MACHINE))
            .build();
        EntityIdentityMetadata metadata = entityMetadata(EntityType.VIRTUAL_MACHINE)
            .nonVolatileProp("commoditiesBoughtList(providerType,PHYSICAL_MACHINE)/providerId*")
            .build();

        EntityDescriptor descriptor = testIdentifyingProperties(dto, metadata,
            Lists.newArrayList(dto.getEntityType().name(), "pm-1"));
        assertEquals(0, descriptor.getVolatileProperties(null).size());
        assertEquals(0, descriptor.getHeuristicProperties(null).size());
    }
}
