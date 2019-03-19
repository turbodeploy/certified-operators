package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecResource;
import com.vmturbo.plan.orchestrator.templates.exceptions.NoMatchingTemplateSpecException;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.PMProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;

public class TemplatesMapperTest {

    private Map<String, TemplateSpec> templateSpecMap = new HashMap<>();

    @Before
    public void init() {
        TemplateSpec vmTemplateSec = TemplateSpec.newBuilder()
            .setId(1)
            .setName("VM Template Spec")
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .addResources(TemplateSpecResource.newBuilder()
                .setCategory(ResourcesCategory.newBuilder()
                    .setName(ResourcesCategoryName.Compute))
                    .addFields(TemplateSpecField.newBuilder()
                        .setName("numOfCpu"))
                    .addFields(TemplateSpecField.newBuilder()
                        .setName("cpuConsumedFactor")
                        .setDefaultValue(0.5f))
                    .addFields(TemplateSpecField.newBuilder()
                        .setName("memoryConsumedFactor")
                        .setDefaultValue(0.75f)))
                .addResources(TemplateSpecResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder()
                        .setName(ResourcesCategoryName.Storage))
                    .addFields(TemplateSpecField.newBuilder()
                        .setName("diskConsumedFactor")
                        .setDefaultValue(1.0f)))
                .build();

        TemplateSpec pmTemplateSec = TemplateSpec.newBuilder()
            .setId(2)
            .setName("PM Template Spec")
            .addResources(TemplateSpecResource.getDefaultInstance())
            .build();

        TemplateSpec storageTemplateSpec = TemplateSpec.newBuilder()
            .setId(3)
            .setName("Storage Template Spec")
            .addResources(TemplateSpecResource.getDefaultInstance())
            .build();
        templateSpecMap.put(EntityType.VIRTUAL_MACHINE.toString(), vmTemplateSec);
        templateSpecMap.put(EntityType.PHYSICAL_MACHINE.toString(), pmTemplateSec);
        templateSpecMap.put(EntityType.STORAGE.toString(), storageTemplateSpec);
    }

    @Test
    public void testConvertDiscoveredVMTemplate() throws NoMatchingTemplateSpecException {
        EntityProfileDTO vmProfile = EntityProfileDTO.newBuilder()
            .setId("test-vm-template")
            .setDisplayName("VM-Template")
            .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE)
            .setVmProfileDTO(VMProfileDTO.newBuilder()
                .setNumVCPUs(1)
                .setVCPUSpeed(2.0f))
            .build();
        TemplateInfo templateInstance = TemplatesMapper.createTemplateInfo(vmProfile, templateSpecMap);
        List<TemplateField> computeTemplateFields = templateInstance.getResourcesList().stream()
            .filter(resource -> resource.getCategory().getName().equals(ResourcesCategoryName.Compute))
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        List<TemplateField> storageTemplateFields = templateInstance.getResourcesList().stream()
            .filter(resource -> resource.getCategory().getName().equals(ResourcesCategoryName.Storage))
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        List<TemplateField> numCpuTemplateField = computeTemplateFields.stream()
            .filter(templateField -> templateField.getName().equals("numOfCpu"))
            .collect(Collectors.toList());

        List<TemplateField> vcpuSpeedTemplateField = computeTemplateFields.stream()
            .filter(templateField -> templateField.getName().equals("cpuSpeed"))
            .collect(Collectors.toList());

        List<TemplateField> cpuConsumedFactor = computeTemplateFields.stream()
             .filter(templateField -> templateField.getName().equals("cpuConsumedFactor"))
             .collect(Collectors.toList());

        List<TemplateField> memConsumedFactor = computeTemplateFields.stream()
                .filter(templateField -> templateField.getName().equals("memoryConsumedFactor"))
                .collect(Collectors.toList());

        List<TemplateField> diskConsumedFactor = storageTemplateFields.stream()
                .filter(templateField -> templateField.getName().equals("diskConsumedFactor"))
                .collect(Collectors.toList());

        assertEquals(1, numCpuTemplateField.size());
        assertEquals("numOfCpu", numCpuTemplateField.get(0).getName());
        assertEquals("1", numCpuTemplateField.get(0).getValue());

        assertEquals(1, vcpuSpeedTemplateField.size());
        assertEquals("cpuSpeed", vcpuSpeedTemplateField.get(0).getName());
        assertEquals("2.0", vcpuSpeedTemplateField.get(0).getValue());

        assertEquals(1, cpuConsumedFactor.size());
        assertEquals("cpuConsumedFactor", cpuConsumedFactor.get(0).getName());
        assertEquals("0.5", cpuConsumedFactor.get(0).getValue());

        assertEquals(1, memConsumedFactor.size());
        assertEquals("memoryConsumedFactor", memConsumedFactor.get(0).getName());
        assertEquals("0.75", memConsumedFactor.get(0).getValue());

        assertEquals(1, diskConsumedFactor.size());
        assertEquals("diskConsumedFactor", diskConsumedFactor.get(0).getName());
        assertEquals("1.0", diskConsumedFactor.get(0).getValue());
    }

    @Test
    public void testConvertDiscoveredPMTemplate() throws NoMatchingTemplateSpecException {
        EntityProfileDTO vmProfile = EntityProfileDTO.newBuilder()
            .setId("test-pm-template")
            .setDisplayName("PM-Template")
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setPmProfileDTO(PMProfileDTO.newBuilder()
                .setNumCores(5)
                .setCpuCoreSpeed(6.0f))
            .build();
        TemplateInfo templateInstance = TemplatesMapper.createTemplateInfo(vmProfile, templateSpecMap);
        List<TemplateField> templateFields = templateInstance.getResourcesList().stream()
            .filter(resource -> resource.getCategory().getName().equals(ResourcesCategoryName.Compute))
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        List<TemplateField> numCpuCoreTemplateField = templateFields.stream()
            .filter(templateField -> templateField.getName().equals("numOfCores"))
            .collect(Collectors.toList());

        List<TemplateField> cpuSpeedTemplateField = templateFields.stream()
            .filter(templateField -> templateField.getName().equals("cpuSpeed"))
            .collect(Collectors.toList());

        assertEquals(1, numCpuCoreTemplateField.size());
        assertEquals("numOfCores", numCpuCoreTemplateField.get(0).getName());
        assertEquals("5", numCpuCoreTemplateField.get(0).getValue());

        assertEquals(1, cpuSpeedTemplateField.size());
        assertEquals("cpuSpeed", cpuSpeedTemplateField.get(0).getName());
        assertEquals("6.0", cpuSpeedTemplateField.get(0).getValue());
    }

    @Test
    public void testConvertDiscoveredTemplateWithCommodity() throws NoMatchingTemplateSpecException {
        EntityProfileDTO vmProfile = EntityProfileDTO.newBuilder()
            .setId("test-vm-template")
            .setDisplayName("VM-Template")
            .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE)
            .setVmProfileDTO(VMProfileDTO.newBuilder()
                .setNumVCPUs(7)
                .setVCPUSpeed(8.0f))
            .addCommodityProfile(CommodityProfileDTO.newBuilder()
                .setCommodityType(CommodityType.IO_THROUGHPUT)
                .setConsumed(1024.0f))
            .build();

        TemplateInfo templateInstance = TemplatesMapper.createTemplateInfo(vmProfile, templateSpecMap);
        List<TemplateField> templateFields = templateInstance.getResourcesList().stream()
            .filter(resource -> resource.getCategory().getName().equals(ResourcesCategoryName.Compute))
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        List<TemplateField> ioThroughputTemplateField = templateFields.stream()
            .filter(templateField -> templateField.getName().equals("ioThroughputConsumed"))
            .collect(Collectors.toList());

        assertEquals(1, ioThroughputTemplateField.size());
        assertEquals("ioThroughputConsumed", ioThroughputTemplateField.get(0).getName());
        assertEquals("1024.0", ioThroughputTemplateField.get(0).getValue());
    }

    @Test
    public void testConvertDiscoveredStorageTemplate() throws NoMatchingTemplateSpecException {
        EntityProfileDTO storageProfile = EntityProfileDTO.newBuilder()
            .setId("test-storage-template")
            .setDisplayName("Storage-Template")
            .setEntityType(EntityType.STORAGE)
            .addCommodityProfile(CommodityProfileDTO.newBuilder()
                .setCommodityType(CommodityType.STORAGE)
                .setCapacity(1024.0f))
            .addCommodityProfile(CommodityProfileDTO.newBuilder()
                .setCommodityType(CommodityType.STORAGE_ACCESS)
                .setCapacity(2.0f))
            .build();
        TemplateInfo templateInstance = TemplatesMapper.createTemplateInfo(storageProfile, templateSpecMap);
        List<TemplateField> templateFields = templateInstance.getResourcesList().stream()
            .filter(resource -> resource.getCategory().getName().equals(ResourcesCategoryName.Storage))
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        List<TemplateField> disTopsTemplateField = templateFields.stream()
            .filter(templateField -> templateField.getName().equals("diskIops"))
            .collect(Collectors.toList());

        List<TemplateField> diskSizeSpeedTemplateField = templateFields.stream()
            .filter(templateField -> templateField.getName().equals("diskSize"))
            .collect(Collectors.toList());

        assertEquals(1, disTopsTemplateField.size());
        assertEquals("diskIops", disTopsTemplateField.get(0).getName());
        assertEquals("2.0", disTopsTemplateField.get(0).getValue());

        assertEquals(1, diskSizeSpeedTemplateField.size());
        assertEquals("diskSize", diskSizeSpeedTemplateField.get(0).getName());
        assertEquals("1024.0", diskSizeSpeedTemplateField.get(0).getValue());
    }
}
