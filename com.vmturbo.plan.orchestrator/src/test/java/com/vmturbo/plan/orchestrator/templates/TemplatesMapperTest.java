package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecResource;
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
            .addResources(TemplateSpecResource.getDefaultInstance())
            .build();

        TemplateSpec pmTemplateSec = TemplateSpec.newBuilder()
            .setId(2)
            .setName("PM Template Spec")
            .addResources(TemplateSpecResource.getDefaultInstance())
            .build();

        TemplateSpec storageTempalteSpec = TemplateSpec.newBuilder()
            .setId(3)
            .setName("Storage Template Spec")
            .addResources(TemplateSpecResource.getDefaultInstance())
            .build();
        templateSpecMap.put(EntityType.VIRTUAL_MACHINE.toString(), vmTemplateSec);
        templateSpecMap.put(EntityType.PHYSICAL_MACHINE.toString(), pmTemplateSec);
        templateSpecMap.put(EntityType.STORAGE.toString(), storageTempalteSpec);
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
        List<TemplateField> templateFields = templateInstance.getResourcesList().stream()
            .filter(resource -> resource.getCategory().getName().equals(ResourcesCategoryName.Compute))
            .map(TemplateResource::getFieldsList)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        List<TemplateField> numCpuTemplateField = templateFields.stream()
            .filter(templateField -> templateField.getName().equals("numOfCpu"))
            .collect(Collectors.toList());

        List<TemplateField> vcpuSpeedTemplateField = templateFields.stream()
            .filter(templateField -> templateField.getName().equals("cpuSpeed"))
            .collect(Collectors.toList());

        assertEquals(numCpuTemplateField.size(), 1);
        assertEquals(numCpuTemplateField.get(0).getName(), "numOfCpu");
        assertEquals(numCpuTemplateField.get(0).getValue(), "1");

        assertEquals(vcpuSpeedTemplateField.size(), 1);
        assertEquals(vcpuSpeedTemplateField.get(0).getName(), "cpuSpeed");
        assertEquals(vcpuSpeedTemplateField.get(0).getValue(), "2.0");
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

        assertEquals(numCpuCoreTemplateField.size(), 1);
        assertEquals(numCpuCoreTemplateField.get(0).getName(), "numOfCores");
        assertEquals(numCpuCoreTemplateField.get(0).getValue(), "5");

        assertEquals(cpuSpeedTemplateField.size(), 1);
        assertEquals(cpuSpeedTemplateField.get(0).getName(), "cpuSpeed");
        assertEquals(cpuSpeedTemplateField.get(0).getValue(), "6.0");
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

        assertEquals(ioThroughputTemplateField.size(), 1);
        assertEquals(ioThroughputTemplateField.get(0).getName(), "ioThroughputConsumed");
        assertEquals(ioThroughputTemplateField.get(0).getValue(), "1024.0");
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

        assertEquals(disTopsTemplateField.size(), 1);
        assertEquals(disTopsTemplateField.get(0).getName(), "diskIops");
        assertEquals(disTopsTemplateField.get(0).getValue(), "2.0");

        assertEquals(diskSizeSpeedTemplateField.size(), 1);
        assertEquals(diskSizeSpeedTemplateField.get(0).getName(), "diskSize");
        assertEquals(diskSizeSpeedTemplateField.get(0).getValue(), "1024.0");
    }
}
