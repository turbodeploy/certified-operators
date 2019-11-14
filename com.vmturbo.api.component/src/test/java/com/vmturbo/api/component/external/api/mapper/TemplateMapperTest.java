package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.dto.template.TemplateApiInputDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecResource;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

public class TemplateMapperTest {

    private TemplateMapper templateMapper = new TemplateMapper();

    private final static TemplateInfo TEMPLATE_VM_INFO = TemplateInfo.newBuilder()
        .setName("test-VM-template")
        .setTemplateSpecId(1)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Compute))
            .addFields(TemplateField.newBuilder()
                .setName("numOfCpu")
                .setValue("1.0")))
        .build();

    private final static TemplateInfo TEMPLATE_PM_INFO = TemplateInfo.newBuilder()
            .setName("test-PM-template")
            .setTemplateSpecId(2)
            .setEntityType(EntityType.PHYSICAL_MACHINE.getValue())
            .setCpuModel("cpu-model")
            .addResources(TemplateResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder()
                            .setName(ResourcesCategoryName.Infrastructure))
                    .addFields(TemplateField.newBuilder()
                            .setName("numOfCores")
                            .setValue("1.0")))
            .build();

    private final static TemplateInfo TEMPLATE_ST_INFO = TemplateInfo.newBuilder()
        .setName("test-ST-template")
        .setTemplateSpecId(3)
        .setEntityType(EntityType.STORAGE.getValue())
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Storage))
            .addFields(TemplateField.newBuilder()
                .setName("diskIops")
                .setValue("1.0")))
        .build();

    private final static Template TEMPLATE_VM = Template.newBuilder()
        .setId(11)
        .setTemplateInfo(TEMPLATE_VM_INFO)
        .build();

    private final static Template TEMPLATE_PM = Template.newBuilder()
        .setId(22)
        .setTemplateInfo(TEMPLATE_PM_INFO)
        .build();

    private final static Template TEMPLATE_ST = Template.newBuilder()
        .setId(33)
        .setTemplateInfo(TEMPLATE_ST_INFO)
        .build();

    private final static TemplateSpec TEMPLATE_VM_SPEC = TemplateSpec.newBuilder()
        .setId(1)
        .setName("VM-template-spec")
        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
        .addResources(TemplateSpecResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Compute))
            .addFields(TemplateSpecField.newBuilder()
                .setName("numOfCpu")))
        .build();

    private final static TemplateSpec TEMPLATE_PM_SPEC = TemplateSpec.newBuilder()
        .setId(2)
        .setName("PM-template-spec")
        .setEntityType(EntityType.PHYSICAL_MACHINE.getValue())
        .addResources(TemplateSpecResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Infrastructure))
            .addFields(TemplateSpecField.newBuilder()
                .setName("numOfCores")))
        .build();

    private final static TemplateSpec TEMPLATE_ST_SPEC = TemplateSpec.newBuilder()
        .setId(3)
        .setName("ST-template-spec")
        .setEntityType(EntityType.STORAGE.getValue())
        .addResources(TemplateSpecResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Storage))
            .addFields(TemplateSpecField.newBuilder()
                .setName("diskIops")))
        .build();

    @Test
    public void testMapVMTemplateToApiDTO() {
        final TemplateApiDTO templateApiDTO =
            templateMapper.mapToTemplateApiDTO(TEMPLATE_VM, TEMPLATE_VM_SPEC, Collections.emptyList());
        assertEquals("test-VM-template", templateApiDTO.getDisplayName());
        assertEquals(1, templateApiDTO.getComputeResources().size());
        assertEquals(1, templateApiDTO.getComputeResources().get(0).getStats().size());
        assertEquals("numOfCpu", templateApiDTO.getComputeResources().get(0).getStats().get(0).getName());
        assertEquals(1.0f, templateApiDTO.getComputeResources().get(0).getStats().get(0).getValue(), 0.000001);
    }

    @Test
    public void testMapPMTemplateToApiDTO() {
        final TemplateApiDTO templateApiDTO =
            templateMapper.mapToTemplateApiDTO(TEMPLATE_PM, TEMPLATE_PM_SPEC, Collections.emptyList());
        assertEquals("test-PM-template", templateApiDTO.getDisplayName());
        assertEquals("cpu-model", templateApiDTO.getCpuModel());
        assertEquals(1, templateApiDTO.getInfrastructureResources().size());
        assertEquals(1, templateApiDTO.getInfrastructureResources().get(0).getStats().size());
        assertEquals("numOfCores", templateApiDTO.getInfrastructureResources().get(0).getStats().get(0).getName());
        assertEquals(1.0f, templateApiDTO.getInfrastructureResources().get(0).getStats().get(0).getValue(), 0.000001);
    }

    @Test
    public void testMapSTTemplateToApiDTO() {
        final TemplateApiDTO templateApiDTO =
            templateMapper.mapToTemplateApiDTO(TEMPLATE_ST, TEMPLATE_ST_SPEC, Collections.emptyList());
        assertEquals("test-ST-template", templateApiDTO.getDisplayName());
        assertEquals(1, templateApiDTO.getStorageResources().size());
        assertEquals(1, templateApiDTO.getStorageResources().get(0).getStats().size());
        assertEquals("diskIops", templateApiDTO.getStorageResources().get(0).getStats().get(0).getName());
        assertEquals(1.0f, templateApiDTO.getStorageResources().get(0).getStats().get(0).getValue(), 0.000001);
    }

    @Test
    public void testVMApiInputDTOtoTemplateInfo() throws IllegalArgumentException {
        final TemplateApiInputDTO templateApiInputDTO = new TemplateApiInputDTO();
        templateApiInputDTO.setDisplayName("test-VM-template");
        templateApiInputDTO.setClassName("VirtualMachineProfile");
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName("numOfCpu");
        statApiDTO.setValue(1.0f);
        resourceApiDTO.setStats(Lists.newArrayList(statApiDTO));
        templateApiInputDTO.setComputeResources(Lists.newArrayList(resourceApiDTO));
        int entityType = EntityType.VIRTUAL_MACHINE.getValue();
        TemplateInfo templateInfo = templateMapper.mapToTemplateInfo(templateApiInputDTO, TEMPLATE_VM_SPEC, entityType);
        assertEquals(TEMPLATE_VM_INFO, templateInfo);
    }

    /**
     * Test mapping from an external TemplateApiInputDTO with fields for a PM
     * to a TemplateInfo internal protobuf.
     * @throws IllegalArgumentException
     */
    @Test
    public void testPMApiInputDTOtoTemplateInfo() throws IllegalArgumentException {
        final TemplateApiInputDTO templateApiInputDTO = new TemplateApiInputDTO();
        templateApiInputDTO.setDisplayName("test-PM-template");
        templateApiInputDTO.setClassName("PhysicalMachineProfile");
        templateApiInputDTO.setCpuModel("cpu-model");
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName("numOfCores");
        statApiDTO.setValue(1.0f);
        resourceApiDTO.setStats(Lists.newArrayList(statApiDTO));
        templateApiInputDTO.setInfrastructureResources(Lists.newArrayList(resourceApiDTO));
        int entityType = EntityType.PHYSICAL_MACHINE.getValue();
        TemplateInfo templateInfo = templateMapper.mapToTemplateInfo(templateApiInputDTO, TEMPLATE_PM_SPEC, entityType);
        assertEquals(TEMPLATE_PM_INFO, templateInfo);
    }

    @Test
    public void testSTApiInputDTOtoTemplateInfo() throws IllegalArgumentException {
        final TemplateApiInputDTO templateApiInputDTO = new TemplateApiInputDTO();
        templateApiInputDTO.setDisplayName("test-ST-template");
        templateApiInputDTO.setClassName("StorageProfile");
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName("diskIops");
        statApiDTO.setValue(1.0f);
        resourceApiDTO.setStats(Lists.newArrayList(statApiDTO));
        templateApiInputDTO.setStorageResources(Lists.newArrayList(resourceApiDTO));
        int entityType = EntityType.STORAGE.getValue();
        TemplateInfo templateInfo = templateMapper.mapToTemplateInfo(templateApiInputDTO, TEMPLATE_ST_SPEC, entityType);
        assertEquals(TEMPLATE_ST_INFO, templateInfo);
    }

    /**
     * Test a non valid template.
     *
     * @throws IllegalArgumentException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCheckNotValidTemplate() throws IllegalArgumentException {
        final TemplateApiInputDTO templateApiInputDTO = new TemplateApiInputDTO();
        templateApiInputDTO.setDisplayName("test-ST-template");
        templateApiInputDTO.setClassName("StorageProfile");
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName("numCpu");
        statApiDTO.setValue(1F);
        resourceApiDTO.setStats(Lists.newArrayList(statApiDTO));
        templateApiInputDTO.setStorageResources(Lists.newArrayList(resourceApiDTO));
        int entityType = EntityType.STORAGE.getValue();
        templateMapper.mapToTemplateInfo(templateApiInputDTO, TEMPLATE_ST_SPEC, entityType);
    }
}
