package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.CpuInfoMapper;
import com.vmturbo.api.component.external.api.mapper.TemplateMapper;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.dto.template.TemplateApiInputDTO;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecByEntityTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateSpecServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Unit tests for {@link TemplatesService}.
 */
public class TemplatesServiceTest {

    private static final TemplateInfo TEMPLATE_INFO = TemplateInfo.newBuilder()
        .setName("test-template")
        .setTemplateSpecId(456)
        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Compute))
            .addFields(TemplateField.newBuilder()
                .setName("numOfCpu")
                .setValue("1")))
        .build();

    private static final Template TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(TEMPLATE_INFO)
        .build();

    private static final TemplateSpec TEMPLATE_SPEC = TemplateSpec.newBuilder()
        .setId(456)
        .setName("VM-template-spec")
        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
        .addResources(TemplateSpecResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Compute))
            .addFields(TemplateSpecField.newBuilder()
                .setName("numOfCpu")))
        .build();

    private static final int CPU_CATALOG_LIFE_HOURS = 1;

    private final TemplateServiceMole templateService = Mockito.spy(TemplateServiceMole.class);

    private final TemplateSpecServiceMole templateSpecService = Mockito.spy(TemplateSpecServiceMole.class);

    private TemplatesService templatesService;

    private TemplateMapper templateMapper = mock(TemplateMapper.class);

    private TemplatesUtils templatesUtils = mock(TemplatesUtils.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * The expected exception in the current test.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * gRPC server to help mock out gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templateService, templateSpecService);

    /**
     * Common setup code for all tests.
     */
    @Before
    public void setup() {
        final CpuInfoMapper cpuInfoMapper = new CpuInfoMapper();
        templatesService = new TemplatesService(
            TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            templateMapper, TemplateSpecServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            CpuCapacityServiceGrpc.newBlockingStub(grpcServer.getChannel()), cpuInfoMapper,
            templatesUtils,
            CPU_CATALOG_LIFE_HOURS);

    }

    /**
     * Test that getting the templates forwards to the helper class.
     */
    @Test
    public void testGetTemplates() {
        TemplateApiDTO templateApiDTO = new TemplateApiDTO();
        when(templatesUtils.getTemplates(any()))
            .thenReturn(Stream.of(templateApiDTO));
        final List<TemplateApiDTO> result = templatesService.getTemplates();
        assertThat(result, containsInAnyOrder(templateApiDTO));
        verify(templatesUtils).getTemplates(GetTemplatesRequest.getDefaultInstance());
    }

    /**
     * Test that getting templates by entity type forwards to the helper class.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetTemplateByType() throws Exception {
        TemplateApiDTO templateApiDTO = new TemplateApiDTO();
        when(templatesUtils.getTemplates(any()))
            .thenReturn(Stream.of(templateApiDTO));
        final List<TemplateApiDTO> result = templatesService.getTemplate(UIEntityType.VIRTUAL_MACHINE.apiStr());
        assertThat(result, containsInAnyOrder(templateApiDTO));
        verify(templatesUtils).getTemplates(GetTemplatesRequest.newBuilder()
            .setFilter(TemplatesFilter.newBuilder()
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .build());
    }

    /**
     * Test that getting templates by name forwards to the helper class.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetTemplateByName() throws Exception {
        TemplateApiDTO templateApiDTO = new TemplateApiDTO();
        when(templatesUtils.getTemplates(any()))
            .thenReturn(Stream.of(templateApiDTO));
        final List<TemplateApiDTO> result = templatesService.getTemplate("my name");
        assertThat(result, containsInAnyOrder(templateApiDTO));
        verify(templatesUtils).getTemplates(GetTemplatesRequest.newBuilder()
            .setFilter(TemplatesFilter.newBuilder()
                .addTemplateName("my name"))
            .build());
    }

    /**
     * Test that getting templates by id forwards to the helper class.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetTemplateById() throws Exception {
        TemplateApiDTO templateApiDTO = new TemplateApiDTO();
        when(templatesUtils.getTemplates(any()))
            .thenReturn(Stream.of(templateApiDTO));
        final List<TemplateApiDTO> result = templatesService.getTemplate("7");
        assertThat(result, containsInAnyOrder(templateApiDTO));
        verify(templatesUtils).getTemplates(GetTemplatesRequest.newBuilder()
            .setFilter(TemplatesFilter.newBuilder()
                .addTemplateIds(7L))
            .build());
    }

    /**
     * Test that adding a template correctly converts and forwards the request to the relevant gRPC
     * endpoint and returns + converts the result.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAddTemplate() throws Exception {
        final TemplateApiInputDTO templateApiInputDTO = new TemplateApiInputDTO();
        templateApiInputDTO.setDisplayName("test-template");
        templateApiInputDTO.setClassName("VirtualMachineProfile");
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName("numOfCpu");
        statApiDTO.setValue(1.0f);
        resourceApiDTO.setStats(Lists.newArrayList(statApiDTO));
        templateApiInputDTO.setComputeResources(Lists.newArrayList(resourceApiDTO));

        doReturn(TEMPLATE_SPEC).when(templateSpecService)
            .getTemplateSpecByEntityType(GetTemplateSpecByEntityTypeRequest.newBuilder()
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .build());

        doReturn(TEMPLATE_INFO).when(templateMapper)
            .mapToTemplateInfo(templateApiInputDTO, TEMPLATE_SPEC, UIEntityType.VIRTUAL_MACHINE.typeNumber());

        doReturn(TEMPLATE).when(templateService).createTemplate(CreateTemplateRequest.newBuilder()
            .setTemplateInfo(TEMPLATE_INFO)
            .build());

        TemplateApiDTO retTemplate = new TemplateApiDTO();
        when(templateMapper.mapToTemplateApiDTO(TEMPLATE, TEMPLATE_SPEC, Collections.emptyList()))
            .thenReturn(retTemplate);

        TemplateApiDTO result = templatesService.addTemplate(templateApiInputDTO);

        assertThat(result, is(retTemplate));
    }

    /**
     * Test that editing a template correctly converts and forwards the request to the relevant gRPC
     * endpoint and returns + converts the result.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEditTemplate() throws Exception {
        final String uuid = String.valueOf(TEMPLATE.getId());
        final TemplateApiInputDTO templateApiInputDTO = new TemplateApiInputDTO();
        templateApiInputDTO.setDisplayName("new-template");
        templateApiInputDTO.setClassName("VirtualMachineProfile");
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName("numOfCpu");
        statApiDTO.setValue(2.0f);
        resourceApiDTO.setStats(Lists.newArrayList(statApiDTO));
        templateApiInputDTO.setComputeResources(Lists.newArrayList(resourceApiDTO));

        doReturn(TEMPLATE_SPEC).when(templateSpecService)
            .getTemplateSpec(GetTemplateSpecRequest.newBuilder()
                .setId(TEMPLATE_INFO.getTemplateSpecId())
                .build());

        doReturn(SingleTemplateResponse.newBuilder()
            .setTemplate(TEMPLATE)
            .build()).when(templateService).getTemplate(GetTemplateRequest.newBuilder()
                .setTemplateId(TEMPLATE.getId())
                .build());

        doReturn(TEMPLATE_INFO).when(templateMapper)
            .mapToTemplateInfo(templateApiInputDTO, TEMPLATE_SPEC, UIEntityType.VIRTUAL_MACHINE.typeNumber());

        doReturn(TEMPLATE).when(templateService).editTemplate(EditTemplateRequest.newBuilder()
            .setTemplateId(TEMPLATE.getId())
            .setTemplateInfo(TEMPLATE_INFO)
            .build());

        TemplateApiDTO retTemplate = new TemplateApiDTO();
        when(templateMapper.mapToTemplateApiDTO(TEMPLATE, TEMPLATE_SPEC, Collections.emptyList()))
            .thenReturn(retTemplate);

        TemplateApiDTO result = templatesService.editTemplate(uuid, templateApiInputDTO);
        assertThat(result, is(retTemplate));
    }

    /**
     * Test that deleting a template forwards the call to the utility method.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDeleteTemplate() throws Exception {
        final String uuid = String.valueOf(TEMPLATE.getId());
        Boolean result = templatesService.deleteTemplate(uuid);
        assertTrue(result);
    }
}
