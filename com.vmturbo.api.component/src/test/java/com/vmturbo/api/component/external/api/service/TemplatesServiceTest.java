package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.external.api.mapper.TemplateMapper;
import com.vmturbo.api.dto.ResourceApiDTO;
import com.vmturbo.api.dto.TemplateApiDTO;
import com.vmturbo.api.dto.input.TemplateApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecByEntityTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpecResource;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceImplBase;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

public class TemplatesServiceTest {

    private final static TemplateInfo TEMPLATE_INFO = TemplateInfo.newBuilder()
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

    private final static Template TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(TEMPLATE_INFO)
        .build();

    private final static TemplateSpec TEMPLATE_SPEC = TemplateSpec.newBuilder()
        .setId(456)
        .setName("VM-template-spec")
        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
        .addResources(TemplateSpecResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Compute))
            .addFields(TemplateSpecField.newBuilder()
                .setName("numOfCpu")))
        .build();

    private final TestTemplateService templateService = Mockito.spy(new TestTemplateService());

    private final TestTemplateSpecService templateSpecService = Mockito.spy(new TestTemplateSpecService());

    private TemplatesService templatesService;

    private TemplateMapper templateMapper;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templateService, templateSpecService);

    @Before
    public void setup() throws IOException {
        templateMapper = new TemplateMapper();
        templatesService = new TemplatesService(TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            templateMapper, TemplateSpecServiceGrpc.newBlockingStub(grpcServer.getChannel()));

    }

    @Test
    public void testGetTemplates() throws Exception {
        List<TemplateApiDTO> result = templatesService.getTemplates();
        assertEquals(1, result.size());
        String expectedResult = objectMapper.writeValueAsString(templateMapper.mapToTemplateApiDTO(TEMPLATE, TEMPLATE_SPEC));
        String resultStr = objectMapper.writeValueAsString(result.get(0));
        assertEquals(expectedResult, resultStr);
    }

    @Test
    public void testGetTemplate() throws Exception {
        List<TemplateApiDTO> result = templatesService.getTemplate("123");
        assertEquals(1, result.size());
        String expectedResult = objectMapper.writeValueAsString(templateMapper.mapToTemplateApiDTO(TEMPLATE, TEMPLATE_SPEC));
        String resultStr = objectMapper.writeValueAsString(result.get(0));
        assertEquals(expectedResult, resultStr);
    }

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

        TemplateApiDTO result = templatesService.addTemplate(templateApiInputDTO);

        assertEquals("777", result.getUuid());
        assertEquals("test-template", result.getDisplayName());
        assertEquals(1, result.getComputeResources().size());
    }

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

        TemplateApiDTO result = templatesService.editTemplate(uuid, templateApiInputDTO);

        assertEquals(uuid, result.getUuid());
        assertEquals("new-template", result.getDisplayName());
        assertEquals(1, result.getComputeResources().size());
    }

    @Test
    public void testDeleteTemplate() throws Exception {
        final String uuid = String.valueOf(TEMPLATE.getId());
        Boolean result = templatesService.deleteTemplate(uuid);
        assertTrue(result);
    }

    private class TestTemplateService extends TemplateServiceImplBase {

        public void getTemplates(GetTemplatesRequest request,
                                 StreamObserver<Template> responseObserver) {
            responseObserver.onNext(TEMPLATE);
            responseObserver.onCompleted();
        }

        public void getTemplate(GetTemplateRequest request,
                                 StreamObserver<Template> responseObserver) {
            if (request.getTemplateId() == TEMPLATE.getId()) {
                responseObserver.onNext(TEMPLATE);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

        public void createTemplate(CreateTemplateRequest request,
                                   StreamObserver<Template> responseObserver) {
            final Template template = Template.newBuilder()
                .setId(777)
                .setTemplateInfo(request.getTemplateInfo())
                .build();
            responseObserver.onNext(template);
            responseObserver.onCompleted();
        }

        public void editTemplate(EditTemplateRequest request,
                                 StreamObserver<Template> responseObserver) {
            if (request.getTemplateId() == TEMPLATE.getId()) {
                final Template template = Template.newBuilder()
                    .setId(request.getTemplateId())
                    .setTemplateInfo(request.getTemplateInfo())
                    .build();
                responseObserver.onNext(template);
                responseObserver.onCompleted();
            }
            else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

        public void deleteTemplate(DeleteTemplateRequest request,
                                   StreamObserver<Template> responseObserver) {
            responseObserver.onNext(TEMPLATE);
            responseObserver.onCompleted();
        }

    }

    private class TestTemplateSpecService extends TemplateSpecServiceImplBase {
        public void getTemplateSpecs(GetTemplateSpecsRequest request,
                                     StreamObserver<TemplateSpec> responseObserver) {
            responseObserver.onNext(TEMPLATE_SPEC);
            responseObserver.onCompleted();
        }

        public void getTemplateSpec(GetTemplateSpecRequest request,
                                    StreamObserver<TemplateSpec> responseObserver) {
            if (request.getId() == TEMPLATE_SPEC.getId()) {
                responseObserver.onNext(TEMPLATE_SPEC);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

        public void getTemplateSpecByEntityType(GetTemplateSpecByEntityTypeRequest request,
                                                StreamObserver<TemplateSpec> responseObserver) {
            responseObserver.onNext(TEMPLATE_SPEC);
            responseObserver.onCompleted();
        }
    }
}
