package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Unit test for {@link TemplatesRpcService}.
 */
public class TemplatesRpcTest {

    private TemplatesDao templatesDao = Mockito.mock(TemplatesDao.class);

    private TemplatesRpcService templatesRpcService = new TemplatesRpcService(templatesDao);

    private TemplateServiceBlockingStub templateServiceBlockingStub;

    private TemplateSpecParser templateSpecParser;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templatesRpcService);

    @Before
    public void init() throws Exception {
        templateSpecParser = Mockito.mock(TemplateSpecParser.class);
        templateServiceBlockingStub = TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    @Test
    public void testGetTemplates() {
        final GetTemplatesRequest request = GetTemplatesRequest.newBuilder().build();
        Set<Template> templateSet = Sets.newHashSet(Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build());
        Mockito.when(templatesDao.getAllTemplates()).thenReturn(templateSet);
        Iterator<Template> result = templateServiceBlockingStub.getTemplates(request);
        assertTrue(result.hasNext());
        assertEquals(templateSet, Sets.newHashSet(result));
    }

    @Test
    public void testGetTemplate() {
        final GetTemplateRequest request = GetTemplateRequest.newBuilder().setTemplateId(123).build();
        Optional<Template> templateOptional = Optional.of(Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build());
        Mockito.when(templatesDao.getTemplate(123)).thenReturn(templateOptional);
        Template result = templateServiceBlockingStub.getTemplate(request);
        assertEquals(result, templateOptional.get());
    }

    @Test
    public void testCreateTemplate() {
        TemplateInfo templateInstance = TemplateInfo.newBuilder().setName("test").build();
        final CreateTemplateRequest request = CreateTemplateRequest.newBuilder()
            .setTemplateInfo(templateInstance)
            .build();
        Template template = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(templateInstance)
            .build();
        Mockito.when(templatesDao.createTemplate(templateInstance)).thenReturn(template);
        Template result = templateServiceBlockingStub.createTemplate(request);
        assertEquals(result, template);
    }

    @Test
    public void testEditTemplate() throws NoSuchObjectException, IllegalTemplateOperationException {
        TemplateInfo templateInstance = TemplateInfo.newBuilder().setName("test").build();
        TemplateInfo newTemplateInstance = TemplateInfo.newBuilder().setName("new").build();
        final EditTemplateRequest request = EditTemplateRequest.newBuilder()
            .setTemplateId(123)
            .setTemplateInfo(templateInstance)
            .build();
        Template newTemplate = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(newTemplateInstance)
            .build();
        Mockito.when(templatesDao.editTemplate(123, templateInstance)).thenReturn(newTemplate);
        Template result = templateServiceBlockingStub.editTemplate(request);
        assertEquals(result, newTemplate);
    }

    @Test
    public void testDeleteTemplate() throws NoSuchObjectException, IllegalTemplateOperationException {
        final DeleteTemplateRequest request = DeleteTemplateRequest.newBuilder()
            .setTemplateId(123)
            .build();
        Template template = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build();
        Mockito.when(templatesDao.deleteTemplateById(123)).thenReturn(template);
        Template result = templateServiceBlockingStub.deleteTemplate(request);
        assertEquals(result, template);
    }

}
