package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByNameRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;

/**
 * Unit test for {@link TemplatesRpcService}.
 */
public class TemplatesRpcTest {

    private TemplatesDao templatesDao = mock(TemplatesDao.class);

    private TemplatesRpcService templatesRpcService = new TemplatesRpcService(templatesDao);

    private TemplateServiceBlockingStub templateServiceBlockingStub;

    private TemplateSpecParser templateSpecParser;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templatesRpcService);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void init() throws Exception {
        templateSpecParser = mock(TemplateSpecParser.class);
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
    public void testCreateTemplate() throws DuplicateTemplateException {
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
    public void testCreateTemplateWithDuplicates() throws DuplicateTemplateException {
        TemplateInfo templateInstance = TemplateInfo.newBuilder().setName("test1").build();
        final CreateTemplateRequest request = CreateTemplateRequest.newBuilder()
                .setTemplateInfo(templateInstance)
                .build();
        Mockito.when(templatesDao.createTemplate(templateInstance)).thenThrow(new DuplicateTemplateException("test1"));
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.ALREADY_EXISTS).descriptionContains("Template with name test1 already exist"));
        templateServiceBlockingStub.createTemplate(request);
    }


    @Test
    public void testEditTemplateToDuplicateName() throws DuplicateTemplateException, NoSuchObjectException, IllegalTemplateOperationException {
        TemplateInfo templateInstance = TemplateInfo.newBuilder().setName("test1").build();
        final EditTemplateRequest request = EditTemplateRequest.newBuilder()
                .setTemplateId(123)
                .setTemplateInfo(templateInstance)
                .build();
        Mockito.when(templatesDao.editTemplate(anyLong(), any(TemplateInfo.class))).thenThrow(new DuplicateTemplateException("test1"));
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.ALREADY_EXISTS).descriptionContains("Template with name test1 already exist"));
        templateServiceBlockingStub.editTemplate(request);
    }

    @Test
    public void testEditTemplate() throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {
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

    @Test
    public void testGetTemplatesByName() throws Exception {
        String templateName = "templateName";
        Mockito.when(templatesDao.getTemplatesByName(templateName))
                .thenReturn(Arrays.asList(Template.getDefaultInstance()));

        GetTemplatesByNameRequest request = GetTemplatesByNameRequest.newBuilder()
                .setTemplateName(templateName)
                .build();
        StreamObserver<Template> observer =
                (StreamObserver<Template>)mock(StreamObserver.class);
        templatesRpcService.getTemplatesByName(request, observer);

        Mockito.verify(observer).onNext(any(Template.class));
        Mockito.verify(observer).onCompleted();
    }
}
