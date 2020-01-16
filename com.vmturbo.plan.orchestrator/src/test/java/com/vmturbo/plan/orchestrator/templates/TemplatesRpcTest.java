package com.vmturbo.plan.orchestrator.templates;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.plan.orchestrator.reservation.ReservationDaoImpl;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.TemplateDTO.CreateTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.DeleteTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.EditTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateDTO.UpdateHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.UpdateHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImpl;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;

/**
 * Unit test for {@link TemplatesRpcService}.
 */
public class TemplatesRpcTest {

    private TemplatesDao templatesDao = mock(TemplatesDao.class);
    private DeploymentProfileDaoImpl deploymentProfileDao = mock(DeploymentProfileDaoImpl.class);
    private ReservationDaoImpl reservationDao = mock(ReservationDaoImpl.class);

    private TemplatesRpcService templatesRpcService = new TemplatesRpcService(templatesDao, deploymentProfileDao, reservationDao, 1);

    private TemplateServiceBlockingStub templateServiceBlockingStub;

    private TemplateSpecParser templateSpecParser;

    /**
     * GRPC test server used to simulate a server we can connect to with a client.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(templatesRpcService);

    /**
     * Expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Initialization code that runs before every test.
     */
    @Before
    public void init() {
        templateSpecParser = mock(TemplateSpecParser.class);
        templateServiceBlockingStub = TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Test that the RPC to get templates calls the underlying DAO correctly.
     */
    @Test
    public void testGetTemplates() {
        final GetTemplatesRequest request = GetTemplatesRequest.newBuilder().build();
        Set<Template> templateSet = Sets.newHashSet(Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build());
        when(templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance()))
            .thenReturn(templateSet);
        Iterator<Template> result = TemplateProtoUtil.flattenGetResponse(templateServiceBlockingStub.getTemplates(request))
            .map(SingleTemplateResponse::getTemplate)
            .iterator();
        assertTrue(result.hasNext());
        assertEquals(templateSet, Sets.newHashSet(result));
    }

    /**
     * Test that the RPC to get a single template calls the underlying DAO correctly.
     */
    @Test
    public void testGetTemplate() {
        final GetTemplateRequest request = GetTemplateRequest.newBuilder().setTemplateId(123).build();
        Optional<Template> templateOptional = Optional.of(Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build());
        when(templatesDao.getTemplate(123)).thenReturn(templateOptional);
        Template result = templateServiceBlockingStub.getTemplate(request).getTemplate();
        assertEquals(result, templateOptional.get());
    }

    /**
     * Test that the RPC to get templates with their deployment profiles calls the underlying
     * DAOs and combines the results correctly.
     */
    @Test
    public void testGetTemplatesWithDeploymentProfile() {
        final GetTemplatesRequest request = GetTemplatesRequest.newBuilder()
            .setIncludeDeploymentProfiles(true)
            .build();
        final Template template = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build();
        when(templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance()))
            .thenReturn(Collections.singleton(template));
        final DeploymentProfile profile1 = DeploymentProfile.newBuilder()
            .setId(7777)
            .build();
        final DeploymentProfile profile2 = DeploymentProfile.newBuilder()
            .setId(8888)
            .build();
        when(deploymentProfileDao.getDeploymentProfilesForTemplates(Collections.singleton(template.getId())))
            .thenReturn(Collections.singletonMap(template.getId(), Sets.newHashSet(profile1, profile2)));

        final SingleTemplateResponse result = TemplateProtoUtil.flattenGetResponse(
                templateServiceBlockingStub.getTemplates(request))
            .findFirst()
            // There should be a result.
            .get();
        assertThat(result.getTemplate(), is(template));
        assertThat(result.getDeploymentProfileList(), containsInAnyOrder(profile1, profile2));
    }

    /**
     * Test that the RPC to get a single template with its deployment profiles calls the underlying
     * DAOs and combines the results correctly.
     */
    @Test
    public void testGetTemplateWithDeploymentProfile() {
        final GetTemplateRequest request = GetTemplateRequest.newBuilder()
            .setTemplateId(123)
            .setIncludeDeploymentProfiles(true)
            .build();
        final Template template = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build();
        when(templatesDao.getTemplate(123L))
            .thenReturn(Optional.of(template));
        final DeploymentProfile profile1 = DeploymentProfile.newBuilder()
            .setId(7777)
            .build();
        final DeploymentProfile profile2 = DeploymentProfile.newBuilder()
            .setId(8888)
            .build();
        when(deploymentProfileDao.getDeploymentProfilesForTemplates(Collections.singleton(template.getId())))
            .thenReturn(Collections.singletonMap(template.getId(), Sets.newHashSet(profile1, profile2)));

        final SingleTemplateResponse result = templateServiceBlockingStub.getTemplate(request);
        assertThat(result.getTemplate(), is(template));
        assertThat(result.getDeploymentProfileList(), containsInAnyOrder(profile1, profile2));
    }

    /**
     * Test that the RPC to get a single template with its deployment profiles calls the underlying
     * DAOs and combines the results correctly.
     *
     * @throws DuplicateTemplateException because of TemplatesDao declaration.
     */
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
        when(templatesDao.createTemplate(templateInstance)).thenReturn(template);
        Template result = templateServiceBlockingStub.createTemplate(request);
        assertEquals(result, template);
    }

    /**
     * Test that the create templates RPC call properly handles the duplicate template name exception.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testCreateTemplateWithDuplicates() throws Exception {
        TemplateInfo templateInstance = TemplateInfo.newBuilder().setName("test1").build();
        final CreateTemplateRequest request = CreateTemplateRequest.newBuilder()
                .setTemplateInfo(templateInstance)
                .build();
        when(templatesDao.createTemplate(templateInstance)).thenThrow(new DuplicateTemplateException("test1"));
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.ALREADY_EXISTS).descriptionContains("Template with name test1 already exist"));
        templateServiceBlockingStub.createTemplate(request);
    }


    /**
     * Test that the edit templates RPC call properly handles the duplicate template name exception.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEditTemplateToDuplicateName() throws Exception {
        TemplateInfo templateInstance = TemplateInfo.newBuilder().setName("test1").build();
        final EditTemplateRequest request = EditTemplateRequest.newBuilder()
                .setTemplateId(123)
                .setTemplateInfo(templateInstance)
                .build();
        when(templatesDao.editTemplate(anyLong(), any(TemplateInfo.class))).thenThrow(new DuplicateTemplateException("test1"));
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.ALREADY_EXISTS).descriptionContains("Template with name test1 already exist"));
        templateServiceBlockingStub.editTemplate(request);
    }

    /**
     * Test that the edit templates RPC calls the DAO correctly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEditTemplate() throws Exception {
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
        when(templatesDao.editTemplate(123, templateInstance)).thenReturn(newTemplate);
        Template result = templateServiceBlockingStub.editTemplate(request);
        assertEquals(result, newTemplate);
    }

    /**
     * Test that the delete templates RPC calls the DAO correctly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDeleteTemplate() throws Exception {
        final DeleteTemplateRequest request = DeleteTemplateRequest.newBuilder()
            .setTemplateId(123)
            .build();
        Template template = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build();
        when(templatesDao.deleteTemplateById(123)).thenReturn(template);
        Template result = templateServiceBlockingStub.deleteTemplate(request);
        assertEquals(result, template);
    }

    /**
     * Test that if there is reservation depends on this user template, {@link StatusRuntimeException} will be thrown.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test(expected = StatusRuntimeException.class)
    public void testDeleteTemplateFailed() throws Exception {
        int value = 123;
        final DeleteTemplateRequest request = DeleteTemplateRequest.newBuilder()
                .setTemplateId(value)
                .build();
        Template template = Template.newBuilder()
                .setId(value)
                .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
                .build();
        when(reservationDao.getReservationsByTemplates(Collections.singleton(Integer.toUnsignedLong(value))))
                .thenReturn(Collections.singleton(ReservationDTO.Reservation.getDefaultInstance()));
       templateServiceBlockingStub.deleteTemplate(request);
    }

    /**
     * Tests getting the template for a cluster.
     */
    @Test
    public void testGetHeadroomTemplateForCluster() {
        final long groupId = 5L;
        final GetHeadroomTemplateRequest request = GetHeadroomTemplateRequest.newBuilder()
            .setGroupId(groupId).build();

        Template template = Template.newBuilder()
            .setId(123)
            .setTemplateInfo(TemplateInfo.newBuilder().setName("test").build())
            .build();

        when(templatesDao.getClusterHeadroomTemplateForGroup(groupId))
            .thenReturn(Optional.of(template));

        GetHeadroomTemplateResponse result = templateServiceBlockingStub
            .getHeadroomTemplateForCluster(request);

        assertEquals(template, result.getHeadroomTemplate());
    }

    /**
     * Tests getting the template for a cluster when the request does not have
     * any group id.
     */
    @Test
    public void testGetHeadroomTemplateForClusterNoGroupId() {
        final GetHeadroomTemplateRequest request = GetHeadroomTemplateRequest.newBuilder()
            .build();

        try {
            templateServiceBlockingStub
                .getHeadroomTemplateForCluster(request);
        } catch (StatusRuntimeException exception) {
            assertThat(exception, GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Group ID is missing"));
            return;
        }
        fail("exception was expected");
    }

    /**
     * Tests getting the template for a cluster when there is data access exception.
     */
    @Test
    public void testGetHeadroomTemplateForClusterDAException() {
        final long groupId = 5L;
        final GetHeadroomTemplateRequest request = GetHeadroomTemplateRequest.newBuilder()
            .setGroupId(groupId).build();

        when(templatesDao.getClusterHeadroomTemplateForGroup(groupId))
            .thenThrow(new DataAccessException("ERR1"));

        try {
            templateServiceBlockingStub
                .getHeadroomTemplateForCluster(request);
        } catch (StatusRuntimeException exception) {
            assertThat(exception, GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains("ERR1"));
            return;
        }
        fail("exception was expected");
    }

    /**
     * Tests updating the template for a cluster.
     */
    @Test
    public void testUpdateHeadroomTemplateForCluster() {
        final long groupId = 5L;
        final long templateId = 10;
        final UpdateHeadroomTemplateRequest request = UpdateHeadroomTemplateRequest.newBuilder()
            .setGroupId(groupId).setTemplateId(templateId).build();

        UpdateHeadroomTemplateResponse result =
            templateServiceBlockingStub.updateHeadroomTemplateForCluster(request);

        verify(templatesDao).setOrUpdateHeadroomTemplateForCluster(eq(groupId), eq(templateId));
    }

    /**
     * Tests updating headroom template when no template id has been set.
     */
    @Test
    public void testUpdateHeadroomTemplateForClusterNoTemplateId() {
        final long groupId = 5L;
        final UpdateHeadroomTemplateRequest request = UpdateHeadroomTemplateRequest.newBuilder()
            .setGroupId(groupId).build();

        try {
            templateServiceBlockingStub
                .updateHeadroomTemplateForCluster(request);
        } catch (StatusRuntimeException exception) {
            assertThat(exception, GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("template"));
            return;
        }
        fail("exception was expected");
    }

    /**
     * Tests setting the template id for a cluster when there is data access exception.
     */
    @Test
    public void testUpdateHeadroomTemplateForClusterDAException() {
        final long groupId = 5L;
        final long templateId = 10;
        final UpdateHeadroomTemplateRequest request = UpdateHeadroomTemplateRequest.newBuilder()
            .setGroupId(groupId).setTemplateId(templateId).build();

        doThrow(new DataAccessException("ERR1")).when(templatesDao)
            .setOrUpdateHeadroomTemplateForCluster(groupId, templateId);

        try {
            templateServiceBlockingStub
                .updateHeadroomTemplateForCluster(request);
        } catch (StatusRuntimeException exception) {
            assertThat(exception, GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains("ERR1"));
            return;
        }
        fail("exception was expected");
    }
}
