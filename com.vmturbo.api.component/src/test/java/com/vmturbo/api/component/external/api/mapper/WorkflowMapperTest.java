package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.HttpMethod;
import com.vmturbo.api.dto.workflow.WebhookApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.enums.OrchestratorType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;

/**
 * Test the mapper from internal Workflow DTO to External API WorkflowApiDTO.
 **/
public class WorkflowMapperTest {
    private static final long TARGET_OID = 111L;
    private static final String TARGET_OID_STRING = Long.toString(TARGET_OID);
    /**
     * The ID of workflow used for unit testing.
     */
    public static final long WORKFLOW_OID = 1L;
    private static final String TARGET_CATEGORY = "CATEGORY";
    private static final String TARGET_TYPE = "TYPE";
    public static final String WORKFLOW_1_DISPLAYNAME = "workflow 1";
    public static final String WORKFLOW_1_NAME = "workflow-1";
    private static final String WORKFLOW_1_DESCRIPTION = "Workflow 1 description";
    private static final String WORKFLOW_1_SCRIPT_PATH = "/scripts/script1.sh";
    private static final String WORKFLOW_1_ACTION_TYPE_NAME = "MOVE";
    private static final String WORKFLOW_1_ACTION_PHASE_NAME = "PRE";
    private static final long WORKFLOW_1_TIME_LIMIT = 15*60;
    private static final String WORKFLOW_CLASSNAME = "Workflow";
    private static final String WEBHOOK_URL = "http://turbonomic.com";
    private static final String TEMPLATE = "testTemplate";

    /**
     * Tests conversion of actionscript workflow to api object.
     */
    @Test
    public void testActionscriptWorkflowMapperTest() {
        // arrange
        Workflow workflow = createActionscriptWorkflow();
        TargetApiDTO target = new TargetApiDTO();
        target.setUuid(TARGET_OID_STRING);
        target.setCategory(TARGET_CATEGORY);
        target.setType(TARGET_TYPE);
        WorkflowMapper workflowMapper = new WorkflowMapper();
        // act
        WorkflowApiDTO result = workflowMapper.toUiWorkflowApiDTO(workflow, target);
        // assert
        assertThat(result.getClassName(), equalTo(WORKFLOW_CLASSNAME));
        assertThat(result.getUuid(), equalTo(Long.toString(WORKFLOW_OID)));
        assertThat(result.getDisplayName(), equalTo(WORKFLOW_1_DISPLAYNAME));
        assertThat(result.getDescription(), equalTo(WORKFLOW_1_DESCRIPTION));
        assertThat(result.getScriptPath(), equalTo(WORKFLOW_1_SCRIPT_PATH));
        assertThat(result.getActionType(), equalTo(ActionType.valueOf(WORKFLOW_1_ACTION_TYPE_NAME).name()));
        assertThat(result.getActionPhase(), equalTo(WORKFLOW_1_ACTION_PHASE_NAME));
        assertThat(result.getTimeLimitSeconds(), equalTo(WORKFLOW_1_TIME_LIMIT));
        assertThat(result.getDiscoveredBy(), equalTo(target));
        assertThat(result.getType(), equalTo(OrchestratorType.ACTION_SCRIPT));
        assertNull(result.getTypeSpecificDetails());
        // TODO: test result.getParameters()
    }

    /**
     * Tests conversion of webhook workflow to api object.
     */
    @Test
    public void testWebhookWorkflowMapperTest() {
        // ARRANGE
        Workflow workflow = createWebhookWorkflow();

        WorkflowMapper workflowMapper = new WorkflowMapper();

        // ACT
        WorkflowApiDTO workflowApiDTO = workflowMapper.toUiWorkflowApiDTO(workflow, new TargetApiDTO());

        // ASSERT
        verifyWebhookWorkflowEquality(workflowApiDTO, createWebhookWorkflowApiDto());
    }

    /**
     * Test the logic for converting API object internal workflow.
     */
    @Test
    public void testMapWebhookWorkflowFromApi() {
        // ARRANGE
        WorkflowApiDTO workflowApiDTO = createWebhookWorkflowApiDto();

        WorkflowMapper workflowMapper = new WorkflowMapper();

        // ACT
        Workflow workflow = workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO, WORKFLOW_1_NAME);

        // ASSERT
        assertThat(workflow, equalTo(createWebhookWorkflow()));
    }

    /**
     * Creates an instance of {@link WorkflowApiDTO} of type webhook.
     *
     * @return an instance of {@link WorkflowApiDTO}.
     */
    public static WorkflowApiDTO createWebhookWorkflowApiDto() {
        WorkflowApiDTO workflowApiDTO = new WorkflowApiDTO();
        workflowApiDTO.setClassName(WORKFLOW_CLASSNAME);
        workflowApiDTO.setUuid(String.valueOf(WORKFLOW_OID));
        workflowApiDTO.setDisplayName(WORKFLOW_1_DISPLAYNAME);
        workflowApiDTO.setType(OrchestratorType.WEBHOOK);
        workflowApiDTO.setDiscoveredBy(new TargetApiDTO());
        WebhookApiDTO webhookApiDTO = new WebhookApiDTO();
        webhookApiDTO.setUrl(WEBHOOK_URL);
        webhookApiDTO.setMethod(HttpMethod.POST);
        webhookApiDTO.setTemplate(TEMPLATE);
        workflowApiDTO.setTypeSpecificDetails(webhookApiDTO);
        return workflowApiDTO;
    }

    /**
     * Creates an instance of {@link Workflow} of type webhook.
     *
     * @return an instance of {@link Workflow}.
     */
    public static Workflow createWebhookWorkflow() {
        return createWebhookWorkflow(true);
    }

    /**
     * Creates an instance of {@link Workflow} of type webhook.
     *
     * @param populateName if true the name in workflow object gets set.
     * @return an instance of {@link Workflow}.
     */
    public static Workflow createWebhookWorkflow(boolean populateName) {
        final WorkflowDTO.WorkflowInfo.Builder workflowInfo = WorkflowDTO.WorkflowInfo.newBuilder()
            .setDisplayName(WORKFLOW_1_DISPLAYNAME)
            .setType(WorkflowDTO.OrchestratorType.WEBHOOK)
            .setWebhookInfo(WorkflowDTO.WorkflowInfo.WebhookInfo.newBuilder()
                .setUrl(WEBHOOK_URL)
                .setHttpMethod(WorkflowDTO.WorkflowInfo.WebhookInfo.HttpMethod.POST)
                .setTemplate(TEMPLATE)
                .build());
        if (populateName) {
            workflowInfo.setName(WORKFLOW_1_NAME);
        }

        Workflow workflow = Workflow.newBuilder()
            .setId(WORKFLOW_OID)
            .setWorkflowInfo(workflowInfo)
            .build();
        return workflow;
    }

    /**
     * Creates an instance of {@link Workflow} of type actionscript.
     *
     * @return an instance of {@link Workflow}.
     */
    public static Workflow createActionscriptWorkflow() {
        final WorkflowDTO.WorkflowInfo workflowInfo = WorkflowDTO.WorkflowInfo.newBuilder()
            .setTargetId(TARGET_OID)
            .setDisplayName(WORKFLOW_1_DISPLAYNAME)
            .setName(WORKFLOW_1_NAME)
            .setDescription(WORKFLOW_1_DESCRIPTION)
            .setScriptPath(WORKFLOW_1_SCRIPT_PATH)
            .setActionType(ActionType.valueOf(WORKFLOW_1_ACTION_TYPE_NAME))
            .setActionPhase(ActionPhase.valueOf(WORKFLOW_1_ACTION_PHASE_NAME))
            .setType(WorkflowDTO.OrchestratorType.ACTION_SCRIPT)
            .setTimeLimitSeconds(WORKFLOW_1_TIME_LIMIT)
            .build();
        Workflow workflow = Workflow.newBuilder()
            .setId(WORKFLOW_OID)
            .setWorkflowInfo(workflowInfo)
            .build();
        return workflow;
    }

    /**
     * Creates an instance of {@link WorkflowApiDTO} of type actionscript.
     *
     * @return an instance of {@link WorkflowApiDTO}.
     */
    public static WorkflowApiDTO createActionscriptApiWorkflow() {
        WorkflowApiDTO workflowApiDTO = new WorkflowApiDTO();
        workflowApiDTO.setClassName(WORKFLOW_CLASSNAME);
        workflowApiDTO.setUuid(String.valueOf(WORKFLOW_OID));
        workflowApiDTO.setDisplayName(WORKFLOW_1_DISPLAYNAME);
        workflowApiDTO.setDescription(WORKFLOW_1_DESCRIPTION);
        workflowApiDTO.setActionType(WORKFLOW_1_ACTION_TYPE_NAME);
        workflowApiDTO.setActionPhase(WORKFLOW_1_ACTION_PHASE_NAME);
        workflowApiDTO.setScriptPath(WORKFLOW_1_SCRIPT_PATH);
        workflowApiDTO.setTimeLimitSeconds(WORKFLOW_1_TIME_LIMIT);
        workflowApiDTO.setType(OrchestratorType.ACTION_SCRIPT);
        workflowApiDTO.setDiscoveredBy(new TargetApiDTO());
        return workflowApiDTO;
    }

    /**
     * Test if two webhook workflows API objects are equal.
     *
     * @param firstWorkflow first webhook workflow.
     * @param secondWorkflow second webhook workflow.
     */
    public static void verifyWebhookWorkflowEquality(WorkflowApiDTO firstWorkflow, WorkflowApiDTO secondWorkflow) {
        assertThat(firstWorkflow.getClassName(), equalTo(secondWorkflow.getClassName()));
        assertThat(firstWorkflow.getUuid(), equalTo(secondWorkflow.getUuid()));
        assertThat(firstWorkflow.getDisplayName(), equalTo(secondWorkflow.getDisplayName()));
        assertThat(firstWorkflow.getDescription(), equalTo(secondWorkflow.getDescription()));
        assertThat(firstWorkflow.getScriptPath(), equalTo(secondWorkflow.getScriptPath()));
        assertThat(firstWorkflow.getActionType(), equalTo(secondWorkflow.getActionType()));
        assertThat(firstWorkflow.getActionPhase(), equalTo(secondWorkflow.getActionPhase()));
        assertThat(firstWorkflow.getTimeLimitSeconds(), equalTo(secondWorkflow.getTimeLimitSeconds()));
        assertThat(firstWorkflow.getType(), equalTo(secondWorkflow.getType()));
        WebhookApiDTO firstWebhookApiDTO = (WebhookApiDTO)firstWorkflow.getTypeSpecificDetails();
        WebhookApiDTO secondWebhookApiDTO = (WebhookApiDTO)secondWorkflow.getTypeSpecificDetails();
        assertThat(firstWebhookApiDTO.getType(), equalTo("WebhookApiDTO"));
        assertThat(secondWebhookApiDTO.getType(), equalTo("WebhookApiDTO"));
        assertThat(firstWebhookApiDTO.getUrl(), equalTo(secondWebhookApiDTO.getUrl()));
        assertThat(firstWebhookApiDTO.getMethod(), equalTo(secondWebhookApiDTO.getMethod()));
        assertThat(firstWebhookApiDTO.getTemplate(), equalTo(secondWebhookApiDTO.getTemplate()));
    }
}
