package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
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
    private static final long WORKFLOW_OID = 1L;
    private static final String TARGET_CATEGORY = "CATEGORY";
    private static final String TARGET_TYPE = "TYPE";
    public static final String WORKFLOW_1_DISPLAYNAME = "workflow 1";
    public static final String WORKFLOW_1_NAME = "workflow-1";
    private static final String WORKFLOW_1_DESCRIPTION = "Workflow 1 description";
    private static final String WORKFLOW_1_SCRIPT_PATH = "/scripts/script1.sh";
    private static final String WORKFLOW_1_ACTION_TYPE_NAME = "MOVE";
    private static final String WORKFLOW_1_ACTION_PHASE_NAME = "PREP";
    private static final long WORKFLOW_1_TIME_LIMIT = 15*60;
    private static final String WORKFLOW_CLASSNAME = "Workflow";

    @Test
    public void testWorkflowMapperTest() {
        // arrange
        final WorkflowDTO.WorkflowInfo workflowInfo = WorkflowDTO.WorkflowInfo.newBuilder()
            .setTargetId(TARGET_OID)
            .setDisplayName(WORKFLOW_1_DISPLAYNAME)
            .setName(WORKFLOW_1_NAME)
            .setDescription(WORKFLOW_1_DESCRIPTION)
            .setScriptPath(WORKFLOW_1_SCRIPT_PATH)
            .setActionType(ActionType.valueOf(WORKFLOW_1_ACTION_TYPE_NAME))
            .setActionPhase(ActionPhase.valueOf(WORKFLOW_1_ACTION_PHASE_NAME))
            .setTimeLimitSeconds(WORKFLOW_1_TIME_LIMIT)
            .build();
        Workflow workflow = Workflow.newBuilder()
            .setId(WORKFLOW_OID)
            .setWorkflowInfo(workflowInfo)
            .build();
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
        // TODO: test result.getParameters()
    }
}
