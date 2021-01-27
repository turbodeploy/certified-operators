package com.vmturbo.topology.processor.workflow;

import java.util.List;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowParameter;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;

/**
 * Utilities for workflow discovery/processing tests.
 **/
public class DiscoveredWorkflowTestUtils {

    static final long TARGET_ID = 123L;
    public static final String WORKFLOW_TYPE_GEN_TEXT_INPUT = "gen_text_input";

    static List<Workflow> NME_WITH_WORKFLOWS = Lists.newArrayList(
            buildWorkflow("a"),
            buildWorkflow("b")
    );

    static List<WorkflowInfo> EXPECTED_WORKFLOW_DTOS = Lists.newArrayList(
            buildWorkflowDTO("a", TARGET_ID),
            buildWorkflowDTO("b", TARGET_ID)
    );

    static Workflow buildWorkflow(String name) {
        return Workflow.newBuilder()
                .setId("WORKFLOW-" + name)
                .setDisplayName("WORKFLOW " + name)
                .setDescription("description of " + "WORKFLOW " + name)
            .addParam(Workflow.Parameter.newBuilder()
                .setName("x-" + name)
                .setType(WORKFLOW_TYPE_GEN_TEXT_INPUT)
                .setDescription("description for: x-" + name)
                .setMandatory(true)
                .build())
            .addParam(Workflow.Parameter.newBuilder()
                .setName("y-" + name)
                .setType(WORKFLOW_TYPE_GEN_TEXT_INPUT)
                .setDescription("description for: y-" + name)
                .setMandatory(true)
                .build())
            .addParam(Workflow.Parameter.newBuilder()
                .setName("z-" + name)
                .setType(WORKFLOW_TYPE_GEN_TEXT_INPUT)
                .setDescription("description for: z-" + name)
                .setMandatory(false)
                .build())
            .build();
    }

    static WorkflowInfo buildWorkflowDTO(String name, long targetId) {
        return WorkflowInfo.newBuilder()
                .setName("WORKFLOW-" + name)
                .setDisplayName("WORKFLOW " + name)
                .setDescription("description of " + "WORKFLOW " + name)
                .setTargetId(targetId)
                .addAllWorkflowParam(Lists.newArrayList(
                        buildWorkflowParams("x-" + name, WORKFLOW_TYPE_GEN_TEXT_INPUT, true),
                        buildWorkflowParams("y-" + name, WORKFLOW_TYPE_GEN_TEXT_INPUT, true),
                        buildWorkflowParams("z-" + name, WORKFLOW_TYPE_GEN_TEXT_INPUT, false)
                ))
                .build();
    }

    static WorkflowParameter buildWorkflowParams(String paramName, String typeName, boolean mandatory) {
        return WorkflowParameter.newBuilder()
                .setName(paramName)
                .setType(typeName)
                .setMandatory(mandatory)
                .setDescription("description for: " + paramName)
                .build();
    }
}
