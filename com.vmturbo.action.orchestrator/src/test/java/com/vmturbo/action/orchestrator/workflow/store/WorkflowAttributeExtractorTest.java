package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_TARGET_ID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.junit.Test;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.identity.attributes.IdentityMatchingAttribute;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;

/**
 * Test the class that extracts identifying attributes from a WorkflowItem.
 */
public class WorkflowAttributeExtractorTest {

    public static final String TEST_WORKFLOW_NAME = "testWorkflow";
    public static final long TEST_TARGET_ID = 123L;

    /**
     * Test that the WorkflowInfo external name and target id fields are extracted into the
     * IdentityMatchingAttributes correctly.
     */
    @Test
    public void testExtractAttributes() {
        // arrange
        WorkflowInfo testItem = WorkflowInfo.newBuilder()
                .setName(TEST_WORKFLOW_NAME)
                .setTargetId(TEST_TARGET_ID)
                .build();
        // a workflow has two identifying attributes:  name and targetId
        IdentityMatchingAttribute nameAttr = new IdentityMatchingAttribute(
                WORKFLOW_NAME, TEST_WORKFLOW_NAME);
        IdentityMatchingAttribute targetAttr = new IdentityMatchingAttribute(
                WORKFLOW_TARGET_ID, Long.toString(TEST_TARGET_ID));
        WorkflowAttributeExtractor extractorToTest = new WorkflowAttributeExtractor();
        // act
        IdentityMatchingAttributes attributesExtracted = extractorToTest.extractAttributes(testItem);
        // assert
        final Set<IdentityMatchingAttribute> matchingAttributes =
                attributesExtracted.getMatchingAttributes();
        assertThat(matchingAttributes.size(), equalTo(2));
        assertThat(matchingAttributes, containsInAnyOrder(nameAttr, targetAttr));
    }
}