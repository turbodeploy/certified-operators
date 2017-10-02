package com.vmturbo.components.test.utilities.alert.jira;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.IssueType;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraComponent.Type;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraPriority;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraProject;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraTransition;

public class JiraIssueTest {
    @Test
    public void testTransition() {
        final JiraTransition transition = new JiraTransition();
        transition.setId("foo");
        transition.setName("bar");

        assertEquals("foo", transition.getId());
        assertEquals("bar", transition.getName());
    }

    @Test
    public void testBuilder() {
        final JiraIssue issue = JiraIssue.newBuilder()
            .assignee("tester.mctesterson")
            .components(Type.XL)
            .description("issue description")
            .fixVersion("1.1")
            .labels("label1", "label2")
            .priority(JiraPriority.Type.P2)
            .project(JiraProject.Type.OpsManager)
            .summary("issue summary")
            .type(IssueType.Type.Story)
            .build();

        assertEquals("tester.mctesterson", issue.getFields().getAssignee().getName());
        assertEquals("XL", issue.getFields().getComponents().get(0).getName());
        assertEquals("issue description", issue.getFields().getDescription());
        assertEquals("1.1", issue.getFields().getFixVersions().get(0).getName());
        assertThat(issue.getFields().getLabels(), containsInAnyOrder("label1", "label2"));
        assertEquals("P2", issue.getFields().getPriority().getName());
        assertEquals("Operations Manager", issue.getFields().getProject().getName());
        assertEquals("OM", issue.getFields().getProject().getKey());
        assertEquals("issue summary", issue.getFields().getSummary());
        assertEquals("Story", issue.getFields().getIssuetype().getName());
    }

    @Test
    public void testXlBuilder() {
        final JiraIssue issue = JiraIssue.xlBugBuilder()
            .build();

        assertEquals("XL", issue.getFields().getComponents().get(0).getName());
        assertEquals("P2", issue.getFields().getPriority().getName());
        assertEquals("Operations Manager", issue.getFields().getProject().getName());
        assertEquals("OM", issue.getFields().getProject().getKey());
        assertEquals("Bug", issue.getFields().getIssuetype().getName());
    }
}