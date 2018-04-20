package com.vmturbo.components.test.utilities.alert.jira;

import org.junit.Ignore;
import org.junit.Test;

public class JiraAccessTest {

    @Test
    @Ignore("This is for manual tests only")
    public void test() {
        final JiraCommunicator jira = new JiraCommunicator();
        jira.getIssue("OM-33333");
    }
}
