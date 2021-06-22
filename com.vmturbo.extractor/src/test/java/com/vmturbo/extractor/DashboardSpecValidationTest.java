package com.vmturbo.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.extractor.grafana.DashboardsOnDisk;

/**
 * This test is meant to verify that we never try to commit dashboards that won't load when
 * we actually start up the extractor component.
 */
public class DashboardSpecValidationTest {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Test that all dashboards are valid.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDashboardsValid() throws Exception {
        final DashboardsOnDisk dashboards = new DashboardsOnDisk(
            ResourcePath.getTestResource(DashboardsOnDisk.class, "dashboards").toString());
        // The "visit" method performs validation on the dashboards, so we make sure that
        // it doesn't crash.
        dashboards.visit(folderData -> {
            logger.info(folderData.getFileName());
        });

    }
}
