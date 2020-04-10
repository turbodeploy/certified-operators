package com.vmturbo.integrations.intersight;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;

/**
 * The Intersight integration component performs tasks to integrate with Intersight by running
 * alongside with Intersight in the SaaS.
 */
@Configuration("theComponent")
@Import({
    IntersightConfig.class,
})
public class IntersightIntegrationComponent extends BaseVmtComponent {

    @Autowired
    private IntersightConfig intersightConfig;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(IntersightIntegrationComponent.class);
    }

    @PostConstruct
    private void setup() {
        logger.info("Adding Intersight health check to the component health monitor.");
        getHealthMonitor().addHealthCheck(intersightConfig.getIntersightMonitor());

        logger.info("Scheduling tasks to integrate with Intersight.");
        intersightConfig.scheduleTasks();
    }
}
