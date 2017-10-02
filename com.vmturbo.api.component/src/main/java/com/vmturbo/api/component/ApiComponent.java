package com.vmturbo.api.component;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.controller.DBAdminController;
import com.vmturbo.api.component.external.api.ExternalApiConfig;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.component.external.api.swagger.SwaggerConfig;
import com.vmturbo.components.common.BaseVmtComponent;

/**
 * This is the "main()" for the API Component. The API component implements
 * all external REST API calls. Some calls are simply forwarded to the correct component. Other
 * calls make one or more calls to other components
 * and then assemble the response - e.g. filtering, joining related information, etc.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@Import({ApiComponentGlobalConfig.class,
         ServiceConfig.class,
         ExternalApiConfig.class,
         SwaggerConfig.class,
         DBAdminController.class})
public class ApiComponent extends BaseVmtComponent {
    private final static Logger LOGGER = LogManager.getLogger();

    @Value("${spring.application.name}")
    private String componentName;

    @Autowired
    private SwaggerConfig swaggerConfig;

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(ApiComponent.class)
                .run(args);
    }

    @Override
    public String getComponentName() {
        return componentName;
    }
}
