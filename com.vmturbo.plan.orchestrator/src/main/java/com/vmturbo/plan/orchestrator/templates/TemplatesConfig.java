package com.vmturbo.plan.orchestrator.templates;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOREST.DiscoveredTemplateDeploymentProfileServiceController;
import com.vmturbo.common.protobuf.plan.TemplateDTOREST.TemplateServiceController;
import com.vmturbo.common.protobuf.plan.TemplateDTOREST.TemplateSpecServiceController;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import(SQLDatabaseConfig.class)
public class TemplatesConfig {
    @Value("${templateSpecFile}")
    private String templateSpecFile;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Bean
    public TemplateSpecParser templateSpecParser() {
        return new TemplateSpecParser(templateSpecFile);
    }

    @Bean
    public TemplatesDao templatesDao() {
        return new TemplatesDaoImpl(databaseConfig.dsl());
    }

    @Bean
    DiscoveredTemplateDeploymentProfileDaoImpl discoveredTemplateDeploymentProfileDao() {
        return new DiscoveredTemplateDeploymentProfileDaoImpl(databaseConfig.dsl());
    }

    @Bean
    public TemplatesRpcService templatesService() {
        return new TemplatesRpcService(templatesDao());
    }

    @Bean
    public TemplateSpecRpcService templateSpecService() {
        return new TemplateSpecRpcService(templateSpecParser());
    }

    @Bean
    public DiscoveredTemplateDeploymentProfileRpcService discoveredTemplateDeploymentProfileService() {
        return new DiscoveredTemplateDeploymentProfileRpcService(templateSpecParser(),
            discoveredTemplateDeploymentProfileDao());
    }

    @Bean
    public TemplateServiceController templateServiceController() {
        return new TemplateServiceController(templatesService());
    }

    @Bean
    public TemplateSpecServiceController templateSpecServiceController() {
        return new TemplateSpecServiceController(templateSpecService());
    }

    @Bean
    public DiscoveredTemplateDeploymentProfileServiceController discoveredTemplateDeploymentProfileServiceController() {
        return new DiscoveredTemplateDeploymentProfileServiceController(discoveredTemplateDeploymentProfileService());
    }
}
