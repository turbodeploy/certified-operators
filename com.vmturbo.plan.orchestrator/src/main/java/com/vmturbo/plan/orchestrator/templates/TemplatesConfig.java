package com.vmturbo.plan.orchestrator.templates;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOREST.DiscoveredTemplateDeploymentProfileServiceController;
import com.vmturbo.common.protobuf.plan.TemplateDTOREST.TemplateServiceController;
import com.vmturbo.common.protobuf.plan.TemplateDTOREST.TemplateSpecServiceController;
import com.vmturbo.plan.orchestrator.DbAccessConfig;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({DbAccessConfig.class, GlobalConfig.class, DeploymentProfileConfig.class})
public class TemplatesConfig {

    @Value("${templateSpecFile}")
    private String templateSpecFile;

    @Value("${defaultTemplatesFile}")
    private String defaultTemplatesFile;

    /**
     * The default is 60 after a cursory test. The optimal size for a single chunk in gRPC is
     * 16-64KB. A template with all fields initialized and a single field is around 256 bytes,
     * which we can take to be the lower bound on the template size. Multiplying the lower bound
     * by 60 gives around 16KB.
     */
    @Value("${getTemplatesChunkSize:60}")
    private int getTemplatesChunkSize;

    @Autowired
    private DbAccessConfig databaseConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private DeploymentProfileConfig deploymentProfileConfig;

    @Bean
    public TemplateSpecParser templateSpecParser() {
        return new TemplateSpecParser(templateSpecFile);
    }

    @Bean
    public TemplatesDao templatesDao() {
        try {
            return new TemplatesDaoImpl(databaseConfig.dsl(), defaultTemplatesFile,
                    globalConfig.identityInitializer());

        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create TemplatesDao", e);
        }
    }

    @Bean
    DiscoveredTemplateDeploymentProfileDaoImpl discoveredTemplateDeploymentProfileDao() {
        try {
            return new DiscoveredTemplateDeploymentProfileDaoImpl(databaseConfig.dsl());

        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create DiscoveredTemplateDeploymentProfileDao", e);
        }
    }

    @Bean
    public TemplatesRpcService templatesService() {
        return new TemplatesRpcService(templatesDao(),
                deploymentProfileConfig.deploymentProfileDao(),
                getTemplatesChunkSize);
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
