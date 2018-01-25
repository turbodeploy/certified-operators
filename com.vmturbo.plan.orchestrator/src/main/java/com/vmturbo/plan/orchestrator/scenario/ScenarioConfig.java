package com.vmturbo.plan.orchestrator.scenario;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.PlanDTOREST.ScenarioServiceController;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, GlobalConfig.class})
public class ScenarioConfig {
    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Bean
    public ScenarioDao scenarioDao() {
        return new ScenarioDao(databaseConfig.dsl());
    }

    @Bean
    public ScenarioRpcService scenarioService() {
        return new ScenarioRpcService(scenarioDao(), globalConfig.identityInitializer());
    }

    @Bean
    public ScenarioServiceController scenarioServiceController() {
        return new ScenarioServiceController(scenarioService());
    }
}
