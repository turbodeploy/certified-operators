package com.vmturbo.topology.processor.controllable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import(SQLDatabaseConfig.class)
public class ControllableConfig {
    @Value("${controllableInProgressRecordExpiredSeconds}")
    private int controllableInProgressExpiredSeconds;

    @Value("${controllableSucceedRecordExpiredSeconds}")
    private int controllableSucceedRecordExpiredSeconds;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Bean
    public EntityActionDaoImp entityActionDaoImp() {
        return new EntityActionDaoImp(databaseConfig.dsl(), controllableSucceedRecordExpiredSeconds,
                controllableInProgressExpiredSeconds);
    }

    @Bean
    public ControllableManager controllableManager() {
        return new ControllableManager(entityActionDaoImp());
    }
}
