package com.vmturbo.cost.component.entity.cost;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class})
public class EntityCostConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Value("${persistEntityCostChunkSize}")
    private int persistEntityCostChunkSize;

    @Bean
    public EntityCostStore entityCostStore() {
        return new SQLEntityCostStore(databaseConfig.dsl(), Clock.systemUTC(), persistEntityCostChunkSize);
    }
}
