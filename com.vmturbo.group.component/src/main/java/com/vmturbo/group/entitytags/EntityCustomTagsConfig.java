package com.vmturbo.group.entitytags;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.GroupComponentDBConfig;

/**
 * Configuration for EntityCustomTags RPC Service.
 */
@Configuration
@Import({GroupComponentDBConfig.class})
public class EntityCustomTagsConfig {

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    /**
     * Create a EntityCustomTagsStore from the db context.
     *
     * @return EntityCustomTagsStore based on the database context.
     */
    @Bean
    public EntityCustomTagsStore entityCustomTagsStore() {
        return new EntityCustomTagsStore(databaseConfig.dsl());
    }
}
