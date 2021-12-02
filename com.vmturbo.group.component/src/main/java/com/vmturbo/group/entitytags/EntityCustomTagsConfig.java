package com.vmturbo.group.entitytags;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration for EntityCustomTags RPC Service.
 */
@Configuration
@Import({DbAccessConfig.class})
public class EntityCustomTagsConfig {

    @Autowired
    private DbAccessConfig databaseConfig;

    /**
     * Create a EntityCustomTagsStore from the db context.
     *
     * @return EntityCustomTagsStore based on the database context.
     */
    @Bean
    public EntityCustomTagsStore entityCustomTagsStore() {
        try {
            return new EntityCustomTagsStore(databaseConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create EntityCustomTagsStore", e);
        }
    }
}
