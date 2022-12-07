package com.vmturbo.cost.component;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.scope.SqlOidMappingStore;
import com.vmturbo.cost.component.scope.UploadAliasedOidsRpcService;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Config for Scope id replacement log and related beans.
 */
@Configuration
@Import(DbAccessConfig.class)
public class ScopeIdReplacementLogConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    /**
     * Returns an instance of {@link UploadAliasedOidsRpcService}.
     *
     * @return an instance of {@link UploadAliasedOidsRpcService}.
     */
    @Bean
    public UploadAliasedOidsRpcService uploadAliasedOidsRpcService() {
        return new UploadAliasedOidsRpcService(sqlOidMappingStore());
    }

    /**
     * Returns an instance of {@link SqlOidMappingStore}.
     *
     * @return an instance of {@link SqlOidMappingStore}.
     */
    @Bean
    public SqlOidMappingStore sqlOidMappingStore() {
        try {
            return new SqlOidMappingStore(dbAccessConfig.dsl());
        } catch (SQLException | DbEndpoint.UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create SqlOidMappingStore.", e);
        }
    }
}