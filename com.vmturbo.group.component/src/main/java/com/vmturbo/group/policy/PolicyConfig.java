package com.vmturbo.group.policy;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.DbAccessConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({DbAccessConfig.class,
    IdentityProviderConfig.class,
    GroupConfig.class})
public class PolicyConfig {

    @Autowired
    private DbAccessConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Bean
    public PolicyStore policyStore() {
        try {
            return new PolicyStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider(),
                policyValidator());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PolicyStore", e);
        }
    }

    @Bean
    public PolicyValidator policyValidator() {
        return new PolicyValidator(groupConfig.groupStore());
    }

}
