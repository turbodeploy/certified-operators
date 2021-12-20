package com.vmturbo.cost.component.discount;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({DbAccessConfig.class, IdentityProviderConfig.class})
public class DiscountConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public DiscountStore discountStore() {
        try {
            return new SQLDiscountStore(dbAccessConfig.dsl(), identityProviderConfig.identityProvider());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create DiscountStore bean", e);
        }
    }
}
