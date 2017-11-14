package com.vmturbo.topology.processor.template;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.plan.PlanConfig;

@Configuration
@Import({IdentityProviderConfig.class, PlanConfig.class})
public class TemplateConfig {

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private PlanConfig planConfig;

    @Bean
    public TemplateConverterFactory templateConverterFactory() {
        return new TemplateConverterFactory(planConfig.templateServiceBlockingStub(),
                identityProviderConfig.identityProvider());
    }
}
