package com.vmturbo.api.component.external.api.dispatcher;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.api.validators.InputDTOValidator;

/**
 * Configuration for the validators used in the controllers
 * in the dispatcher servlet.
 *
 * The validators here should match those in
 * com.vmturbo.api.validators and com.vmturbo.api.validators.
 */
@Configuration
// DO NOT import configurations outside the external.api.dispatcher package here, because
// that will re-create the configuration's beans in the child context for the dispatcher servlet.
// You will end up with multiple instances of the same beans, which could lead to tricky bugs.
public class DispatcherValidatorConfig {

    @Bean
    public InputDTOValidator inputDTOValidator() {
        return new InputDTOValidator();
    }
}
