package com.vmturbo.api.component.external.api.dispatcher;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.api.validators.InputDTOValidator;

/**
 * Configuration for the validators used in the controllers
 * in the dispatcher servlet.
 *
 * The controllers here should match those in
 * com.vmturbo.api.validators and com.vmturbo.api.validators.
 */
@Configuration
public class DispatcherValidatorConfig {

    @Bean
    public InputDTOValidator inputDTOValidator() {
        return new InputDTOValidator();
    }
}
