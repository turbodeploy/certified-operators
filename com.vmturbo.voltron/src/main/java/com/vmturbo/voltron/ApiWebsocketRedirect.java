package com.vmturbo.voltron;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Attempts to redirect views to the right places to help the UI/external API work.
 */
@Configuration
@Import({ApiSecurityConfig.class})
public class ApiWebsocketRedirect extends WebMvcConfigurerAdapter {
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/", "/api_component/app/index.html");
        registry.addRedirectViewController("/vmturbo/apidoc", "/api_component/vmturbo/apidoc/index.html");
        registry.addRedirectViewController("/swagger", "/api_component/swagger/index.html");
    }
}
