package com.vmturbo.plan.orchestrator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Top-level configuration for common things (e.g. Spring config, Swagger config)
 * that do not belong in any specific package.
 */

@Configuration
@EnableSwagger2
public class GlobalConfig {
    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    /**
     * GSON HTTP converter configured to support swagger.
     * (see: http://stackoverflow.com/questions/30219946/springfoxswagger2-does-not-work-with-gsonhttpmessageconverterconfig/30220562#30220562)
     *
     * @return The {@link GsonHttpMessageConverter}.
     */
    @Bean
    public GsonHttpMessageConverter gsonHttpMessageConverter() {
        final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        return msgConverter;
    }

    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }
}
