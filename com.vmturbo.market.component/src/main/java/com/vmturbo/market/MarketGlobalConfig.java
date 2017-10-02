package com.vmturbo.market;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Global configuration for beans that do not belong in a specific
 * package.
 */
@Configuration
public class MarketGlobalConfig {

    /**
     * This bean performs registration of all configured websocket endpoints.
     *
     * @return bean
     */
    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }

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
}
