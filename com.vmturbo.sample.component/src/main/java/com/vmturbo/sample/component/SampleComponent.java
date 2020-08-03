package com.vmturbo.sample.component;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import io.grpc.BindableService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.json.GsonHttpMessageConverter;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.sample.component.echo.EchoRpcConfig;
import com.vmturbo.sample.component.notifications.SampleComponentNotificationSender;
import com.vmturbo.sample.component.notifications.SampleComponentNotificationsConfig;

/**
 * This component is for illustration purposes, to document how a basic component works, how
 * to create a new one, and what the best practices are when developing a component.
 * See the associated wiki page:
 * https://vmturbo.atlassian.net/wiki/display/Home/Creating+An+XL+Component
 *
 * <p>The {@link SampleComponent} class is the main configuration for this component.
 * Most of the actual functionality should live in sub-packages (see {@link EchoRpcConfig} and
 * {@link SampleComponentNotificationSender}) but this is the class that ties everything together,
 * and defines global beans that cross package borders.
 *
 * <p>The name "theComponent" is required for autowiring to work properly.
 */

@Configuration("theComponent")
@Import({EchoRpcConfig.class, SampleComponentNotificationsConfig.class})
public class SampleComponent extends BaseVmtComponent {

    @Autowired
    private EchoRpcConfig echoRpcConfig;

    @Value("${spring.application.name}")
    private String componentName;

    @Override
    public String getComponentName() {
        return componentName;
    }

    /**
     * This is the method that's called to initialize the component.
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {
        startContext(SampleComponent.class);
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Collections.singletonList(echoRpcConfig.echoRpcService());
    }

    /**
     * This bean creates a custom GSON HTTP converter configured to support swagger.
     * Swagger is what we use to document our rest controllers.
     *
     * <p>(see: http://stackoverflow.com/questions/30219946/springfoxswagger2-does-not-work-with-gsonhttpmessageconverterconfig/30220562#30220562)
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
