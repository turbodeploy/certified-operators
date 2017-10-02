package com.vmturbo.sample.component.echo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.sample.EchoREST.EchoServiceController;
import com.vmturbo.sample.component.notifications.SampleComponentNotificationsConfig;

/**
 * This is the configuration for the package that implements the
 * services defined in com.vmturbo.common.protobuf/src/main/protobuf/sample/Echo.proto.
 */
@Configuration
// According to the Turbonomic Spring Coding Standards, we manage inter-configuration dependencies
// by explicitly importing the dependency class, and autowiring the specific instance.
@Import({SampleComponentNotificationsConfig.class})
public class EchoRpcConfig {

    /**
     * Spring will inject this value from Consul during context initialization at startup.
     * The defaults are listed in
     * com.vmturbo.clustermgr/src/main/resources/factoryInstalledComponents.yml.
     *
     * IMPORTANT: Use @Value annotations only in configuration classes, and pass them to
     *  the classes that need them via constructor properties.
     */
    @Value("${someIntProperty}")
    private int someIntProperty;

    /**
     * This is the instance of {@link SampleComponentNotificationsConfig} that exists in the context.
     */
    @Autowired
    private SampleComponentNotificationsConfig notificationsConfig;

    /**
     * This is the actual implementation of EchoService.
     */
    @Bean
    public EchoRpcService echoRpcService() {
        return new EchoRpcService(
                notificationsConfig.sampleComponentNotificationSender(),
                someIntProperty);
    }

    /**
     * Every bean of a gRPC service implementation MUST have an associated controller bean!
     *
     * This is the REST controller that provides a swagger-annotated REST interface (for
     * debugging and development purposes) to the gRPC service. gRPC doesn't have an easy
     * way to trigger services over HTTP (via tools like cURL, or swagger-UI), so we wrote
     * a protobuf compiler plugin to auto-generate Spring REST controllers.
     *
     * See: the protoc-spring-rest module (XL/protoc-spring-rest) for the implementation of
     * the REST controller generation.
     */
    @Bean
    public EchoServiceController echoServiceController() {
        return new EchoServiceController(echoRpcService());
    }
}
