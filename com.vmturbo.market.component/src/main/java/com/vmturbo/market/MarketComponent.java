package com.vmturbo.market;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.market.api.MarketApiConfig;
import com.vmturbo.market.topology.TopologyProcessorConfig;

/**
 * Component wrapping the Market implementation.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@Import({
    MarketGlobalConfig.class,
    TopologyProcessorConfig.class,
    MarketApiConfig.class
})
public class MarketComponent extends BaseVmtComponent {

    @Autowired
    private MarketApiConfig marketApiConfig;

    @Value("${spring.application.name}")
    private String componentName;

    public static void main(String[] args) {
        // apply the configuration properties for this component prior to Spring instantiation
        fetchConfigurationProperties();
        // instantiate and run this component
        new SpringApplicationBuilder()
                .sources(MarketComponent.class)
                .run(args);
    }

    @PostConstruct
    private void setup() {
        // add kafka producer health check
        getHealthMonitor().addHealthCheck(marketApiConfig.kafkaHealthMonitor());
    }

    @Override
    public String getComponentName() {
        return componentName;
    }
}
