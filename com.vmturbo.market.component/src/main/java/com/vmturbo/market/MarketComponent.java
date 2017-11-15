package com.vmturbo.market;

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
@Import({MarketGlobalConfig.class, TopologyProcessorConfig.class, MarketApiConfig.class})
public class MarketComponent extends BaseVmtComponent {

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(MarketComponent.class)
                .run(args);
    }

    @Value("${spring.application.name}")
    private String componentName;

    @Override
    public String getComponentName() {
        return componentName;
    }

}
