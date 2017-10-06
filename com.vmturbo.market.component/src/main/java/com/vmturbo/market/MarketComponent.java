package com.vmturbo.market;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.BaseVmtComponent;

/**
 * Component wrapping the Market implementation.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan("com.vmturbo.market")
public class MarketComponent extends BaseVmtComponent {

    private Logger log = LogManager.getLogger();

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
