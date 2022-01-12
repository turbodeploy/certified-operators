package com.vmturbo.topology.processor.discoverydumper;

import java.io.File;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Binary Discovery Dumper.
 */
@Configuration
public class BinaryDiscoveryDumperConfig {


    /**
     * The path to cached discovery responses.
     */
    @Value("${discoveryResponsesCachePath:/home/turbonomic/data/cached_responses}")
    private String discoveryResponsesCachePath;

    /**
     * Returns the BinaryDiscoveryDumper that writes and loads cached discovery responses.
     *
     * @return the binaryDiscoveryDumper.
     */
    @Bean
    public BinaryDiscoveryDumper binaryDiscoveryDumper() {
        return new BinaryDiscoveryDumper(new File(discoveryResponsesCachePath));
    }
}
