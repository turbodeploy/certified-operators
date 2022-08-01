package com.vmturbo.voltron.extensions.tp;

import java.io.File;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Cache Only Discovery Dumper.
 */
@Configuration
public class CacheOnlyDiscoveryDumperConfig {

    /**
     * The path to cached discovery responses.
     */
    @Value("${discoveryResponsesCachePath:/home/turbonomic/data/cached_responses}")
    private String discoveryResponsesCachePath;

    /**
     * Returns the CacheOnlyDiscoveryDumper that writes and loads cached discovery responses in
     * a text form.
     *
     * @return the cache only discovery dumper.
     */
    @Bean
    public CacheOnlyDiscoveryDumper cacheOnlyDiscoveryDumper() {
        return new CacheOnlyDiscoveryDumper(new File(discoveryResponsesCachePath));
    }
}
