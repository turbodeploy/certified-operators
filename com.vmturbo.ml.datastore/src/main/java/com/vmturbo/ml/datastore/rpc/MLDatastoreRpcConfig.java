package com.vmturbo.ml.datastore.rpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.ml.datastore.MLDatastoreREST.MLDatastoreServiceController;
import com.vmturbo.ml.datastore.influx.InfluxConfig;

/**
 * Configuration class for the RPC services for the ML datastore component.
 */
@Configuration
@Import({
    InfluxConfig.class
})
public class MLDatastoreRpcConfig {

    @Autowired
    private InfluxConfig influxConfig;

    @Bean
    public MLDatastoreRpcService mlDatastoreRpcService() {
        return new MLDatastoreRpcService(influxConfig.metricsStoreWhitelist());
    }

    @Bean
    public MLDatastoreServiceController mlDatastoreServiceController() {
        return new MLDatastoreServiceController(mlDatastoreRpcService());
    }
}
