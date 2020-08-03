package com.vmturbo.ml.datastore;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.influxdb.InfluxHealthMonitor;
import com.vmturbo.ml.datastore.influx.InfluxConfig;
import com.vmturbo.ml.datastore.rpc.MLDatastoreRpcConfig;
import com.vmturbo.ml.datastore.topology.ActionsListenerConfig;
import com.vmturbo.ml.datastore.topology.TopologyListenerConfig;

/**
 * The ML datastore component receives information broadcast from various components in
 * the system containing metrics about the state of the customer's environment.
 *
 * <p>It writes this data to influxdb for persistent storage for use in training ML algorithms.
 */
@Configuration("theComponent")
@Import({
    MLDatastoreConfig.class,
    TopologyListenerConfig.class,
    ActionsListenerConfig.class,
    MLDatastoreRpcConfig.class,
    InfluxConfig.class
})
public class MLDatastoreComponent extends BaseVmtComponent {

    @Value("${influxHealthCheckIntervalSeconds:60}")
    private double influxHealthCheckIntervalSeconds;

    @Autowired
    private MLDatastoreRpcConfig rpcConfig;

    @Autowired
    private TopologyListenerConfig listenerConfig;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        // Check if Metron is enabled. If not, immediately exit.
        if (Boolean.parseBoolean(System.getenv("METRON_ENABLED"))) {
            logger.info("Metron is enabled. Starting Metron component {}.",
                MLDatastoreComponent.class.getSimpleName());
            startContext(MLDatastoreComponent.class);
        } else {
            logger.info("Metron not enabled. Component {} exiting.",
                MLDatastoreComponent.class.getSimpleName());
            // Exit with a success code so that the service is not automatically restarted.
            System.exit(0);
        }
    }

    @PostConstruct
    private void setup() {
        logger.info("Adding InfluxDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck(
            new InfluxHealthMonitor(influxHealthCheckIntervalSeconds,
                () -> listenerConfig.topologyEntitiesListener()
                    .getInfluxConnection()));
    }


    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Collections.singletonList(rpcConfig.mlDatastoreRpcService());
    }
}
