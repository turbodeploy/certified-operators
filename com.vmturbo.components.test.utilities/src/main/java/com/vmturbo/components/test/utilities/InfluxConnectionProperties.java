package com.vmturbo.components.test.utilities;

import javax.annotation.Nonnull;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

/**
 * A central class that defines InfluxDB connection properties that can be overriden via
 * environment variables.
 */
public class InfluxConnectionProperties {

    private static final String URL_PROP = "INFLUX_DB_URL";

    private static final String USER_PROP = "INFLUX_DB_USER";

    private static final String PASSWORD_PROP = "INFLUX_DB_PASSWORD";

    private static final String DATABASE_PROP = "INFLUX_DB_DATABASE";

    private final EnvOverrideableProperties props = EnvOverrideableProperties.newBuilder()
            .addProperty(URL_PROP, "http://localhost:8086")
            .addProperty(USER_PROP, "root")
            .addProperty(PASSWORD_PROP, "root")
            .addProperty(DATABASE_PROP, "test")
            .build();

    @Nonnull
    public InfluxDB newInfluxConnection() {
        return InfluxDBFactory.connect(props.get(URL_PROP),
                props.get(USER_PROP), props.get(PASSWORD_PROP));
    }

    @Nonnull
    public String getDatabase() {
        return props.get(DATABASE_PROP);
    }
}
