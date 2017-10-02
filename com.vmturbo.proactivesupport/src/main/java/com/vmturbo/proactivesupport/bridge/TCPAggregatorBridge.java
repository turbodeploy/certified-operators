package com.vmturbo.proactivesupport.bridge;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;

import com.vmturbo.proactivesupport.AggregatorBridge;
import com.vmturbo.proactivesupport.DataMetric;
import com.vmturbo.proactivesupport.DataMetricLOB;
import com.vmturbo.proactivesupport.communications.ClientChannel;

/**
 * The TCPAggregatorBridge implements the TCP/IP aggregator bridge.
 * The TCPAggregatorBridge will forward the binary data to the Data Aggregator using TCP/IP based
 * protocol.
 * The client must create an instance of the bridge, and it is not shareable.
 */
@NotThreadSafe
public class TCPAggregatorBridge implements AggregatorBridge {
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(TCPAggregatorBridge.class);

    /**
     * The JSON builder.
     */
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setLongSerializationPolicy(LongSerializationPolicy.STRING)
            .setPrettyPrinting()
            .create();

    /**
     * The serialization character set.
     */
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * The client channel host.
     */
    private final String host_;

    /**
     * The client channel port.
     */
    private final int port_;

    /**
     * Constructs the bridge.
     */
    public TCPAggregatorBridge(final @Nonnull String host, final int port) {
        host_ = Objects.requireNonNull(host);
        port_ = validatePort(port);
    }

    /**
     * Validates the port to be between {@code 1024} and {code 65536} (non-inclusive).
     *
     * @param port The port to be validated.
     * @return The port unchanged.
     * @throws IllegalArgumentException In case of an invalid port.
     */
    private static int validatePort(final int port) throws IllegalArgumentException {
        if (port <= 1024 || port >= 65536) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        return port;
    }

    /**
     * Sends the urgent collection of messages over to the Data Aggregator.
     *
     * @param messages The collection of messages.
     */
    @Override
    public void sendUrgent(@Nonnull Collection<DataMetric> messages) {
        messages.forEach(m -> {
            try (ClientChannel channel = new ClientChannel(host_, port_)) {
                final String json = GSON.toJson(m, m.getClass());
                channel.send(m.getName(), new ByteArrayInputStream(json.getBytes(CHARSET)));
            } catch (IOException e) {
                logger_.error("Error sending the data", e);
            } catch (Exception e) {
                logger_.error("Error sending an urgent message or closing a channel", e);
            }
        });
    }

    /**
     * Sends the offline collection of messages over to the Data Aggregator.
     *
     * @param messages The collection of messages.
     */
    @Override
    public void sendOffline(@Nonnull Collection<DataMetric> messages) {
        messages.stream()
                .filter(m -> m instanceof DataMetricLOB)
                .forEach(m -> {
                    try (ClientChannel channel = new ClientChannel(host_, port_);
                         InputStream in = ((DataMetricLOB)m).getData()) {
                        channel.send(m.getName(), in);
                    } catch (IOException e) {
                        logger_.error("Error sending the data", e);
                    } catch (Exception e) {
                        logger_.error("Error sending an offline message or closing a channel", e);
                    }
                });
    }
}
