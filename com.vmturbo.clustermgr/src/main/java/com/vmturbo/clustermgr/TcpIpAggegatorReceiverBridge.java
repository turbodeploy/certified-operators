package com.vmturbo.clustermgr;

import java.io.IOException;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.clustermgr.aggregator.DataAggregator;
import com.vmturbo.proactivesupport.communications.MessageServer;

/**
 * The TcpIpAggegatorReceiverBridge implements the TCP/IP aggregator bridge (the receiving part).
 */
public class TcpIpAggegatorReceiverBridge {
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(TcpIpAggegatorReceiverBridge.class);

    /**
     * The message server.
     */
    private final MessageServer server_;

    /**
     * The message server thread.
     */
    private final Thread thread_;

    /**
     * Constructs the bridge.
     *
     * @param port           The listening port.
     * @param dataAggregator The data aggregator.
     * @throws IOException In case of an error constructing the server.
     */
    public TcpIpAggegatorReceiverBridge(final int port,
                                        final @Nonnull DataAggregator dataAggregator)
            throws IOException {
        server_ = new MessageServer(port, dataAggregator.listenerInstance());
        thread_ = new Thread(server_, "TCP/IP Data Aggregator Server");
        thread_.start();
    }
}
