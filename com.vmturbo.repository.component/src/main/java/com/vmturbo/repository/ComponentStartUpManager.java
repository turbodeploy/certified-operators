package com.vmturbo.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;

/**
 * Class to handle tasks when the Repository component starts up.
 */
public class ComponentStartUpManager {
    private final Logger logger = LoggerFactory.getLogger(ComponentStartUpManager.class);

    /**
     * The maximal retry count for Database server startup
     */
    static final int MAX_CONNECT_RETRY_COUNT = 100;

    /**
     * The retry delay for Database server startup
     */
    static final int RETRY_DELAY_IN_MILLI_SEC = 10 * 1000;

    private final GraphDatabaseDriverBuilder graphDatabaseDriverBuilder;

    public ComponentStartUpManager(GraphDatabaseDriverBuilder graphDatabaseDriverBuilder) {
        this.graphDatabaseDriverBuilder = graphDatabaseDriverBuilder;
    }

    /**
     * Performs tasks needed to startup the component. The following tasks will be
     * performed when startup:
     *  <ul>
     *     <li> Waiting for the response from the database (ArangoDB) </li>
     * </ul>
     * @throws GraphDatabaseException
     */
    void startup() throws GraphDatabaseException {
        waitForServerUp(MAX_CONNECT_RETRY_COUNT, RETRY_DELAY_IN_MILLI_SEC);
    }
    /**
     * Checks the database server is up. Throw an exception on failure of connection with retries.
     * The number of retries is {@link #MAX_CONNECT_RETRY_COUNT} with a delay of
     * {@link #RETRY_DELAY_IN_MILLI_SEC} seconds applied before each retry.
     *
     * @param maxConnectRetryCount
     * @param retryDelayInMilliSec
     * @throws GraphDatabaseException when the server is down
     */
    void waitForServerUp(final int maxConnectRetryCount, final int retryDelayInMilliSec)
                    throws GraphDatabaseException {
        // Now, an empty string is used for database name as the isServerUp method checks
        // if the server is up by getting its version, which doesn't use the database string.
        final GraphDatabaseDriver graphDatabaseDriver = graphDatabaseDriverBuilder.build("");
        int count = 0;
        while (true) {
            try {
                logger.info("Waiting for DB server to response");
                graphDatabaseDriver.isServerUp();
                logger.info("Connected to DB server");
                break;
            } catch (GraphDatabaseException e) {
                if (count++ >= maxConnectRetryCount) {
                    logger.error("Cannot connecting to DB server with "
                                + maxConnectRetryCount + " retries");
                    throw e;
                }

                logger.error("Failed connecting to DB server: ", e);
                logger.info("Waiting for DB server to response. Retry #" + count);
                try {
                    Thread.sleep(retryDelayInMilliSec);
                }
                catch (InterruptedException e1) {
                    throw new GraphDatabaseException(
                                     "Exception while retrying connection to DB server", e1);
                }
            }
        }
    }
}
