package com.vmturbo.group;

import com.arangodb.ArangoDB;

@FunctionalInterface
public interface ArangoDriverFactory {

    /**
     * Returns a new instance/connection of the ArangoDB driver at every call.
     * This is in order to try to prevent the connection to got stuck.
     * <p>
     * It's user's responsibility to shutdown this instance when done.
     *
     * @return the ArangoDB driver instance
     */
    ArangoDB getDriver();
}
