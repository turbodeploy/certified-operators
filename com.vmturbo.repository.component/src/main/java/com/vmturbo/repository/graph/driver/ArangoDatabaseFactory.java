package com.vmturbo.repository.graph.driver;

import com.arangodb.ArangoDB;

@FunctionalInterface
public interface ArangoDatabaseFactory {
    ArangoDB getArangoDriver();
}
