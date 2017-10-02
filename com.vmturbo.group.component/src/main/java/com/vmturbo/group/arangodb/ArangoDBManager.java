package com.vmturbo.group.arangodb;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.ArangoDBVersion;
import com.arangodb.entity.CollectionEntity;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.vmturbo.group.ArangoDriverFactory;
import com.vmturbo.group.GroupDBDefinition;

/**
 * A manager to make ArangoDB is up and running when the group component is starting up.
 * Moreover, the manager will make sure the required database and collections by Group
 * component are present.
 */
public class ArangoDBManager {
    private final static Logger LOG = LoggerFactory.getLogger(ArangoDBManager.class);

    private final ArangoDriverFactory arangoDriverFactory;
    private final GroupDBDefinition groupDBDefinition;

    public ArangoDBManager(final ArangoDriverFactory arangoDriverFactory,
                           final GroupDBDefinition groupDBDefinitionArg) {
        this.arangoDriverFactory = Objects.requireNonNull(arangoDriverFactory);
        this.groupDBDefinition = Objects.requireNonNull(groupDBDefinitionArg);
    }

    /**
     * Wait forever until ArangoDB is up and running,
     * then check if the required database and collections are present.
     *
     * @throws ExecutionException Problem when trying to contact ArangoDB.
     * @throws RetryException Problem when the retry mechanism has failed.
     */
    @PostConstruct
    private void init() throws ExecutionException, RetryException {
        final Retryer<ArangoDBVersion> isArangoDBUpForever = RetryerBuilder.<ArangoDBVersion>newBuilder()
                .retryIfExceptionOfType(ArangoDBException.class)
                .withWaitStrategy(WaitStrategies.fixedWait(10, TimeUnit.SECONDS))
                .withStopStrategy(StopStrategies.neverStop())
                .build();

        // get the arangoDB driver
        final ArangoDB arangoDB = arangoDriverFactory.getDriver();

        // Wait forever until ArangoDB is up.
        isArangoDBUpForever.call(() -> {
            LOG.info("Making sure ArangoDB is up");
            return arangoDB.getVersion();
        });

        try {
            // Initialize database if it does not exist
            if (!arangoDB.getDatabases().contains(groupDBDefinition.databaseName())) {
                LOG.info("Creating database {}", groupDBDefinition.databaseName());
                arangoDB.createDatabase(groupDBDefinition.databaseName());
            }

            // Create the collections if they do not exist
            final ArangoDatabase database = arangoDB.db(groupDBDefinition.databaseName());

            if (database != null) {
                final List<String> existingCollections = database.getCollections().stream()
                                .map(CollectionEntity::getName)
                                .collect(Collectors.toList());
                if (!existingCollections.contains(groupDBDefinition.groupCollection())) {
                    LOG.info("Creating collection {}", groupDBDefinition.groupCollection());
                    database.createCollection(groupDBDefinition.groupCollection());
                }

                if (!existingCollections.contains(groupDBDefinition.policyCollection())) {
                    LOG.info("Creating collection {}", groupDBDefinition.policyCollection());
                    database.createCollection(groupDBDefinition.policyCollection());
                }

                if (!existingCollections.contains(groupDBDefinition.clusterCollection())) {
                    LOG.info("Creating collection {}", groupDBDefinition.clusterCollection());
                    database.createCollection(groupDBDefinition.clusterCollection());
                }
            }
        } finally {
            arangoDB.shutdown();
        }
    }
}
