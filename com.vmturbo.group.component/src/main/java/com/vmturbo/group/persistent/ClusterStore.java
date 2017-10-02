package com.vmturbo.group.persistent;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.group.ArangoDriverFactory;
import com.vmturbo.group.GroupDBDefinition;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetClusterUpdate;

/**
 * The {@link ClusterStore} is responsible for the storage and retrieval of clusters.
 *
 * TODO (roman, Aug 2 2017) OM-21983: Clusters are currently stored in Arango. They should be
 * stored in MySQL.
 */
public class ClusterStore {
    private final Logger logger = LogManager.getLogger();

    private final static String ALL_CLUSTERS_QUERY = "FOR cluster IN @@cluster_collection RETURN cluster";

    private final static String CLUSTERS_BY_TARGET_QUERY = "FOR cluster in @@cluster_collection " +
            "FILTER cluster.targetId == @targetId " +
            "RETURN cluster";

    private final ArangoDriverFactory arangoDriverFactory;
    private final GroupDBDefinition groupDBDefinition;
    private final IdentityProvider identityProvider;

    public ClusterStore(@Nonnull final ArangoDriverFactory arangoDriverFactory,
                        @Nonnull final GroupDBDefinition groupDBDefinitionArg,
                        @Nonnull final IdentityProvider identityProvider) {
        this.arangoDriverFactory = Objects.requireNonNull(arangoDriverFactory);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.groupDBDefinition = Objects.requireNonNull(groupDBDefinitionArg);
    }

    /**
     * Retrieve a {@link Cluster} by it's ID.
     *
     * @param id The ID of the cluster to look for.
     * @return The {@link Cluster} associated with that ID.
     * @throws DatabaseException If there is an error interacting with the database.
     */
    @Nonnull
    public Optional<Cluster> get(final Long id) throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            return Optional.ofNullable(arangoDB.db(groupDBDefinition.databaseName())
                    .collection(groupDBDefinition.clusterCollection())
                    .getDocument(Long.toString(id), Cluster.class));
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to get cluster " + id, e);
        } finally {
            arangoDB.shutdown();
        }
    }

    /**
     * Update the set of clusters discovered by a particular target.
     * The new set of clusters will completely replace the old, even if the new set is empty.
     *
     * <p>See {@link TargetClusterUpdate} for details on the update behaviour.
     *
     * @param targetId The ID of the target that discovered the clusters.
     * @param newClusters The new set of discovered {@link ClusterInfo}s.
     * @return a mapping of cluster key (name/SE type) to cluster OID
     * @throws DatabaseException If there is an error interacting with the database.
     */
    public Map<String, Long>  updateTargetClusters(final long targetId,
                                     @Nonnull final List<ClusterInfo> newClusters)
            throws DatabaseException {
        logger.info("Updating clusters discovered by {}. Got {} new clusters",
                targetId, newClusters.size());
        final TargetClusterUpdate update = new TargetClusterUpdate(targetId, identityProvider, newClusters,
                getDiscoveredByTarget(targetId));
        // (roman, August 1 2017): These should happen in a single transaction, but
        // since we are migrating from ArangoDB to MySQL I'm not taking the time to figure
        // out how to do transactional updates in ArangoDB.
        Map<String, Long> map = update.apply(this::store, this::delete);
        logger.info("Finished updating clusters discovered by {}.", targetId);
        return map;
    }

    /**
     * Get all clusters in the store.
     *
     * @return A collection of {@link Cluster}s for every cluster the store knows about.
     *         Each cluster will only appear once.
     * @throws DatabaseException If there is an error interacting with the database.
     */
    @Nonnull
    public Collection<Cluster> getAll() throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final Map<String, Object> bindVars = ImmutableMap.of(
                    "@cluster_collection", groupDBDefinition.clusterCollection());

            final ArangoCursor<Cluster> cursor = arangoDB
                    .db(groupDBDefinition.databaseName())
                    .query(ALL_CLUSTERS_QUERY, bindVars, null, Cluster.class);

            return cursor.asListRemaining();
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to retrieve clusters.", e);
        } finally {
            arangoDB.shutdown();
        }
    }

    @Nonnull
    private Collection<Cluster> getDiscoveredByTarget(final long targetId)
            throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final Map<String, Object> bindVars = ImmutableMap.of(
                    "@cluster_collection", groupDBDefinition.clusterCollection(),
                    "targetId", targetId);

            final ArangoCursor<Cluster> cursor = arangoDB
                    .db(groupDBDefinition.databaseName())
                    .query(CLUSTERS_BY_TARGET_QUERY, bindVars, null, Cluster.class);

            return cursor.asListRemaining();
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to get clusters discovered by target " + targetId,
                    e);
        } finally {
            arangoDB.shutdown();
        }
    }

    private void store(@Nonnull final Cluster cluster) throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            final ArangoCollection clusterCollection = arangoDB.db(groupDBDefinition.databaseName())
                    .collection(groupDBDefinition.clusterCollection());
            final String idStr = Long.toString(cluster.getId());

            if (clusterCollection.documentExists(idStr)) {
                clusterCollection.replaceDocument(idStr, cluster);
            } else {
                clusterCollection.insertDocument(cluster);
            }
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to save cluster " + cluster, e);
        } finally {
            arangoDB.shutdown();
        }
    }

    private void delete(Long id) throws DatabaseException {
        ArangoDB arangoDB = arangoDriverFactory.getDriver();

        try {
            arangoDB.db(groupDBDefinition.databaseName())
                    .collection(groupDBDefinition.clusterCollection())
                    .deleteDocument(Long.toString(id));
        } catch (ArangoDBException e) {
            throw new DatabaseException("Failed to delete cluster " + id, e);
        } finally {
            arangoDB.shutdown();
        }
    }
}
