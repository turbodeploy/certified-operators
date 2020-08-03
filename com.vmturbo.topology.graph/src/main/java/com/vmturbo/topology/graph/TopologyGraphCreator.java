package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.topology.graph.TopologyGraphEntity.Builder;

/**
 * A utility class to create a properly structured {@link TopologyGraph}. The user of the class
 * is responsible for implementing a {@link TopologyGraphEntity} and a
 * {@link TopologyGraphEntity.Builder}.
 *
 * The intended use:
 * 1) Create a {@link TopologyGraphCreator}.
 * 2) (optional) Add {@link TopologyGraphEntity.Builder}s using
 *    {@link TopologyGraphCreator#addEntities(Collection)} or
 *    {@link TopologyGraphCreator#addEntity(Builder)}.
 * 3) Call {@link TopologyGraphCreator#build}. The creator will link all the underlying entities
 *    to each other, and return the final {@link TopologyGraph}.
 *
 * @param <BUILDER> The {@link TopologyGraphEntity.Builder} implementation for the particular
 *                 {@link TopologyGraphEntity}.
 * @param <ENTITY> The {@link TopologyGraphEntity} implementation.
 */
public class TopologyGraphCreator<BUILDER extends TopologyGraphEntity.Builder<BUILDER, ENTITY>,
                                  ENTITY extends TopologyGraphEntity<ENTITY>> {

    private final Logger logger = LogManager.getLogger();

    private final Long2ObjectMap<BUILDER> topoEntities;

    /**
     * Create a new instance with a specific set of input entities.
     *
     * @param entities The entities, by id.
     */
    public TopologyGraphCreator(@Nonnull final Long2ObjectMap<BUILDER> entities) {
        topoEntities = entities;
    }

    public TopologyGraphCreator() {
        topoEntities = new Long2ObjectOpenHashMap<>();
    }

    /**
     * Create a new graph creator, with a provided expected size.
     *
     * @param expectedSize The expected size for the graph.
     */
    public TopologyGraphCreator(int expectedSize) {
        topoEntities = new Long2ObjectOpenHashMap<>(expectedSize);
    }

    /**
     * Add an entity to the graph.
     *
     * @param entity The builder for the entity to add.
     */
    public TopologyGraphCreator<BUILDER, ENTITY> addEntity(@Nonnull final BUILDER entity) {
        return addEntities(Collections.singletonList(entity));
    }

    /**
     * Add multiple entities to the graph.
     *
     * @param entities The builders for the entities to add.
     */
    public TopologyGraphCreator<BUILDER, ENTITY> addEntities(@Nonnull final Collection<BUILDER> entities) {
        entities.forEach(entity -> {
            final BUILDER previous = topoEntities.put(entity.getOid(), entity);
            if (previous != null) {
                logger.error("Topology graph creation encountered duplicate entities " +
                        "with OID {}. Keeping latest.", entity.getOid());
            }
        });
        return this;
    }

    /**
     * Build the {@link TopologyGraph}.
     *
     * This will add the provider, consumer, connected to and connected from relationships to each
     * builder added to the graph using the appropriate methods in {@link TopologyGraphEntity.Builder},
     * and then call {@link TopologyGraphEntity.Builder#build} for every builder.
     *
     * @return The complete, immutable {@link TopologyGraph}.
     */
    @Nonnull
    public TopologyGraph<ENTITY> build() {
        MutableLong missedProviders = new MutableLong(0);
        MutableLong missedConnections = new MutableLong(0);

        // Clear any previously established consumers and providers because these relationships
        // will be set up from scratch while constructing the graph.
        topoEntities.values().forEach(Builder::clearConsumersAndProviders);

        topoEntities.values().forEach(entity -> {
            entity.getProviderIds()
                .forEach(providerId -> {
                    final BUILDER provider = topoEntities.get(providerId);
                    // The provider might be null if the entity has unplaced commodities (e.g. for clones)
                    if (provider != null) {
                        // Note that providers and consumers are lists, but we do not perform an explicit check
                        // that the edge does not already exist. That is because such a check is unnecessary on
                        // properly formed input. The providers are pulled from a set, so they must be unique,
                        // and because the entities themselves must be unique by OID (keys in constructor map are unique
                        // plus check that key==OID in the constructor ensures this as long as OIDs are unique),
                        // we are guaranteed that:
                        //
                        // 1. the provider cannot already be in the TopologyEntity's list of providers
                        // 2. the TopologyEntity cannot already be in the provider's list of consumers
                        //
                        // Having an explicit check for this invariant given the current implementation would
                        // be both expensive and redundant. However, keep this requirement in mind if making changes
                        // to the implementation.
                        entity.addProvider(provider);
                        provider.addConsumer(entity);
                    } else if (providerId >= 0) {
                        // Clones have negative OIDs for their unplaced commodities.
                        // If the OID is non-negative (and, thus, a valid ID), this means
                        // the input topology is inconsistent.
                        missedProviders.increment();
                    }
                });

            entity.getConnectionIds()
                .forEach(connectionToEntity -> {
                    final BUILDER connected = topoEntities.get(connectionToEntity.getConnectedEntityId());
                    if (connected != null) {
                        switch (connectionToEntity.getConnectionType()) {
                            case NORMAL_CONNECTION:
                                entity.addOutboundAssociation(connected);
                                connected.addInboundAssociation(entity);
                                break;
                            case AGGREGATED_BY_CONNECTION:
                                entity.addAggregator(connected);
                                connected.addAggregatedEntity(entity);
                                break;
                            case OWNS_CONNECTION:
                                entity.addOwnedEntity(connected);
                                connected.addOwner(entity);
                                break;
                            case CONTROLLED_BY_CONNECTION:
                                entity.addController(connected);
                                connected.addControlledEntity(entity);
                                break;
                        }
                    } else {
                        // it can be null as the connectedToEntity may not be in plan because
                        // we already filtered by scope.
                        // for example, if connectedEntity is a business account and connected
                        // to VMs from region 1 and VMs from region 2, when user select region1
                        // as the scope, then VMs from region 2 will be filtered out by scoping,
                        // thus VMs from region 2 do not appear in topologyBuilderMap
                        missedConnections.increment();
                    }
                });
        });

        final Long2ObjectOpenHashMap<ENTITY> entities = new Long2ObjectOpenHashMap<>(topoEntities.size());
        topoEntities.values().forEach(e -> {
            ENTITY newE = e.build();
            ENTITY oldE = entities.put(e.getOid(), newE);
            if (oldE != null) {
                throw new IllegalArgumentException("Entity " + e.getOid()
                    + " (name: " + newE.getDisplayName() + ") appears multiple times.");
            }
        });
        entities.trim();

        final Map<Integer, Collection<ENTITY>> entityTypeIndex = new HashMap<>();
        entities.values().forEach(entity ->
                entityTypeIndex.computeIfAbsent(entity.getEntityType(),
                    k -> new ArrayList<>()).add(entity));
        entityTypeIndex.values().forEach(entitiesOfType ->
            ((ArrayList<ENTITY>)entitiesOfType).trimToSize());
        return new TopologyGraph<>(entities, entityTypeIndex);
    }
}
