package com.vmturbo.repository.graph.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.CollectionOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.EdgeOperationException;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.VertexOperationException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.graph.parameter.CollectionParameter;
import com.vmturbo.repository.graph.parameter.EdgeParameter;
import com.vmturbo.repository.graph.parameter.EdgeParameter.EdgeType;
import com.vmturbo.repository.graph.parameter.VertexParameter;

/**
 * Constructs a subgraph in an associated graph database from input SE DTOs.
 *
 * The subgraph contains:
 * <ul>
 *     <li> An SE vertex collection, where a vertex is created for each SE DTO. </li>
 *     <li> A provider edge collection, where an edge is created each provider relationship b/t SEs.
 *          Such edge indicates that the start vertex is a provider of the end vertex. </li>
 * </ul>
 * E.g., for a topology contains two VMs and one provider PM, the SE vertex collection will contain
 * three vertices for the three SEs and the provider edge collection contains two edges for each
 * provider relationship b/t the PM and its hosting VMs.
 *
 * The graph with the collections will then be used by the graph database for the queries,
 * e.g., graph traversal.
 */
public class ServiceEntitySubGraphCreator {
    private final Logger logger = LoggerFactory.getLogger(ServiceEntitySubGraphCreator.class);

    /**
     * Map keeping all edge types to create, and the function for each edge type which is about how
     * to get entities on the other side of the edge for a given entity.
     */
    private static final Map<EdgeType, Function<ServiceEntityRepoDTO, List<String>>> EDGE_TYPE_TO_GET_ENTITIES_FUNCTION =
            ImmutableMap.of(
                    EdgeType.CONSUMES, se -> se.getProviders(),
                    EdgeType.CONNECTED, se -> se.getConnectedEntityList() == null ?
                            Collections.emptyList() :
                            se.getConnectedEntityList().stream()
                                    .map(connectedEntity -> String.valueOf(connectedEntity.getConnectedEntityId()))
                                    .collect(Collectors.toList())
            );

    private final GraphDatabaseDriver graphDatabaseDriver;
    private final GraphDefinition graphDefinition;
    private final int batchSize;
    private final int replicaCount;

    public ServiceEntitySubGraphCreator(final GraphDatabaseDriver graphDatabaseDriver,
                                        final GraphDefinition graphDefinition,
                                        final int batchSize,
                                        final int replicaCount) {
        this.graphDefinition = Objects.requireNonNull(graphDefinition);
        this.graphDatabaseDriver = Objects.requireNonNull(graphDatabaseDriver);
        this.batchSize = batchSize;
        this.replicaCount = replicaCount;
    }

    /**
     * Creates the vertex and edge collections in the subgraph.
     *
     * @throws CollectionOperationException
     */
    void init() throws CollectionOperationException {
        logger.debug("Creating vertex collection: {}", graphDefinition.getServiceEntityVertex());
        CollectionParameter paramSeVertexCollection =
                new CollectionParameter.Builder(graphDefinition.getServiceEntityVertex())
                        .replicaCount(replicaCount)
                        .build();
        graphDatabaseDriver.createCollection(paramSeVertexCollection);

        logger.debug("Creating edge collection: {}", graphDefinition.getProviderRelationship());
        CollectionParameter paramSeProvEdgeCollection =
                new CollectionParameter.Builder(graphDefinition.getProviderRelationship())
                        .replicaCount(replicaCount)
                        .edge()
                        .waitForSync()
                        .build();
        graphDatabaseDriver.createCollection(paramSeProvEdgeCollection);

        logger.debug("Creating supply chain relationship collection: {}",
                ArangoDBExecutor.GLOBAL_SUPPLY_CHAIN_RELS_COLLECTION);
        CollectionParameter globalSupplyChainProviderRels =
            new CollectionParameter
                    .Builder(ArangoDBExecutor.GLOBAL_SUPPLY_CHAIN_RELS_COLLECTION)
                    .replicaCount(replicaCount)
                    .waitForSync()
                    .build();
        graphDatabaseDriver.createCollection(globalSupplyChainProviderRels);

        logger.debug("Creating supply chain relationship collection: {}",
                ArangoDBExecutor.GLOBAL_SUPPLY_CHAIN_ENTITIES_COLLECTION);
        CollectionParameter globalSupplyChainEntitiesCollection =
                new CollectionParameter
                        .Builder(ArangoDBExecutor.GLOBAL_SUPPLY_CHAIN_ENTITIES_COLLECTION)
                        .replicaCount(replicaCount)
                        .waitForSync()
                        .build();
        graphDatabaseDriver.createCollection(globalSupplyChainEntitiesCollection);
    }

    /**
     * Empties the vertex and edge collections in the subgraph.
     *
     * @throws CollectionOperationException
     */
    void reset() throws CollectionOperationException {
        logger.debug("Reseting vertex collection: " + graphDefinition.getServiceEntityVertex());
        graphDatabaseDriver.emptyCollection(graphDefinition.getServiceEntityVertex());

        logger.debug("Reseting edge collection: " + graphDefinition.getProviderRelationship());
        graphDatabaseDriver.emptyCollection(graphDefinition.getProviderRelationship());
    }

    /**
     * Creates the subgraph for the topology
     *
     * @param ses The service entities of the topology
     * @throws VertexOperationException
     * @throws EdgeOperationException
     */
    void create(Collection<ServiceEntityRepoDTO> ses)
            throws VertexOperationException, EdgeOperationException {
        logger.debug("Creating vertices for SEs");
        createVertices(ses);

        // create edges for different types of relationship on the SE
        for (Map.Entry<EdgeType, Function<ServiceEntityRepoDTO, List<String>>> entry :
                EDGE_TYPE_TO_GET_ENTITIES_FUNCTION.entrySet()) {
            logger.debug("Creating edges for SE {} relationship", entry.getKey());
            createEdges(ses, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Create edges for a collection of entities, for the given edge type and the function about
     * how to get related entities for an entity.
     *
     * @param ses the collection of entities to create edges for
     * @param edgeType the type of the edge to create
     * @param getRelatedEntities function which tells how to find related entities for an entity
     * @throws EdgeOperationException
     */
    private void createEdges(@Nonnull Collection<ServiceEntityRepoDTO> ses,
                             @Nonnull EdgeType edgeType,
                             @Nonnull Function<ServiceEntityRepoDTO, List<String>> getRelatedEntities)
            throws EdgeOperationException {
        // TODO: move the Arango-specific logic to ArangoGraphDatabaseDriver
        final String handlePrefix = graphDefinition.getServiceEntityVertex() + "/";
        EdgeParameter p = new EdgeParameter.Builder(
                graphDefinition.getProviderRelationship(), "from", "to", edgeType).build();

        int counter = 0;
        List<String> froms = new ArrayList<>();
        List<String> tos = new ArrayList<>();

        for (ServiceEntityRepoDTO se : ses) {
            Collection<String> providers = getRelatedEntities.apply(se);
            if (providers == null || providers.isEmpty()) {
                continue;
            }

            for (String prov : providers) {
                if (counter == batchSize) {
                    counter = 0;
                    graphDatabaseDriver.createEdgesInBatch(p.withFroms(froms).withTos(tos));
                    froms = new ArrayList<>();
                    tos = new ArrayList<>();
                }
                froms.add(handlePrefix + prov);
                tos.add(handlePrefix + getVertexKey(se));
                counter++;
            }

        }

        if (!froms.isEmpty()) {
            graphDatabaseDriver.createEdgesInBatch(p.withFroms(froms).withTos(tos));
        }
    }

    private void createVertices(Collection<ServiceEntityRepoDTO> ses)
            throws VertexOperationException {
        VertexParameter p = new VertexParameter.Builder(
                graphDefinition.getServiceEntityVertex()).build();

        List<List<ServiceEntityRepoDTO>> batches =
                Lists.partition(ImmutableList.copyOf(ses), batchSize);
        for (List<ServiceEntityRepoDTO> batch : batches) {
            graphDatabaseDriver.createVerticesInBatch(p
                    .withKeys(batch.stream().map(ServiceEntitySubGraphCreator::getVertexKey)
                                            .collect(Collectors.toList()))
                    .withValues(batch));
        }
    }

    private static String getVertexKey(ServiceEntityRepoDTO se) {
        return String.valueOf(se.getOid());
    }
}
