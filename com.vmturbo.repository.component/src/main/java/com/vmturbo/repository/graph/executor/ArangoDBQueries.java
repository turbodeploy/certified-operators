package com.vmturbo.repository.graph.executor;

/**
 * A simple class to contain all the queries used by the {@link ArangoDBExecutor}.
 */
public class ArangoDBQueries {

    /**
     * Consumer side of the supply query.
     * See https://docs.arangodb.com/3.0/AQL/Graphs/Traversals.html for further details.
     */
    static final String SUPPLY_CHAIN_CONSUMER_QUERY_STRING =
        // Collect edges up to a maximum of 10 degrees away from the starting vertex.
        "LET edgeCollection = (" +
        "   FOR v, e IN 1 .. 10\n" +
        "       OUTBOUND '${startingId}'\n" +
        "       ${edgeCollection}\n" +
        "       OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
        // From are starting vertices on the directed edges (the consumers).
        "       LET from = (\n" +
        "           FOR fv IN ${vertexCollection}\n" +
        "               FILTER fv._id == e._from\n" +
        "               RETURN { entityType: fv.entityType, id: fv._key}\n" +
        "       )\n" +
        // To are the ending vertices on the directed edges (the providers).
        "       LET to = (\n" +
        "           FOR tv IN ${vertexCollection}\n" +
        "               FILTER tv._id == e._to\n" +
        "               RETURN { entityType: tv.entityType, id: tv._key}\n" +
        "       )\n" +
        // "ftype" is the type of the first "from" vertex and because we group by entity type,
        // it will be the type for all "from" vertices in the edgeCollection.
        "   COLLECT ftype = FIRST(from).entityType\n" +
        "   INTO   groups = { provider: FIRST(from), consumer: FIRST(to) }\n" +
        "   RETURN { type: ftype, edges:  UNIQUE(groups[*]) }\n" +
        ")\n" +
        // Collect information on the origin vertex (the vertex where the search started).
        "LET originCollection = ( RETURN DOCUMENT('${startingId}') )\n" +
        "LET origin = FIRST(originCollection)\n" +
        // Return a document containing the origin and all edges up to 10 degrees away from the origin
        // that are reachable by traversing in the consumer direction.
        "RETURN { origin: {id: origin._key, entityType: origin.entityType}, edgeCollection: edgeCollection }\n";

    /**
     * Provider side of the supply chain query.
     * See https://docs.arangodb.com/3.0/AQL/Graphs/Traversals.html for further details.
     */
    static final String SUPPLY_CHAIN_PROVIDER_QUERY_STRING =
        // Collect edges up to a maximum of 10 degrees away from the starting vertex.
        "LET edgeCollection = (" +
        "   FOR v, e IN 1 .. 10\n" +
        "       INBOUND '${startingId}'\n" +
        "       ${edgeCollection}\n" +
        "       OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
        // From are starting vertices on the directed edges (the consumers).
        "       LET from = (\n" +
        "           FOR fv IN ${vertexCollection}\n" +
        "               FILTER fv._id == e._from\n" +
        "               RETURN { entityType: fv.entityType, id: fv._key}\n" +
        "       )\n" +
        // To are the ending vertices on the directed edges (the providers).
        "       LET to = (\n" +
        "           FOR tv IN ${vertexCollection}\n" +
        "               FILTER tv._id == e._to\n" +
        "               RETURN { entityType: tv.entityType, id: tv._key}\n" +
        "       )\n" +
        // "ttype" is the type of the first "to" vertex and because we group by entity type,
        // it will be the type for all "to" vertices in the edgeCollection.
        "       COLLECT ttype = FIRST(to).entityType\n" +
        "       INTO   groups = { provider: FIRST(from), consumer: FIRST(to) }\n" +
        "       RETURN { entityType: ttype, edges:  UNIQUE(groups[*]) }\n" +
        ")\n" +
        "LET originCollection = ( RETURN DOCUMENT('${startingId}') )\n" +
        "LET origin = FIRST(originCollection)\n" +
        // Return a document containing the origin and all edges up to 10 degrees away from the origin
        // that are reachable by traversing in the provider direction.
        "RETURN { origin: {id: origin._key, entityType: origin.entityType}, edgeCollection: edgeCollection }\n";

    /**
     * New query for computing the supply chain for an entity.
     */
    static final String SUPPLY_CHAIN_REACTIVE_QUERY_STRING =
            "FOR v IN 1..10\n" +
            "${direction} '${startingId}'\n" +
            "${edgeCollection}\n" +
            "COLLECT types = v.entityType INTO oids = v.oid\n" +
            "RETURN {\"entityType\": types, \"oids\": UNIQUE(oids)}";

    /**
     * New query for computing the global supply chain.
     */
    static final String GLOBAL_SUPPLY_CHAIN_REACTIVE_QUERY_STRING =
            "FOR entity IN ${seCollection}\n" +
            "COLLECT types = entity.entityType INTO oids = entity.oid\n" +
            "RETURN { \"entityType\": types, \"oids\": oids }";

    /**
     * Perform a <code>FULLTEXT</code> search on a field with a <code>FULLTEXT</code> index.
     */
    static final String SEARCH_SERVICE_ENTITY_FULLTEXT_QUERY_STRING =
            "FOR se IN FULLTEXT(${collection}, '${field}', 'prefix:${query}')\n" +
            "RETURN se";

    /**
     * Basic string equality query on a field.
     */
    static final String SEARCH_SERVICE_ENTITY_QUERY_STRING =
            "FOR se IN ${collection}\n" +
            "FILTER se.${field} == ${value}\n" +
            "RETURN se";

    /**
     * Query to search for multiple entities by OID.
     */
    static final String GET_ENTITIES_BY_OID =
            "FOR se IN ${collection}\n" +
            "FILTER TO_NUMBER(se.oid) IN [${commaSepLongs}]\n" +
            "RETURN se";

    /**
     * Query to return all the entities within a scope.
     */
    static final String SCOPED_ENTITIES_BY_OID =
            "FOR se IN ${collection}\n" +
            "FILTER TO_NUMBER(se.oid) IN [${oids}]\n" +
            "RETURN { oid: se.oid, displayName: se.displayName, entityType: se.entityType, state: se.state }";

    /**
     * Part of the global supply chain query.
     */
    static final String INSTANCES_OF_TYPES_QUERY_STRING =
            "FOR etype IN [${types}]\n" +
            "FOR entity IN ${vertexCollection}\n" +
            "FILTER entity.entityType == etype\n" +
            "COLLECT entityTypes = entity.entityType\n" +
            "INTO instances = entity\n" +
            "RETURN { type: entityTypes, instances: instances }";
}
