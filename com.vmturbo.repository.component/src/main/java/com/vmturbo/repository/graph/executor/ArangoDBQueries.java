package com.vmturbo.repository.graph.executor;

/**
 * A simple class to contain all the queries used by the {@link ArangoDBExecutor}.
 */
public class ArangoDBQueries {

    /**
     * Consumer side of the supply query.
     * See https://docs.arangodb.com/3.0/AQL/Graphs/Traversals.html for further details.
     */
    static final String SUPPLY_CHAIN_CONSUMER_QUERY_TEMPLATE =
        // Collect edges up to a maximum of 10 degrees away from the starting vertex.
        "<if(hasAllowedOidList)>LET accessOids = [<allowedOidList;separator=\",\">]<endif>\n" +
        "LET edgeCollection = (" +
        "   FOR v, e IN 1 .. 10\n" +
        "       OUTBOUND '<startingId>'\n" +
        "       <edgeCollection>\n" +
        "       OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
        "       <if(hasEnvType)>FILTER v.environmentType == '<envType>'<endif>\n" +
        "       <if(hasAllowedOidList)>FILTER TO_NUMBER(v.oid) IN accessOids<endif>\n" +
        // Filter on edge type (consumes or connected)
        "       FILTER e.type == '<edgeType>'\n" +
        // From are starting vertices on the directed edges (the consumers).
        "       LET from = FIRST(\n" +
        "           FOR fv IN <vertexCollection>\n" +
        "               FILTER fv._id == e._from\n" +
        "              <if(hasAllowedOidList)>FILTER TO_NUMBER(fv.oid) IN accessOids<endif>\n" +
        "               RETURN fv\n" +
        "       )\n" +
        // To are the ending vertices on the directed edges (the providers).
        "       LET to = FIRST(\n" +
        "           FOR tv IN <vertexCollection>\n" +
        "               FILTER tv._id == e._to\n" +
        "              <if(hasAllowedOidList)>FILTER TO_NUMBER(tv.oid) IN accessOids<endif>\n" +
        "               RETURN tv\n" +
        "       )\n" +
        "       FILTER from.entityType != null && to.entityType != null\n" +
        // "ftype" is the type of the first "from" vertex and because we group by entity type,
        // it will be the type for all "from" vertices in the edgeCollection.
        "       COLLECT ftype = from.entityType\n" +
        "       INTO groups = {\n" +
        "           provider: { entityType: from.entityType, state: from.state, id: from._key },\n" +
        "           consumer: { entityType: to.entityType, state: to.state, id: to._key }\n" +
        "       }\n" +
        "       RETURN { type: ftype, edges:  UNIQUE(groups[*]) }\n" +
        ")\n" +
        // Collect information on the origin vertex (the vertex where the search started).
        "LET originCollection = ( RETURN DOCUMENT('<startingId>') )\n" +
        "LET origin = FIRST(originCollection)\n" +
        // Return a document containing the origin and all edges up to 10 degrees away from the origin
        // that are reachable by traversing in the consumer direction.
        "RETURN { origin: {id: origin._key, entityType: origin.entityType, state: origin.state}, edgeCollection: edgeCollection }\n";

    /**
     * Provider side of the supply chain query.
     * See https://docs.arangodb.com/3.0/AQL/Graphs/Traversals.html for further details.
     */
    static final String SUPPLY_CHAIN_PROVIDER_QUERY_TEMPLATE =
        // Collect edges up to a maximum of 10 degrees away from the starting vertex.
        "<if(hasAllowedOidList)>LET accessOids = [<allowedOidList;separator=\",\">]<endif>\n" +
        "LET edgeCollection = (" +
        "   FOR v, e IN 1 .. 10\n" +
        "       INBOUND '<startingId>'\n" +
        "       <edgeCollection>\n" +
        "       OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
        "       <if(hasEnvType)>FILTER v.environmentType == '<envType>'<endif>\n" +
        "       <if(hasAllowedOidList)>FILTER TO_NUMBER(v.oid) IN accessOids<endif>\n" +
        // Filter on edge type (consumes or connected)
        "       FILTER e.type == '<edgeType>'\n" +
        // From are starting vertices on the directed edges (the consumers).
        "       LET from = FIRST(\n" +
        "           FOR fv IN <vertexCollection>\n" +
        "               FILTER fv._id == e._from\n" +
        "              <if(hasAllowedOidList)>FILTER TO_NUMBER(fv.oid) IN accessOids<endif>\n" +
        "               RETURN fv\n" +
        "       )\n" +
        // To are the ending vertices on the directed edges (the providers).
        "       LET to = FIRST(\n" +
        "           FOR tv IN <vertexCollection>\n" +
        "               FILTER tv._id == e._to\n" +
        "              <if(hasAllowedOidList)>FILTER TO_NUMBER(tv.oid) IN accessOids<endif>\n" +
        "               RETURN tv\n" +
        "       )\n" +
        "       FILTER from.entityType != null && to.entityType != null\n" +
                // "ttype" is the type of the first "to" vertex and because we group by entity type,
        // it will be the type for all "to" vertices in the edgeCollection.
        "       COLLECT ttype = to.entityType\n" +
        "       INTO groups = {\n" +
        "           provider: { entityType: from.entityType, state: from.state, id: from._key },\n" +
        "           consumer: { entityType: to.entityType, state: to.state, id: to._key }\n" +
        "       }\n" +
        "       RETURN { entityType: ttype, edges:  UNIQUE(groups[*]) }\n" +
        ")\n" +
        "LET originCollection = ( RETURN DOCUMENT('<startingId>') )\n" +
        "LET origin = FIRST(originCollection)\n" +
        // Return a document containing the origin and all edges up to 10 degrees away from the origin
        // that are reachable by traversing in the provider direction.
        "RETURN { origin: {id: origin._key, entityType: origin.entityType, state: origin.state}, edgeCollection: edgeCollection }\n";

    /**
     * New query for computing the global supply chain.
     */
    static final String GLOBAL_SUPPLY_CHAIN_QUERY_TEMPLATE =
            "<if(hasAllowedOidList)>LET accessOids = [<allowedOidList;separator=\",\">]<endif>\n" +
            "FOR entity IN <seCollection>\n" +
            "<if(hasIgnoredEntityTypes)>" +
                "FILTER entity.entityType NOT IN <ignoredEntityTypes>\n" +
            "<endif>" +
            "<if(hasEnvType)>" +
                "FILTER entity.environmentType == '<envType>'\n" +
            "<endif>" +
            "<if(hasAllowedOidList)>" +
                "FILTER TO_NUMBER(entity.oid) IN accessOids\n" +
            "<endif>" +
            "COLLECT types = entity.entityType, states = entity.state \n" +
            "INTO oids = entity.oid\n" +
            "RETURN { \"entityType\": types, \"state\": states, \"oids\": oids }";

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
     *
     */
    static final String GET_ALL_ENTITIES =
            "FOR se IN ${collection} RETURN se";
}
