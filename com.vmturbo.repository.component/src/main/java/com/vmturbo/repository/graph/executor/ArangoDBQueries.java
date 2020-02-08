package com.vmturbo.repository.graph.executor;

/**
 * A simple class to contain all the queries used by the {@link ArangoDBExecutor}.
 */
public class ArangoDBQueries {

    static final String SUPPLY_CHAIN_CONSUMER_QUERY_TEMPLATE =
            "<if(hasAllowedOidList)>LET accessOids = [\"<allowedOidList;separator=\",\">\"]<endif>\n" +
            "   FOR v, e, p IN 0..10\n" +
            "       OUTBOUND '<startingId>'\n" +
            "       <edgeCollection>\n" +
            "       OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
            "       <if(hasInclusionEntityTypes)>FILTER p.vertices[*].entityType ALL IN <inclusionEntityTypes><endif>\n" +
            "       <if(hasExclusionEntityTypes)>FILTER p.vertices[*].entityType NONE IN <exclusionEntityTypes><endif>\n" +
            "       <if(hasEnvType)>FILTER v.environmentType == '<envType>'<endif>\n" +
            "       <if(hasAllowedOidList)>FILTER v.oid IN accessOids<endif>\n" +
            "       RETURN DISTINCT { oid: v.oid, entityType: v.entityType, state: v.state, " +
            "           provider : SUBSTITUTE(e._from, '<vertexCollection>/', '')} ";

    static final String SUPPLY_CHAIN_PROVIDER_QUERY_TEMPLATE =
            "<if(hasAllowedOidList)>LET accessOids = [\"<allowedOidList;separator=\",\">\"]<endif>\n" +
            "   FOR v, e, p IN 0..10\n" +
            "       INBOUND '<startingId>'\n" +
            "       <edgeCollection>\n" +
            "       OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
            "       <if(hasInclusionEntityTypes)>FILTER p.vertices[*].entityType ALL IN <inclusionEntityTypes><endif>\n" +
            "       <if(hasExclusionEntityTypes)>FILTER p.vertices[*].entityType NONE IN <exclusionEntityTypes><endif>\n" +
            "       <if(hasEnvType)>FILTER v.environmentType == '<envType>'<endif>\n" +
            "       <if(hasAllowedOidList)>FILTER v.oid IN accessOids<endif>\n" +
            "       RETURN DISTINCT { oid: v.oid, entityType: v.entityType, state: v.state, " +
            "           consumer: SUBSTITUTE(e._to, '<vertexCollection>/', '')} ";

    /**
     * New query for computing the global supply chain.
     */
    static final String GLOBAL_SUPPLY_CHAIN_QUERY_TEMPLATE =
            "<if(hasAllowedOidList)>LET accessOids = [\"<allowedOidList;separator=\",\">\"]<endif>\n" +
            "FOR entity IN <seCollection>\n" +
            "<if(hasIgnoredEntityTypes)>" +
                "FILTER entity.entityType NOT IN <ignoredEntityTypes>\n" +
            "<endif>" +
            "<if(hasEnvType)>" +
                "FILTER entity.environmentType == '<envType>'\n" +
            "<endif>" +
            "<if(hasAllowedOidList)>" +
                "FILTER entity.oid IN accessOids\n" +
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
            "FILTER se.oid IN [${commaSepLongs}]\n" +
            "RETURN se";

    /**
     * Query to return all the entities within a scope.
     */
    static final String SCOPED_ENTITIES_BY_OID =
            "FOR se IN ${collection}\n" +
            "FILTER se.oid IN [${oids}]\n" +
            "RETURN { " +
                "oid: se.oid, " +
                "displayName: se.displayName, " +
                "entityType: se.entityType, " +
                "state: se.state, " +
                "targetIds: se.targetIds" +
            " }";

    /**
     *
     */
    static final String GET_ALL_ENTITIES =
            "FOR se IN ${collection} RETURN se";
    static final String GET_SUPPLY_CHAIN_RELS =
            "FOR rel IN globalSCProviderRels RETURN rel";
}
