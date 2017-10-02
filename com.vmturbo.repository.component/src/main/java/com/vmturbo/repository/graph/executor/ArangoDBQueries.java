package com.vmturbo.repository.graph.executor;

/**
 * A simple class to contain all the queries used by the {@link ArangoDBExecutor}.
 */
public class ArangoDBQueries {

    /**
     * Consumer side of the supply query.
     */
    static final String SUPPLY_CHAIN_CONSUMER_QUERY_STRING =
            "FOR v, e, p IN 1 .. 10\n" +
            "OUTBOUND '${startingId}'\n" +
            "${edgeCollection}\n" +
            "OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
            "LET from = (\n" +
            "    FOR fv IN ${vertexCollection}\n" +
            "    FILTER fv._id == e._from\n" +
            "    RETURN { entityType: fv.entityType, depth: LENGTH(p.vertices), _key: fv._key}\n" +
            ")\n" +
            "LET to = (\n" +
            "    FOR tv IN ${vertexCollection}\n" +
            "    FILTER tv._id == e._to\n" +
            "    RETURN { entityType: tv.entityType, depth: LENGTH(p.vertices), _key: tv._key}\n" +
            ")\n" +
            "COLLECT ftype = FIRST(from).entityType\n" +
            "INTO   groups = { finstance: FIRST(from), fneighbour: FIRST(to)\n" +
            "                , tinstance: FIRST(to), tneighbour: FIRST(from) }\n" +
            "LET toInstancesGroups = MERGE(\n" +
            "    FOR toI in UNIQUE(groups[*].tinstance)\n" +
            "    COLLECT toInstType = toI.entityType INTO toInstances = { id : toI._key, depth : toI.depth }\n" +
            "    RETURN { [toInstType] : toInstances[*] }\n" +
            ")\n" +
            "RETURN { type: ftype\n" +
            "    , instances: UNIQUE(groups[*].finstance._key)\n" +
            "    , neighbourInstances:  toInstancesGroups\n" +
            "    }\n";

    /**
     * Provider side of the supply chain query.
     */
    static final String SUPPLY_CHAIN_PROVIDER_QUERY_STRING =
            "FOR v, e, p IN 1 .. 10\n" +
            "INBOUND '${startingId}'\n" +
            "${edgeCollection}\n" +
            "OPTIONS { bfs: true, uniqueVertices: 'path', uniqueEdges: 'path' }\n" +
            "LET from = (\n" +
                "FOR fv IN ${vertexCollection}\n" +
                "FILTER fv._id == e._from\n" +
                "RETURN { entityType: fv.entityType, depth: LENGTH(p.vertices), _key: fv._key}\n" +
            ")\n" +
            "LET to = (\n" +
                "FOR tv IN ${vertexCollection}\n" +
                "FILTER tv._id == e._to\n" +
                "RETURN { entityType: tv.entityType, depth: LENGTH(p.vertices), _key: tv._key}\n" +
            ")\n" +
            "COLLECT ttype = FIRST(to).entityType\n" +
            "INTO   groups = { finstance: FIRST(from), fneighbour: FIRST(to)\n" +
                            ", tinstance: FIRST(to), tneighbour: FIRST(from) }\n" +
            "LET fromInstancesGroups = MERGE(\n" +
                "FOR fromI in UNIQUE(groups[*].finstance)\n" +
                "COLLECT fromInstType = fromI.entityType INTO fromInstances = { id : fromI._key, depth : fromI.depth }\n" +
                "RETURN { [fromInstType] : fromInstances[*] }\n" +
            ")\n" +
            "RETURN { type: ttype\n" +
                   ", instances: UNIQUE(groups[*].tinstance._key)\n" +
                   ", neighbourInstances:  fromInstancesGroups\n" +
            "}\n";

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
