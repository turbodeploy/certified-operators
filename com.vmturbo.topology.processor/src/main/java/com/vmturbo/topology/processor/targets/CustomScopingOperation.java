package com.vmturbo.topology.processor.targets;

import java.util.Set;

import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;

/**
 * Interface implemented by custom scoping operations that know how to convert entity scope values
 * for a specific probe type into OIDs of objects to pull the scope properties from.
 */
public interface CustomScopingOperation {

    /**
     * Take the value passed in by the entity scope account value and convert it to a list of
     * object OIDs for the objects that have that property value.
     *
     * @param scopeValue The value to match.
     * @param searchService a reference to the search service to allow searching for matches.
     * @return List of OIDs for matching instances.
     */
    Set<Long> convertScopeValueToOid(String scopeValue, SearchServiceBlockingStub searchService);

}
