package com.vmturbo.cost.component.entity.scope;

/**
 * Any store implementing this interface is dependent on the {@link CloudScopeStore}. If the implementing
 * store is backed by a SQL table, the expectation is that the associated table is foreign keyed to the
 * entity cloud scope table.
 *
 * The consolidation of entity cloud scope into a single store is meant to allow a diverse set of stores
 * to maintain the scope of historical data (in which the associated entity may no longer be in the topology
 * and therefore no longer queryable from repository component), without requiring each store/table to
 * denormalize the data and store repetitive cloud scope information.
 */
public interface CloudScopedStore {
}
