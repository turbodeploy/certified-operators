package com.vmturbo.history.stats.priceindex;

import java.util.Map;

import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * A visitor for {@link TopologyPriceIndices}, to iterate over the possible entity and
 * environment types in an orderly fashion.
 */
public interface TopologyPriceIndexVisitor {

    /**
     * Visit price indexes with given entity type and environment type.
     *
     * @param entityType         The entity type.
     * @param environmentType    The environment type.
     * @param priceIdxByEntityId The price indices for entities of this type and environment type,
     *                           arranged by ID.
     * @throws DataAccessException  If there is an error connecting to the database. Note -
     *                              technically
     *                              we should probably hide this exception and throw a
     *                              visitor-specific exception
     *                              instead, but it's not necessary right now with only one visitor
     *                              implementation.
     * @throws InterruptedException if interrupted
     */
    void visit(Integer entityType,
            EnvironmentType environmentType,
            Map<Long, Double> priceIdxByEntityId) throws DataAccessException, InterruptedException;

    /**
     * Called after all calls to
     * {@link TopologyPriceIndexVisitor#visit(Integer, EnvironmentType, Map)}.
     *
     * @throws DataAccessException  If there is an error connecting to the database. Note -
     *                              technically
     *                              we should probably hide this exception and throw a
     *                              visitor-specific exception
     *                              instead, but it's not necessary right now with only one visitor
     *                              implementation.
     * @throws InterruptedException if interrupted
     */
    void onComplete() throws DataAccessException, InterruptedException;
}
