package com.vmturbo.history.stats.priceindex;

import java.util.Map;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.history.db.VmtDbException;

/**
 * A visitor for {@link TopologyPriceIndices}, to iterate over the possible entity and
 * environment types in an orderly fashion.
 */
public interface TopologyPriceIndexVisitor {

    /**
     * @param entityType The entity type.
     * @param environmentType The environment type.
     * @param priceIdxByEntityId The price indices for entities of this type and environment type,
     *                           arranged by ID.
     * @throws VmtDbException If there is an error connecting to the database. Note - technically
     *         we should probably hide this exception and throw a visitor-specific exception
     *         instead, but it's not necessary right now with only one visitor implementation.
     */
    void visit(final Integer entityType,
               final EnvironmentType environmentType,
               final Map<Long, Double> priceIdxByEntityId) throws VmtDbException;

    /**
     * Called after all calls to
     * {@link TopologyPriceIndexVisitor#visit(Integer, EnvironmentType, Map)}.
     *
     * @throws VmtDbException If there is an error connecting to the database. Note - technically
     *         we should probably hide this exception and throw a visitor-specific exception
     *         instead, but it's not necessary right now with only one visitor implementation.
     */
    void onComplete() throws VmtDbException;
}
