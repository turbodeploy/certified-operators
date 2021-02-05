package com.vmturbo.extractor.topology.attributes;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2LongMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.schema.enums.AttrType;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.topology.attributes.EnumOidRetriever.EnumOidRetrievalException;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Factory class for processors that record the {@link AttrType.ENTITY_STATE} attribute.
 */
class EntityStateAttributeProcessorFactory extends HistoricalAttributeProcessorFactory<Integer> {

    private final EnumOidRetriever<EntityState> enumOidRetriever;

    EntityStateAttributeProcessorFactory(EnumOidRetriever<EntityState> stateRetriever,
            @Nonnull Clock clock,
            long forceUpdateInterval,
            TimeUnit forceUpdateIntervalUnits) {
        super(clock, forceUpdateInterval, forceUpdateIntervalUnits);
        this.enumOidRetriever = stateRetriever;
    }

    @Override
    @Nullable
    HistoricalAttributeProcessor<Integer> newProcessorInternal(DbEndpoint dbEndpoint,
            Long2LongMap hashByEntityIdMap,
            Consumer<Long2LongMap> onSuccessConsumer) {
        final Map<EntityState, Integer> enumOids;
        try {
            enumOids = enumOidRetriever.getEnumOids(dbEndpoint);
            return new HistoricalAttributeProcessor<Integer>(HistoricalAttributes.INT_VALUE,
                    AttrType.ENTITY_STATE, hashByEntityIdMap, onSuccessConsumer) {
                @Override
                @Nullable
                Integer extractValue(TopologyEntityDTO entity) {
                    return enumOids.get(EntityStateUtils.protoToDb(entity.getEntityState()));
                }
            };
        } catch (EnumOidRetrievalException e) {
            logger.error("Failed to fetch entity state to enum OID mappings."
                    + " Not recording entity state properties.", e);
            return null;
        }
    }
}
