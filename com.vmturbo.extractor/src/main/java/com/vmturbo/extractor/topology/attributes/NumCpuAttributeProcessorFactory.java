package com.vmturbo.extractor.topology.attributes;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2LongMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.schema.enums.AttrType;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Factory class for processors that record the {@link AttrType.NUM_VCPU} attribute.
 */
class NumCpuAttributeProcessorFactory extends HistoricalAttributeProcessorFactory<Integer> {

    NumCpuAttributeProcessorFactory(@Nonnull Clock clock, long forceUpdateInterval,
            TimeUnit forceUpdateIntervalUnits) {
        super(clock, forceUpdateInterval, forceUpdateIntervalUnits);
    }

    @Override
    HistoricalAttributeProcessor<Integer> newProcessorInternal(DbEndpoint dbEndpoint,
            Long2LongMap hashByEntityIdMap,
            Consumer<Long2LongMap> onSuccess) {
        return new HistoricalAttributeProcessor<Integer>(HistoricalAttributes.INT_VALUE,
                AttrType.NUM_VCPU, hashByEntityIdMap, onSuccess) {
            @Override
            @Nullable
            Integer extractValue(TopologyEntityDTO entity) {
                if (entity.getTypeSpecificInfo().getVirtualMachine().hasNumCpus()) {
                    return entity.getTypeSpecificInfo().getVirtualMachine().getNumCpus();
                } else {
                    return null;
                }
            }
        };
    }
}
