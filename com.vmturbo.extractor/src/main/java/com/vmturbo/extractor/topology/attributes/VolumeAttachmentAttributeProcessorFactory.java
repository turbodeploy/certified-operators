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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Records the attachment state of a volume - the {@link AttrType.VOLUME_ATTACHED} type.
 */
public class VolumeAttachmentAttributeProcessorFactory extends HistoricalAttributeProcessorFactory<Boolean> {

    VolumeAttachmentAttributeProcessorFactory(@Nonnull Clock clock, long forceUpdateInterval,
            TimeUnit forceUpdateIntervalUnits) {
        super(clock, forceUpdateInterval, forceUpdateIntervalUnits);
    }

    @Nullable
    @Override
    HistoricalAttributeProcessor<Boolean> newProcessorInternal(DbEndpoint dbEndpoint,
            Long2LongMap hashByEntityIdMap, Consumer<Long2LongMap> onSuccess) {
        return new HistoricalAttributeProcessor<Boolean>(HistoricalAttributes.BOOL_VALUE,
                AttrType.VOLUME_ATTACHED, hashByEntityIdMap, onSuccess) {
            @Nullable
            @Override
            Boolean extractValue(TopologyEntityDTO entity) {
                if (entity.getTypeSpecificInfo().hasVirtualVolume()) {
                    return entity.getTypeSpecificInfo().getVirtualVolume().getAttachmentState() == AttachmentState.ATTACHED;
                } else {
                    return null;
                }
            }
        };
    }
}
