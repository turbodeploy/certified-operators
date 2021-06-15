package com.vmturbo.api.component.external.api.mapper.converter;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entity.EntityUptimeApiDTO;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;

/**
 * Class for converting from {@link EntityUptimeDTO} to {@link EntityUptimeApiDTO}.
 */
public class EntityUptimeDtoConverter {

    /**
     * Converts from {@link EntityUptimeDTO} to {@link EntityUptimeApiDTO}.
     *
     * @param entityUptime the {@link EntityUptimeDTO}
     * @return the {@link EntityUptimeApiDTO}
     */
    @Nonnull
    public EntityUptimeApiDTO convert(@Nonnull final EntityUptimeDTO entityUptime) {
        final EntityUptimeApiDTO entityUptimeApiDTO = new EntityUptimeApiDTO();
        if (entityUptime.hasCreationTimeMs()) {
            entityUptimeApiDTO.setCreationTimestamp(entityUptime.getCreationTimeMs());
        }
        if (entityUptime.hasUptimeDurationMs()) {
            entityUptimeApiDTO.setUptimeDurationInMilliseconds(entityUptime.getUptimeDurationMs());
        }
        if (entityUptime.hasTotalDurationMs()) {
            entityUptimeApiDTO.setTotalDurationInMilliseconds(entityUptime.getTotalDurationMs());
        }
        if (entityUptime.hasUptimePercentage()) {
            entityUptimeApiDTO.setUptimePercentage(entityUptime.getUptimePercentage());
        }
        return entityUptimeApiDTO;
    }
}
