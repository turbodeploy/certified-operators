package com.vmturbo.api.component.external.api.mapper.stat;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.statistic.StatApiDTO;

/**
 * A mapper of an internal stat ({@link InternalTypeT}) attribute to an attribute of {@link StatApiDTO}.
 * @param <InternalTypeT> The internal stat representation.
 */
public interface StatAttributeMapper<InternalTypeT> {

    /**
     * Updates the {@code statDto} with data from the {@code internalStat}.
     * @param internalStat The internal stat representation.
     * @param statDto The external stat DTO.
     */
    void updateStatDto(@Nonnull InternalTypeT internalStat,
                       @Nonnull StatApiDTO statDto);
}
