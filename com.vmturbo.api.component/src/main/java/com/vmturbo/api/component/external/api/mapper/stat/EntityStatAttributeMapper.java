package com.vmturbo.api.component.external.api.mapper.stat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.mapper.utils.OidExtractor;
import com.vmturbo.api.dto.statistic.StatApiDTO;

/**
 * A mapper of an entity reference within an {@link InternalTypeT} to a {@link StatApiDTO} attribute. This mapper requires the
 * process to be broken into two steps. First, the OID is optionally extracted from the {@link InternalTypeT} instance. An external
 * process will then be responsible for resolving the OID to {@link CachedEntityInfo}. The second step will be to invoke
 * {@link #updateStatDto(long, CachedEntityInfo, StatApiDTO)} with the entity info, if it can be successfully resolved.
 * @param <InternalTypeT> The internal stat representation.
 */
public interface EntityStatAttributeMapper<InternalTypeT> {

    /**
     * Extractor of OID references from {@link InternalTypeT}.
     * @return Extractor of OID references from {@link InternalTypeT}.
     */
    @Nonnull
    OidExtractor<InternalTypeT> oidExtractor();

    /**
     * Updates the provided {@link StatApiDTO} with the entity info.
     * @param entityOid The entity OID, resolved through the {@link #oidExtractor()}.
     * @param entityInfo The entity info. May be null if the info could not be retrieved.
     * @param statDto The stat DTO to update.
     */
    void updateStatDto(long entityOid,
                       @Nullable CachedEntityInfo entityInfo,
                       @Nonnull StatApiDTO statDto);
}
