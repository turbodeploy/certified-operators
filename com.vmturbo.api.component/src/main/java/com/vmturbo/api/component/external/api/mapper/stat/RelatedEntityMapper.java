package com.vmturbo.api.component.external.api.mapper.stat;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.mapper.utils.OidExtractor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;

/**
 * A mapper of an OID reference within an internal data type ({@link InternalTypeT}) to {@link StatApiDTO#getRelatedEntity()}.
 * @param <InternalTypeT> The internal stat representation.
 */
public class RelatedEntityMapper<InternalTypeT> implements EntityStatAttributeMapper<InternalTypeT> {

    private final OidExtractor<InternalTypeT> oidExtractor;

    private RelatedEntityMapper(@Nonnull OidExtractor<InternalTypeT> oidExtractor) {

        this.oidExtractor = Objects.requireNonNull(oidExtractor);
    }

    /**
     * Creates a new {@link RelatedEntityMapper} instance.
     * @param oidExtractor The OID extractor.
     * @param <InternalTypeT> The internal stat representation.
     * @return The new {@link RelatedEntityMapper} instance.
     */
    @Nonnull
    public static <InternalTypeT> RelatedEntityMapper<InternalTypeT> of(@Nonnull OidExtractor<InternalTypeT> oidExtractor) {
        return new RelatedEntityMapper<>(oidExtractor);
    }

    @Override
    public OidExtractor<InternalTypeT> oidExtractor() {
        return oidExtractor;
    }

    @Override
    public void updateStatDto(long entityOid,
                              @Nullable CachedEntityInfo entityInfo,
                              @Nonnull StatApiDTO statDto) {

        if (entityInfo != null) {
            final BaseApiDTO relatedEntity = new BaseApiDTO();
            relatedEntity.setDisplayName(entityInfo.getDisplayName());
            statDto.setRelatedEntity(relatedEntity);
            statDto.setRelatedEntityType(entityInfo.getEntityType().apiStr());
        }
    }
}
