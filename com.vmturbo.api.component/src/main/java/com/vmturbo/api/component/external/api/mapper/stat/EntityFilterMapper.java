package com.vmturbo.api.component.external.api.mapper.stat;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.mapper.utils.OidExtractor;
import com.vmturbo.api.dto.statistic.StatApiDTO;

/**
 * An {@link EntityStatAttributeMapper}, adding a {@link com.vmturbo.api.dto.statistic.StatFilterApiDTO} with
 * the entity info to the {@link StatApiDTO}.
 * @param <InternalTypeT> The internal stat type.
 */
public class EntityFilterMapper<InternalTypeT> implements EntityStatAttributeMapper<InternalTypeT> {

    private final OidExtractor<InternalTypeT> oidExtractor;

    private final String filterType;

    private EntityFilterMapper(@Nonnull OidExtractor<InternalTypeT> oidExtractor,
                               @Nonnull String filterType) {

        this.oidExtractor = Objects.requireNonNull(oidExtractor);
        this.filterType = Objects.requireNonNull(filterType);
    }

    /**
     * Creates a new {@link EntityFilterMapper} instance.
     * @param oidExtractor The OID extractor.
     * @param filterType The filter type.
     * @param <InternalTypeT> The internal stat type.
     * @return The new {@link EntityFilterMapper} instance.
     */
    @Nonnull
    public static <InternalTypeT> EntityFilterMapper<InternalTypeT> of(@Nonnull OidExtractor<InternalTypeT> oidExtractor,
                                                                       @Nonnull String filterType) {
        return new EntityFilterMapper<>(oidExtractor, filterType);
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
            statDto.addFilter(filterType, entityInfo.getDisplayName());
        } else {
            //TODO:ejf - log warning
            statDto.addFilter(filterType, Long.toString(entityOid));
        }
    }
}
