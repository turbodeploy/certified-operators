package com.vmturbo.api.component.external.api.util.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.components.common.utils.StringConstants;

public class StatsTestUtil {

    @Nonnull
    public static StatApiDTO stat(final String name) {
        final StatApiDTO apiDTO = new StatApiDTO();
        apiDTO.setName(name);
        return apiDTO;
    }

    @Nonnull
    public static StatApiDTO stat(final String name, final String relatedEntityId) {
        final StatApiDTO apiDTO = stat(name);
        BaseApiDTO relatedEntity = new BaseApiDTO();
        relatedEntity.setUuid(relatedEntityId);
        apiDTO.setRelatedEntity(relatedEntity);
        return apiDTO;
    }

    @Nonnull
    public static StatApiDTO statWithKey(final String name, final String key) {
        final StatApiDTO apiDTO = stat(name);
        final List<StatFilterApiDTO> filters = new ArrayList<>();
        final StatFilterApiDTO filter = new StatFilterApiDTO();
        filter.setType(StringConstants.KEY);
        filter.setValue(key);
        filters.add(filter);
        apiDTO.setFilters(filters);
        return apiDTO;
    }

    @Nonnull
    public static StatApiInputDTO statInput(final String name) {
        StatApiInputDTO apiInputDTO = new StatApiInputDTO();
        apiInputDTO.setName(name);
        return apiInputDTO;
    }

    /**
     * Create EntityStatsScope based on the list of entities' ids. It sets the EntityList case.
     *
     * @param entities list of entities to create EntityStatsScope for
     * @return EntityStatsScope containing the given ids
     */
    public static EntityStatsScope createEntityStatsScope(@Nonnull Set<Long> entities) {
        return EntityStatsScope.newBuilder()
            .setEntityList(EntityList.newBuilder().addAllEntities(entities))
            .build();
    }
}
