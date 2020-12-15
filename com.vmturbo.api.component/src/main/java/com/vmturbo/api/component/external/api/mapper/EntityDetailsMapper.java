package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.dto.entity.DetailDataApiDTO;
import com.vmturbo.api.dto.entity.EntityDetailsApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * This class converts {@link TopologyEntityDTO}s to {@link EntityDetailsApiDTO}.
 */
public class EntityDetailsMapper {

    private static final String ENHANCED_BY_PROP = "Enhanced by";

    private final ThinTargetCache thinTargetCache;

    /**
     * Constructor.
     *
     * @param thinTargetCache - a provider of target data.
     */
    public EntityDetailsMapper(@Nonnull ThinTargetCache thinTargetCache) {
        this.thinTargetCache = thinTargetCache;
    }

    /**
     * Return entities with metadata.
     *
     * @param entities list of topology entities.
     * @return a list of {@link EntityDetailsApiDTO}.
     */
    @Nonnull
    public List<EntityDetailsApiDTO> toEntitiesDetails(
            @Nonnull final Collection<TopologyEntityDTO> entities) {
        return entities.stream()
                .map(this::toEntityDetails)
                .collect(Collectors.toList());
    }

    /**
     * Return entity with details.
     *
     * @param entity a topology entity.
     * @return an entity with details.
     */
    @VisibleForTesting
    @Nonnull
    public EntityDetailsApiDTO toEntityDetails(
            @Nonnull final TopologyEntityDTO entity) {
        final EntityDetailsApiDTO entityApiDTO = new EntityDetailsApiDTO();
        entityApiDTO.setUuid(entity.getOid());
        entityApiDTO.setDetails(getDetails(entity));
        return entityApiDTO;
    }

    @Nonnull
    private List<DetailDataApiDTO> getDetails(@Nonnull final TopologyEntityDTO entity) {
        final List<DetailDataApiDTO> details = new ArrayList<>();
        getEnhancedByProperty(entity).ifPresent(details::add);
        return details;
    }

    @Nonnull
    private Optional<DetailDataApiDTO> getEnhancedByProperty(@Nonnull final TopologyEntityDTO entity) {
        final Set<Long> targetsIds = getProxyOriginTargets(entity);
        if (!targetsIds.isEmpty()) {
            final Set<String> targetsTypes = getTargetTypes(targetsIds);
            if (!targetsTypes.isEmpty()) {
                final DetailDataApiDTO detailData = new DetailDataApiDTO();
                detailData.setKey(ENHANCED_BY_PROP);
                detailData.setCritical(true);
                detailData.setValue(String.join(",", targetsTypes));
                return Optional.of(detailData);
            }
        }
        return Optional.empty();
    }

    @Nonnull
    private Set<Long> getProxyOriginTargets(@Nonnull final TopologyEntityDTO entity) {
        return entity.getOrigin().getDiscoveryOrigin()
                .getDiscoveredTargetDataMap().entrySet().stream()
                .filter(entry -> entry.getValue().getOrigin() == EntityOrigin.PROXY)
                .map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    @Nonnull
    private Set<String> getTargetTypes(@Nonnull final Set<Long> targetsIds) {
        return targetsIds.stream().map(thinTargetCache::getTargetInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(info -> info.probeInfo().type())
                .collect(Collectors.toSet());
    }

}
