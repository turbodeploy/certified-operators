package com.vmturbo.stitching.poststitching;

import java.time.Clock;
import java.time.Duration;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * This class used to implement warm up interval feature. When the entities are discovered, during
 * their first a few market cycles, Market should not generate resize down actions for them.
 * Because during their first a few market cycles, some entity data are not collected enough in order
 * to generate resize down actions, such max quantity data.
 * <p>
 * It use entity oid to find out the first time entity was discovered, and if entity's first discovered time is
 * larger than defined warm up interval minimum threshold, it will set IsEligibleForResizeDown flag to
 * false which Market will not generate resize down actions for this entity.
 */
public class SetResizeDownAnalysisSettingPostStitchingOperation implements PostStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    private final double resizeDownWarmUpIntervalHours;
    private final Clock clock;

    public SetResizeDownAnalysisSettingPostStitchingOperation(final double resizeDownWarmUpIntervalHours,
                                                              @Nonnull final Clock clock) {
        this.resizeDownWarmUpIntervalHours = resizeDownWarmUpIntervalHours;
        this.clock = clock;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.globalScope();
    }

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.filter(this::isEligibleForResizeDown)
                .forEach(entity -> resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate -> {
                    entityForUpdate.getTopologyEntityDtoBuilder()
                            .getAnalysisSettingsBuilder()
                            .setIsEligibleForResizeDown(true);
                    logger.debug("Setting resize down to true for entity {}", entityForUpdate.getOid());
                }));
        return resultBuilder.build();
    }

    /**
     * This method try to find out if input entity is eligible for resize down. It uses entity oid
     * to get its first discovered time, and compare with it current milliseconds timestamp, if the
     * difference hour is larger than defined warm up interval hours, it means the entity can be
     * resize down, otherwise, it can not be resized down for now. And also it only allow resize
     * down for active entities.
     *
     * @param entity {@link TopologyEntity} entity object.
     * @return a boolean, if true means can be resize down, if false means can not be resize down.
     */
    private boolean isEligibleForResizeDown(@Nonnull final TopologyEntity entity) {
        // only set resize down to true for active entities.
        if (entity.getTopologyEntityDtoBuilder().getEntityState() != EntityState.POWERED_ON
                // It is possible that resize down flag was already set due to action execution.
                // If it was entity becomes ineligible for setting of this flag again.
                || entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder().hasIsEligibleForResizeDown()) {
            return false;
        }

        final long entityDiscoveredTime = IdentityGenerator.toMilliTime(entity.getOid());
        final long diffMillis = clock.millis() - entityDiscoveredTime;
        return Duration.ofMillis(diffMillis).toMinutes() >= resizeDownWarmUpIntervalHours * 60;
    }
}
