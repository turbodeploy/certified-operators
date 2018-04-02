package com.vmturbo.stitching.prestitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * This class is used to filter out unsupported entities, for example business account entities
 * are not supported right now.
 */
public class RemoveNonMarketEntitiesPreStitchingOperation implements PreStitchingOperation {

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        // get scope by business account entity type.
        return stitchingScopeFactory.entityTypeScope(EntityType.BUSINESS_ACCOUNT);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(
            @Nonnull Stream<StitchingEntity> entities,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // remove all entities in scope which are all business account entities.
        entities.forEach(resultBuilder::queueEntityRemoval);
        return resultBuilder.build();
    }
}
