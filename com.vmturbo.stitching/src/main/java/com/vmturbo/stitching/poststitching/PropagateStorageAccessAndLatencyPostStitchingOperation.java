package com.vmturbo.stitching.poststitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

public class PropagateStorageAccessAndLatencyPostStitchingOperation implements PostStitchingOperation {
    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
        @Nonnull StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.DISK_ARRAY);
    }

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(@Nonnull Stream<TopologyEntity> entities,
                                                 @Nonnull EntitySettingsCollection settingsCollection,
                                                 @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
        return resultBuilder.build();
    }
}
