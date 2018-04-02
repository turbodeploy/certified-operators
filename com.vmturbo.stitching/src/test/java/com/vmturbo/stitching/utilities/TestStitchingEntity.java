package com.vmturbo.stitching.utilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingMergeInformation;

/**
 * A simple wrapper around {@link TestStitchingEntity}. Most methods not implemented at this time.
 */
public class TestStitchingEntity implements StitchingEntity {
    private final EntityDTO.Builder entityBuilder;
    private final long oid;

    public TestStitchingEntity(@Nonnull final EntityDTO.Builder entityBuilder) {
        this.entityBuilder = Objects.requireNonNull(entityBuilder);
        this.oid = 1;
    }

    public TestStitchingEntity(final long oid,
                               @Nonnull final EntityDTO.Builder entityBuilder) {
        this.entityBuilder = Objects.requireNonNull(entityBuilder);
        this.oid = oid;
    }

    @Nonnull
    @Override
    public Builder getEntityBuilder() {
        return entityBuilder;
    }

    @Override
    public Set<StitchingEntity> getProviders() {
        return Collections.emptySet();
    }

    @Override
    public Set<StitchingEntity> getConsumers() {
        return Collections.emptySet();
    }

    @Override
    public long getOid() {
        return oid;
    }

    @Override
    public long getTargetId() {
        throw new IllegalStateException();
    }

    @Override
    public long getLastUpdatedTime() {
        throw new IllegalStateException();
    }

    @Override
    public boolean updateLastUpdatedTime(long updateTime) {
        throw new IllegalStateException();
    }

    @Override
    public Stream<CommodityDTO.Builder> getCommoditiesSold() {
        throw new IllegalStateException();
    }

    @Override
    public void addCommoditySold(@Nonnull CommodityDTO.Builder commoditySold,
                                 @Nonnull Optional<StitchingEntity> accesses) {
        throw new IllegalStateException();
    }

    @Override
    public Map<StitchingEntity, List<CommodityDTO.Builder>> getCommoditiesBoughtByProvider() {
        throw new IllegalStateException();
    }

    @Override
    public Optional<List<CommodityDTO.Builder>> removeProvider(@Nonnull StitchingEntity entity) {
        throw new IllegalStateException();
    }

    @Override
    public boolean hasProvider(@Nonnull StitchingEntity entity) {
        throw new IllegalStateException();
    }

    @Override
    public boolean hasConsumer(@Nonnull StitchingEntity entity) {
        throw new IllegalStateException();
    }

    @Nonnull
    @Override
    public List<StitchingMergeInformation> getMergeInformation() {
        throw new IllegalStateException();
    }

    @Override
    public boolean addMergeInformation(@Nonnull StitchingMergeInformation mergeInformation) {
        throw new IllegalStateException();
    }

    @Override
    public void addAllMergeInformation(@Nonnull List<StitchingMergeInformation> mergeInformation) {
        throw new IllegalStateException();
    }

    @Override
    public boolean hasMergeInformation() {
        throw new IllegalStateException();
    }
}
