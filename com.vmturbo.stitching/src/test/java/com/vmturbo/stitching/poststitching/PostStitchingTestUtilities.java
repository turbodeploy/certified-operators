package com.vmturbo.stitching.poststitching;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;

/**
 * Utility class to create test instances used in post-stitching.
 */
class PostStitchingTestUtilities {

    private  PostStitchingTestUtilities() {}

    /**
     * Result builder subclass for use in post-stitching unit tests.
     */
    static class UnitTestResultBuilder extends EntityChangesBuilder<TopologyEntity> {

        @Override
        public TopologicalChangelog build() {
            return buildInternal();
        }

        @Override
        public EntityChangesBuilder<TopologyEntity>
            queueUpdateEntityAlone(@Nonnull final TopologyEntity entityToUpdate,
                                   @Nonnull final Consumer<TopologyEntity> updateMethod) {
            changes.add(new PostStitchingUnitTestChange(entityToUpdate, updateMethod));
            return this;
        }
    }

    /**
     * Topological change subclass for use in post-stitching unit tests.
     */
    private static class PostStitchingUnitTestChange implements TopologicalChange {
        private final TopologyEntity entityToUpdate;
        private final Consumer<TopologyEntity> updateMethod;

        PostStitchingUnitTestChange(@Nonnull final TopologyEntity entityToUpdate,
                                    @Nonnull final Consumer<TopologyEntity> updateMethod) {
            this.entityToUpdate = Objects.requireNonNull(entityToUpdate);
            this.updateMethod = Objects.requireNonNull(updateMethod);
        }

        @Override
        public void applyChange() {
            updateMethod.accept(entityToUpdate);
        }
    }

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldDTO> commoditiesSold) {
        return TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .addAllCommoditySoldList(commoditiesSold))
            .build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                             @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                             @Nonnull final List<CommodityBoughtDTO> commoditiesBought,
                                             @Nonnull final List<TopologyEntity.Builder> providers) {
        final TopologyEntity.Builder builder =
            makeTopologyEntityBuilder(entityType, commoditiesSold, commoditiesBought);
        providers.forEach(builder::addProvider);
        return builder.build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                             @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                             @Nonnull final Set<CommoditiesBoughtFromProvider> CommoditiesBoughtFromProvider,
                                             @Nonnull final List<TopologyEntity.Builder> providers) {
        final TopologyEntity.Builder builder =
            makeTopologyEntityBuilder(entityType, commoditiesSold, CommoditiesBoughtFromProvider);
        providers.forEach(builder::addProvider);
        return builder.build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                             @Nonnull final List<CommoditySoldDTO> commoditiesSold) {
        return TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder().setEntityType(entityType)
            .addAllCommoditySoldList(commoditiesSold)).build();
    }

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                             @Nonnull final Map<String, String> propertyMap) {
        return TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .addAllCommoditySoldList(commoditiesSold)
            .putAllEntityPropertyMap(propertyMap))
            .build();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
                                                            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                                            @Nonnull final List<CommodityBoughtDTO> commoditiesBought) {
        return makeTopologyEntityBuilder(0, entityType, commoditiesSold, commoditiesBought);
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
                                                            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                                            @Nonnull final Set<CommoditiesBoughtFromProvider> CommoditiesBoughtFromProvider) {
        return makeTopologyEntityBuilder(0, entityType, commoditiesSold, CommoditiesBoughtFromProvider);
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
                                                            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                                            @Nonnull final List<CommodityBoughtDTO> commoditiesBought) {
        return TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType)
                .addAllCommoditySoldList(commoditiesSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(commoditiesBought)
                ));
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
                                                            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                                            @Nonnull final Set<CommoditiesBoughtFromProvider> CommoditiesBoughtFromProvider) {
        return TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType)
                .addAllCommoditySoldList(commoditiesSold)
                .addAllCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider)
        );
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
                                                            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                                            @Nonnull final Map<Long, List<CommodityBoughtDTO>> commoditiesBoughtMap) {
        TopologyEntityDTO.Builder entityDtoBuilder =  TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setEntityType(entityType)
            .addAllCommoditySoldList(commoditiesSold);
        commoditiesBoughtMap.forEach((providerId, commoditiesBought) ->
            entityDtoBuilder.addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(providerId)
                    .addAllCommodityBought(commoditiesBought)
                    .build())
        );
        return TopologyEntity.newBuilder(entityDtoBuilder);
    }

    static class TopologyEntityBuilder {
        private final TopologyEntity.Builder builder;

        private final TopologyEntityDTO.Builder innerBuilder;

        private TopologyEntityBuilder() {
            innerBuilder = TopologyEntityDTO.newBuilder();
            builder = TopologyEntity.newBuilder(innerBuilder);
        }

        static TopologyEntityBuilder newBuilder() {
            return new TopologyEntityBuilder();
        }

        TopologyEntity build() {
            return builder.build();
        }

        TopologyEntity.Builder getBuilder() {
            return builder;
        }

        TopologyEntityBuilder withOid(final long oid) {
            innerBuilder.setOid(oid);
            return this;
        }

        TopologyEntityBuilder withCommoditiesSold(@Nonnull final CommoditySoldDTO... commodities) {
            for (final CommoditySoldDTO commodity : commodities) {
                innerBuilder.addCommoditySoldList(commodity);
            }
            return this;
        }

        TopologyEntityBuilder withCommoditiesSold(@Nonnull final CommoditySoldBuilder... commodities) {
            for (final CommoditySoldBuilder commodity : commodities) {
                innerBuilder.addCommoditySoldList(commodity.build());
            }
            return this;
        }

        TopologyEntityBuilder withCommoditiesSold(@Nonnull final Collection<CommoditySoldDTO> commodities) {
            innerBuilder.addAllCommoditySoldList(commodities);
            return this;
        }

        TopologyEntityBuilder withEntityType(final int type) {
            innerBuilder.setEntityType(type);
            return this;
        }

        TopologyEntityBuilder withCommoditiesBought(
            @Nonnull final CommodityBoughtDTO... commodities) {
            final CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProvider =
                CommoditiesBoughtFromProvider.newBuilder();
            for (final CommodityBoughtDTO commodity : commodities) {
                commoditiesBoughtFromProvider.addCommodityBought(commodity);
            }
            innerBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider);
            return this;
        }

        TopologyEntityBuilder withCommoditiesBought(
            @Nonnull final CommodityBoughtBuilder... commodities) {
            final CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProvider =
                CommoditiesBoughtFromProvider.newBuilder();
            for (final CommodityBoughtBuilder commodity : commodities) {
                commoditiesBoughtFromProvider.addCommodityBought(commodity.getBuilder());
            }
            innerBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider);
            return this;
        }

        TopologyEntityBuilder withCommoditiesBought(
            @Nonnull final Collection<CommodityBoughtDTO> commodities) {

            innerBuilder.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder().addAllCommodityBought(commodities));
            return this;
        }

        TopologyEntityBuilder withCommoditiesBoughtFromProviders(
            @Nonnull final Map<Long, Collection<CommodityBoughtDTO>> commodities) {

            commodities.entrySet().stream().map(entry ->
                CommoditiesBoughtFromProvider.newBuilder().setProviderId(entry.getKey())
                    .addAllCommodityBought(entry.getValue())
            ).forEach(innerBuilder::addCommoditiesBoughtFromProviders);

            return this;
        }

        TopologyEntityBuilder withCommoditiesBoughtFromProviders(
            @Nonnull final Collection<CommoditiesBoughtFromProvider> commodities) {
            innerBuilder.addAllCommoditiesBoughtFromProviders(commodities);
            return this;
        }

        TopologyEntityBuilder withProviders(
            @Nonnull final TopologyEntity.Builder... providers) {
            for (final Builder provider : providers) {
                builder.addProvider(provider);
            }
            return this;
        }

        TopologyEntityBuilder withProviders(@Nonnull final TopologyEntityBuilder... providers) {
            for (final TopologyEntityBuilder provider : providers) {
                builder.addProvider(provider.getBuilder());
            }
            return this;
        }

        TopologyEntityBuilder withConsumers(
            @Nonnull final TopologyEntityBuilder... consumers) {
            for (final TopologyEntityBuilder consumer : consumers) {
                builder.addConsumer(consumer.getBuilder());
            }
            return this;
        }

        TopologyEntityBuilder withConsumers(
            @Nonnull final TopologyEntity.Builder... consumers) {
            for (final TopologyEntity.Builder consumer : consumers) {
                builder.addConsumer(consumer);
            }
            return this;
        }

        TopologyEntityBuilder withProperty(@Nonnull final String key, @Nonnull final String value) {
            innerBuilder.putEntityPropertyMap(key, value);
            return this;
        }

        TopologyEntityBuilder withProperties(@Nonnull final Map<String, String> properties) {
            innerBuilder.putAllEntityPropertyMap(properties);
            return this;
        }

    }

    static class CommoditySoldBuilder {
        private CommoditySoldDTO.Builder builder;

        private CommoditySoldBuilder() {
            builder = CommoditySoldDTO.newBuilder();
        }

        CommoditySoldDTO build() {
            return builder.build();
        }

        static CommoditySoldBuilder newBuilder() {
            return new CommoditySoldBuilder();
        }

        CommoditySoldBuilder withType(@Nonnull final CommodityType type) {
            if (builder.hasCommodityType()) {
                final TopologyDTO.CommodityType current = builder.getCommodityType();
                builder.setCommodityType(current.toBuilder().setType(type.getNumber()));
            } else {
                builder.setCommodityType(
                    TopologyDTO.CommodityType.newBuilder().setType(type.getNumber()));
            }
            return this;
        }

        CommoditySoldBuilder withKey(@Nonnull final String key) {
            if (builder.hasCommodityType()) {
                final TopologyDTO.CommodityType current = builder.getCommodityType();
                builder.setCommodityType(current.toBuilder().setKey(key));
            } else {
                builder.setCommodityType(TopologyDTO.CommodityType.newBuilder().setKey(key));
            }
            return this;
        }

        CommoditySoldBuilder withCapacity(final double capacity) {
            builder.setCapacity(capacity);
            return this;
        }
    }

    static class CommodityBoughtBuilder {

        private CommodityBoughtDTO.Builder builder;

        private CommodityBoughtBuilder() {
            builder = CommodityBoughtDTO.newBuilder();
        }

        static CommodityBoughtBuilder newBuilder() {
            return new CommodityBoughtBuilder();
        }

        CommodityBoughtDTO build() {
            return builder.build();
        }

        CommodityBoughtDTO.Builder getBuilder() {
            return builder;
        }

        CommodityBoughtBuilder withType(final int type) {
            if (builder.hasCommodityType()) {
                final TopologyDTO.CommodityType current = builder.getCommodityType();
                builder.setCommodityType(current.toBuilder().setType(type));
            } else {
                builder.setCommodityType(
                    TopologyDTO.CommodityType.newBuilder().setType(type));
            }
            return this;
        }

        CommodityBoughtBuilder withKey(@Nonnull final String key) {
            if (builder.hasCommodityType()) {
                final TopologyDTO.CommodityType current = builder.getCommodityType();
                builder.setCommodityType(current.toBuilder().setKey(key));
            } else {
                builder.setCommodityType(TopologyDTO.CommodityType.newBuilder().setKey(key));
            }
            return this;
        }
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()))
            .build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                                              final double capacity, @Nonnull final String key) {
        return CommoditySoldDTO.newBuilder()
            .setCapacity(capacity)
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setKey(key)
                .setType(type.getNumber()))
            .build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                    @Nonnull final String key) {
        return CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setKey(key)
                            .setType(type.getNumber()))
                        .build();
    }

    static CommodityBoughtDTO makeCommodityBought(@Nonnull final CommodityType type,
                                              @Nonnull final String key) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setKey(key)
                .setType(type.getNumber()))
            .build();
    }

    static CommodityBoughtDTO makeCommodityBought(@Nonnull final CommodityType type) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()))
            .build();
    }

    static CommodityBoughtDTO.Builder makeCommodityBoughtBuilder(@Nonnull final CommodityType type) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()));
    }

    static Setting makeNumericSetting(final float value) {
        return Setting.newBuilder()
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value).build())
            .build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                                              final double capacity) {
        return CommoditySoldDTO.newBuilder()
            .setCapacity(capacity)
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()))
            .build();
    }
}
