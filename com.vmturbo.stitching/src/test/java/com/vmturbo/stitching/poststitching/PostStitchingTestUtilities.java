package com.vmturbo.stitching.poststitching;

import java.util.Arrays;
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
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.stitching.journal.IStitchingJournal;

/**
 * Utility class to create test instances used in post-stitching.
 *
 * TODO: replace make______() methods with builders in all tests
 */
public class PostStitchingTestUtilities {

    private  PostStitchingTestUtilities() {}

    /**
     * Result builder subclass for use in post-stitching unit tests.
     */
    static class UnitTestResultBuilder extends EntityChangesBuilder<TopologyEntity> {

        @Override
        public TopologicalChangelog<TopologyEntity> build() {
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
    private static class PostStitchingUnitTestChange implements TopologicalChange<TopologyEntity> {
        private final TopologyEntity entityToUpdate;
        private final Consumer<TopologyEntity> updateMethod;

        PostStitchingUnitTestChange(@Nonnull final TopologyEntity entityToUpdate,
                                    @Nonnull final Consumer<TopologyEntity> updateMethod) {
            this.entityToUpdate = Objects.requireNonNull(entityToUpdate);
            this.updateMethod = Objects.requireNonNull(updateMethod);
        }

        @Override
        public void applyChange(@Nonnull final IStitchingJournal stitchingJournal) {
            updateMethod.accept(entityToUpdate);
        }
    }

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldDTO> commoditiesSold) {
        return TopologyEntityBuilder.newBuilder().withCommoditiesSold(commoditiesSold).build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
            @Nonnull final List<CommodityBoughtDTO> commoditiesBought,
            @Nonnull final List<TopologyEntity.Builder> providers) {
        return TopologyEntityBuilder.newBuilder()
                .withEntityType(entityType)
                .withCommoditiesBought(commoditiesBought)
                .withCommoditiesSold(commoditiesSold)
                .withProviders(providers)
                .build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
            @Nonnull final Set<CommoditiesBoughtFromProvider> commoditiesBoughtFromProvider,
            @Nonnull final List<TopologyEntity.Builder> providers) {
        return TopologyEntityBuilder.newBuilder()
                .withEntityType(entityType)
                .withCommoditiesSold(commoditiesSold)
                .withCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider)
                .withProviders(providers)
                .build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
            @Nonnull final List<CommoditySoldDTO> commoditiesSold) {
        return TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder().setEntityType(entityType)
            .addAllCommoditySoldList(commoditiesSold)).build();
    }

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                             @Nonnull final Map<String, String> propertyMap) {
        return TopologyEntityBuilder.newBuilder()
            .withCommoditiesSold(commoditiesSold)
            .withProperties(propertyMap)
            .build();
    }

    static TopologyEntity makeTopologyEntity(final long oid,
                                             final int entityType,
                                             @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                             @Nonnull final Map<String, String> propertyMap) {
        return TopologyEntityBuilder.newBuilder()
                .withOid(oid)
                .withEntityType(entityType)
                .withCommoditiesSold(commoditiesSold)
                .withProperties(propertyMap)
                .build();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
            @Nonnull final List<CommodityBoughtDTO> commoditiesBought) {
        return TopologyEntityBuilder.newBuilder()
            .withEntityType(entityType)
            .withCommoditiesBought(commoditiesBought)
            .withCommoditiesSold(commoditiesSold)
            .getBuilder();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
            @Nonnull final List<CommoditySoldDTO> commoditiesSold,
            @Nonnull final Set<CommoditiesBoughtFromProvider> commoditiesBoughtFromProvider) {
        return TopologyEntityBuilder.newBuilder()
            .withEntityType(entityType)
            .withCommoditiesSold(commoditiesSold)
            .withCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider)
            .getBuilder();
    }

    public static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
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
            @Nonnull final Set<CommoditiesBoughtFromProvider> commoditiesBoughtFromProvider) {
        return TopologyEntityBuilder.newBuilder()
            .withOid(oid)
            .withEntityType(entityType)
            .withCommoditiesSold(commoditiesSold)
            .withCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider)
            .getBuilder();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
                    @Nonnull final List<CommoditySoldDTO> commoditiesSold,
                    @Nonnull final Map<Long, List<CommodityBoughtDTO>> commoditiesBoughtMap) {
        return TopologyEntityBuilder.newBuilder()
            .withOid(oid)
            .withEntityType(entityType)
            .withCommoditiesSold(commoditiesSold)
            .withCommoditiesBoughtFromProviders(commoditiesBoughtMap)
            .getBuilder();
    }

    /**
     * For easily creating a TopologyEntity or TopologyEntity.Builder with any attributes.
     */
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
            return withCommoditiesSold(Arrays.asList(commodities));
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
            @Nonnull final Collection<CommodityBoughtDTO> commodities) {

            innerBuilder.addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder().addAllCommodityBought(commodities));
            return this;
        }

        TopologyEntityBuilder withCommoditiesBoughtFromProviders(
            @Nonnull final Map<Long, List<CommodityBoughtDTO>> commodities) {

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

        TopologyEntityBuilder withProviders(@Nonnull final List<TopologyEntity.Builder> providers) {
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

        TopologyEntityBuilder withProperty(@Nonnull final String key, @Nonnull final String value) {
            innerBuilder.putEntityPropertyMap(key, value);
            return this;
        }

        TopologyEntityBuilder withProperties(@Nonnull final Map<String, String> properties) {
            innerBuilder.putAllEntityPropertyMap(properties);
            return this;
        }

    }

    /**
     * For easily creating a CommoditySoldDTO or CommoditySoldDTO.Builder with any attributes.
     */
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

    /**
     * For easily creating a CommodityBoughtDTO or CommodityBoughtDTO.Builder with any attributes.
     */
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

    public static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()))
            .build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                                              final double capacity, @Nonnull final String key) {
        return CommoditySoldBuilder.newBuilder()
            .withType(type).withKey(key).withCapacity(capacity).build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
                    @Nonnull final String key) {
        return CommoditySoldBuilder.newBuilder().withType(type).withKey(key).build();
    }

    static CommoditySoldDTO makeCommoditySold(@Nonnull final CommodityType type,
        final double capacity) {
        return CommoditySoldBuilder.newBuilder().withCapacity(capacity).withType(type).build();
    }

    static CommodityBoughtDTO makeCommodityBought(@Nonnull final CommodityType type,
                                              @Nonnull final String key) {
        return CommodityBoughtBuilder.newBuilder().withType(type.getNumber()).withKey(key).build();
    }

    static CommodityBoughtDTO makeCommodityBought(@Nonnull final CommodityType type) {
        return CommodityBoughtBuilder.newBuilder().withType(type.getNumber()).build();
    }

    static CommoditiesBoughtFromProvider makeCommoditiesBoughtFromProvider(final long providerId,
                                                                            final int providerType,
                                                                            @Nonnull final List<CommodityBoughtDTO> commodities) {
        return CommoditiesBoughtFromProvider.newBuilder().setProviderId(providerId)
                .setProviderEntityType(providerType)
                .addAllCommodityBought(commodities)
                .build();
    }



    public static CommodityBoughtDTO.Builder makeCommodityBoughtBuilder(@Nonnull final CommodityType type) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(type.getNumber()));
    }

    static Setting makeNumericSetting(final float value) {
        return Setting.newBuilder()
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value).build())
            .build();
    }
}
