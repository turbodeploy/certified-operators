package com.vmturbo.stitching.poststitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.junit.Assert;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoView;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonPOJO.EntityImpl.VirtualVolumeDataImpl.VirtualVolumeFileDescriptorImpl;
import com.vmturbo.platform.common.dto.CommonPOJO.EntityImpl.VirtualVolumeDataImpl.VirtualVolumeFileDescriptorView;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
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

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldView> commoditiesSold) {
        return TopologyEntityBuilder.newBuilder().withCommoditiesSold(commoditiesSold).build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                         @Nonnull final List<CommoditySoldView> commoditiesSold,
                                         @Nonnull final List<CommodityBoughtView> commoditiesBought,
                                         @Nonnull final List<TopologyEntity.Builder> providers) {
        return TopologyEntityBuilder.newBuilder()
                .withEntityType(entityType)
                .withCommoditiesBought(commoditiesBought)
                .withCommoditiesSold(commoditiesSold)
                .withProviders(providers)
                .build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                         final String name, final long oid,
                                         final List<TopologyEntity.Builder> providers) {
        final TopologyEntityBuilder a = TopologyEntityBuilder.newBuilder().withEntityType(entityType).withProviders(providers);
        a.builder.getTopologyEntityImpl().setDisplayName(name).setOid(oid);
        return a.build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                         @Nonnull final List<CommoditySoldView> commoditiesSold,
                                         @Nonnull final Set<CommoditiesBoughtFromProviderView> commoditiesBoughtFromProvider,
                                         @Nonnull final List<TopologyEntity.Builder> providers) {
        return TopologyEntityBuilder.newBuilder()
                .withEntityType(entityType)
                .withCommoditiesSold(commoditiesSold)
                .withCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider)
                .withProviders(providers)
                .build();
    }

    static TopologyEntity makeTopologyEntity(final int entityType,
                                         @Nonnull final List<CommoditySoldView> commoditiesSold) {
        return TopologyEntity.newBuilder(new TopologyEntityImpl().setEntityType(entityType)
            .addAllCommoditySoldList(commoditiesSold)).build();
    }

    static TopologyEntity makeTopologyEntity(@Nonnull final List<CommoditySoldView> commoditiesSold,
                                         @Nonnull final TypeSpecificInfoView typeSpecificInfo) {
        TopologyEntityBuilder builder = TopologyEntityBuilder.newBuilder()
            .withCommoditiesSold(commoditiesSold);
        builder.getBuilder().getTopologyEntityImpl().setTypeSpecificInfo(typeSpecificInfo);
        return builder.build();
    }

    static TopologyEntity makeTopologyEntity(final long oid,
                                         final int entityType,
                                         @Nonnull final List<CommoditySoldView> commoditiesSold,
                                         @Nonnull final Map<String, String> propertyMap) {
        return TopologyEntityBuilder.newBuilder()
                .withOid(oid)
                .withEntityType(entityType)
                .withCommoditiesSold(commoditiesSold)
                .withProperties(propertyMap)
                .build();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
            @Nonnull final List<CommoditySoldView> commoditiesSold,
            @Nonnull final List<CommodityBoughtView> commoditiesBought) {
        return TopologyEntityBuilder.newBuilder()
            .withEntityType(entityType)
            .withCommoditiesBought(commoditiesBought)
            .withCommoditiesSold(commoditiesSold)
            .getBuilder();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
                                                            String name, long oid) {
        final TopologyEntityBuilder a = TopologyEntityBuilder.newBuilder()
                .withEntityType(entityType);
        a.builder.getTopologyEntityImpl().setOid(oid).setDisplayName(name);
        return a.getBuilder();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final int entityType,
            @Nonnull final List<CommoditySoldView> commoditiesSold,
            @Nonnull final Set<CommoditiesBoughtFromProviderView> commoditiesBoughtFromProvider) {
        return TopologyEntityBuilder.newBuilder()
            .withEntityType(entityType)
            .withCommoditiesSold(commoditiesSold)
            .withCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider)
            .getBuilder();
    }

    public static TopologyEntity.Builder makeTopologyEntityBuilder(
            final long oid, final int entityType,
            @Nonnull final List<CommoditySoldView> commoditiesSold,
            @Nonnull final List<CommodityBoughtView> commoditiesBought) {
        return TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setOid(oid)
                .setEntityType(entityType)
                .addAllCommoditySoldList(commoditiesSold)
                .addCommoditiesBoughtFromProviders(new CommoditiesBoughtFromProviderImpl()
                    .addAllCommodityBought(commoditiesBought)
                ));
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
            @Nonnull final List<CommoditySoldView> commoditiesSold,
            @Nonnull final Set<CommoditiesBoughtFromProviderView> commoditiesBoughtFromProvider) {
        return TopologyEntityBuilder.newBuilder()
            .withOid(oid)
            .withEntityType(entityType)
            .withCommoditiesSold(commoditiesSold)
            .withCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider)
            .getBuilder();
    }

    static TopologyEntity.Builder makeTopologyEntityBuilder(final long oid, final int entityType,
                    @Nonnull final List<CommoditySoldView> commoditiesSold,
                    @Nonnull final Map<Long, List<CommodityBoughtView>> commoditiesBoughtMap) {
        return TopologyEntityBuilder.newBuilder()
            .withOid(oid)
            .withEntityType(entityType)
            .withCommoditiesSold(commoditiesSold)
            .withCommoditiesBoughtFromProviders(commoditiesBoughtMap)
            .getBuilder();
    }

    /**
     * For easily creating a TopologyEntity or TopologyEntityImpl with any attributes.
     */
    static class TopologyEntityBuilder {
        private final TopologyEntity.Builder builder;

        private final TopologyEntityImpl innerEntity;

        private TopologyEntityBuilder() {
            innerEntity = new TopologyEntityImpl();
            builder = TopologyEntity.newBuilder(innerEntity);
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
            innerEntity.setOid(oid);
            return this;
        }

        TopologyEntityBuilder withCommoditiesSold(@Nonnull final CommoditySoldView... commodities) {
            return withCommoditiesSold(Arrays.asList(commodities));
        }

        TopologyEntityBuilder withCommoditiesSold(@Nonnull final CommoditySoldBuilder... commodities) {
            for (final CommoditySoldBuilder commodity : commodities) {
                innerEntity.addCommoditySoldList(commodity.build());
            }
            return this;
        }

        TopologyEntityBuilder withCommoditiesSold(@Nonnull final Collection<CommoditySoldView> commodities) {
            innerEntity.addAllCommoditySoldList(commodities);
            return this;
        }

        TopologyEntityBuilder withEnvironmentType(@Nonnull final EnvironmentType environmentType) {
            innerEntity.setEnvironmentType(environmentType);
            return this;
        }

        TopologyEntityBuilder withEntityType(final int type) {
            innerEntity.setEntityType(type);
            return this;
        }

        TopologyEntityBuilder withCommoditiesBought(
            @Nonnull final Collection<CommodityBoughtView> commodities) {

            innerEntity.addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl().addAllCommodityBought(commodities));
            return this;
        }

        TopologyEntityBuilder withCommoditiesBoughtFromProviders(
            @Nonnull final Map<Long, List<CommodityBoughtView>> commodities) {

            commodities.entrySet().stream().map(entry ->
                new CommoditiesBoughtFromProviderImpl().setProviderId(entry.getKey())
                    .addAllCommodityBought(entry.getValue())
            ).forEach(innerEntity::addCommoditiesBoughtFromProviders);

            return this;
        }

        TopologyEntityBuilder withCommoditiesBoughtFromProviders(
            @Nonnull final Collection<CommoditiesBoughtFromProviderView> commodities) {
            innerEntity.addAllCommoditiesBoughtFromProviders(commodities);
            return this;
        }

        TopologyEntityBuilder withProviders(@Nonnull final List<TopologyEntity.Builder> providers) {
            for (final TopologyEntity.Builder provider : providers) {
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

        TopologyEntityBuilder withConsumers(@Nonnull final List<TopologyEntity.Builder> consumers) {
            for (final TopologyEntity.Builder consumer : consumers) {
                builder.addConsumer(consumer);
            }
            return this;
        }

        TopologyEntityBuilder withConsumers(@Nonnull final TopologyEntityBuilder... consumers) {
            for (final TopologyEntityBuilder consumer : consumers) {
                builder.addConsumer(consumer.getBuilder());
            }
            return this;
        }

        TopologyEntityBuilder withProperty(@Nonnull final String key, @Nonnull final String value) {
            innerEntity.putEntityPropertyMap(key, value);
            return this;
        }

        TopologyEntityBuilder withProperties(@Nonnull final Map<String, String> properties) {
            innerEntity.putAllEntityPropertyMap(properties);
            return this;
        }

        TopologyEntityBuilder withTypeSpecificInfo(@Nonnull final TypeSpecificInfoImpl info) {
            innerEntity.setTypeSpecificInfo(info);
            return this;
        }

    }

    /**
     * For easily creating a CommoditySoldDTO or CommoditySoldDTO.Builder with any attributes.
     */
    static class CommoditySoldBuilder {
        private CommoditySoldImpl impl;

        private CommoditySoldBuilder() {
            impl = new CommoditySoldImpl();
        }

        CommoditySoldView build() {
            return impl;
        }

        static CommoditySoldBuilder newBuilder() {
            return new CommoditySoldBuilder();
        }

        CommoditySoldBuilder withType(@Nonnull final CommodityType type) {
            impl.getOrCreateCommodityType().setType(type.getNumber());
            return this;
        }

        CommoditySoldBuilder withKey(@Nonnull final String key) {
            impl.getOrCreateCommodityType().setKey(key);
            return this;
        }

        CommoditySoldBuilder withCapacity(final double capacity) {
            impl.setCapacity(capacity);
            return this;
        }

        CommoditySoldBuilder withUsed(final double used) {
            impl.setUsed(used);
            return this;
        }
    }

    /**
     * For easily creating a CommodityBoughtDTO or CommodityBoughtImpl with any attributes.
     */
    static class CommodityBoughtBuilder {

        private CommodityBoughtImpl commBought;

        private CommodityBoughtBuilder() {
            commBought = new CommodityBoughtImpl();
        }

        static CommodityBoughtBuilder newBuilder() {
            return new CommodityBoughtBuilder();
        }

        CommodityBoughtView build() {
            return commBought;
        }

        CommodityBoughtImpl getImpl() {
            return commBought;
        }

        CommodityBoughtBuilder withType(final int type) {
            commBought.getOrCreateCommodityType().setType(type);
            return this;
        }

        CommodityBoughtBuilder withKey(@Nonnull final String key) {
            commBought.getOrCreateCommodityType().setKey(key);
            return this;
        }

        CommodityBoughtBuilder withUsed(final double used) {
            commBought.setUsed(used);
            return this;
        }
    }

    public static CommoditySoldView makeCommoditySold(@Nonnull final CommodityType type) {
        return new CommoditySoldImpl()
            .setCommodityType(new CommodityTypeImpl()
                .setType(type.getNumber()));
    }

    static CommoditySoldView makeCommoditySold(@Nonnull final CommodityType type,
                                              final double capacity, @Nonnull final String key) {
        return CommoditySoldBuilder.newBuilder()
            .withType(type).withKey(key).withCapacity(capacity).build();
    }

    static CommoditySoldView makeCommoditySold(@Nonnull final CommodityType type,
                    @Nonnull final String key) {
        return CommoditySoldBuilder.newBuilder().withType(type).withKey(key).build();
    }

    static CommoditySoldView makeCommoditySold(@Nonnull final CommodityType type,
        final double capacity) {
        return CommoditySoldBuilder.newBuilder().withCapacity(capacity).withType(type).build();
    }

    static CommoditySoldView makeCommoditySold(@Nonnull final CommodityType type,
            @Nonnull final String key, final double used) {
        return CommoditySoldBuilder.newBuilder().withType(type).withKey(key).withUsed(used).build();
    }

    static CommodityBoughtView makeCommodityBought(@Nonnull final CommodityType type,
                                              @Nonnull final String key) {
        return CommodityBoughtBuilder.newBuilder().withType(type.getNumber()).withKey(key).build();
    }

    static CommodityBoughtView makeCommodityBought(@Nonnull final CommodityType type) {
        return CommodityBoughtBuilder.newBuilder().withType(type.getNumber()).build();
    }

    static CommodityBoughtView makeCommodityBought(@Nonnull final CommodityType type,
                                                  final double used) {
        return CommodityBoughtBuilder.newBuilder().withType(type.getNumber()).withUsed(used).build();
    }

    static CommoditiesBoughtFromProviderView makeCommoditiesBoughtFromProvider(final long providerId,
                                                                               final int providerType,
                                                                               @Nonnull final List<CommodityBoughtView> commodities) {
        return new CommoditiesBoughtFromProviderImpl()
            .setProviderId(providerId)
            .setProviderEntityType(providerType)
            .addAllCommodityBought(commodities);
    }



    public static CommodityBoughtImpl makeCommodityBoughtBuilder(@Nonnull final CommodityType type) {
        return new CommodityBoughtImpl()
            .setCommodityType(new CommodityTypeImpl()
                .setType(type.getNumber()));
    }

    static Setting makeNumericSetting(final float value) {
        return Setting.newBuilder()
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value).build())
            .build();
    }

    static Setting makeBooleanSetting(final boolean value) {
        return Setting.newBuilder()
                .setBooleanSettingValue(
                        BooleanSettingValue.newBuilder().setValue(value).build())
                .build();
    }

    private static Collection<VirtualVolumeFileDescriptorView> createFiles(
            @Nonnull String[] pathNames, @Nonnull long[] sizes) {
        Assert.assertEquals(pathNames.length, sizes.length);
        final List<VirtualVolumeFileDescriptorView> retVal = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++) {
            retVal.add(new VirtualVolumeFileDescriptorImpl()
                    .setPath(pathNames[i])
                    .setSizeKb(sizes[i]));
        }
        return retVal;
    }

    /**
     * Add VirtualVolumeFileDescriptors corresponding to a list of path names to a VirtualVolume.
     *
     * @param builder   A builder for the VirtualVolume's TopologyEntity
     * @param pathNames A String array with the pathnames of the files to add.
     * @param fileSizes A Long array with the sizes in KB of the files in pathNames.
     */
    static void addFilesToVirtualVolume(
            @Nonnull TopologyEntity.Builder builder,
            @Nonnull String[] pathNames,
            @Nonnull long[] fileSizes) {
        builder.getTopologyEntityImpl().setTypeSpecificInfo(new TypeSpecificInfoImpl()
            .setVirtualVolume(
                builder.getTopologyEntityImpl()
                    .getTypeSpecificInfo()
                    .getVirtualVolume()
                    .copy()
                    .addAllFiles(createFiles(pathNames, fileSizes))));
    }

    /**
     * Add VirtualVolumeFileDescriptors corresponding to a list of path names to a VirtualVolume.
     *
     * @param builder   A builder for the VirtualVolume's TopologyEntity
     * @param pathNames A String array with the pathnames of the files to add.
     */
    static void addFilesToVirtualVolume(
            @Nonnull TopologyEntity.Builder builder,
            @Nonnull String[] pathNames) {
        long[] fileSizes = new long[pathNames.length];
        // Default to file sizes bigger than default minimum for wasted files
        Arrays.fill(fileSizes, (long) EntitySettingSpecs.MinWastedFilesSize.getNumericDefault() + 1);
        addFilesToVirtualVolume(builder, pathNames, fileSizes);
    }



    /**
     * Add VirtualVolumeFileDescriptors corresponding to a list of path names to a VirtualVolume.
     *
     * @param builder   A builder for the VirtualVolume's TopologyEntity
     * @param path The pathname of the file to add.
     * @param links An array of alternative paths to the same file as path.
     */
    static void addFileToVirtualVolume(@Nonnull TopologyEntity.Builder builder,
                                               @Nonnull String path, @Nonnull String[] links) {
        builder.getTopologyEntityImpl().setTypeSpecificInfo(new TypeSpecificInfoImpl()
            .setVirtualVolume(
                builder.getTopologyEntityImpl()
                    .getTypeSpecificInfo()
                    .getVirtualVolume()
                    .copy()
                    .addAllFiles(Collections.singletonList(
                        new VirtualVolumeFileDescriptorImpl().setPath(path)
                            .setSizeKb((long) EntitySettingSpecs.MinWastedFilesSize
                                    .getNumericDefault() + 1)
                            .addAllLinkedPaths(Arrays.asList(links)))
                    )));
    }
}
