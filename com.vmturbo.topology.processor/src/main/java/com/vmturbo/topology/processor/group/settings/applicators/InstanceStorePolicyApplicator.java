/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.mediation.util.units.MemoryUnit;
import com.vmturbo.mediation.util.units.ValueWithUnitFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator.SingleSettingApplicator;

/**
 * {@link InstanceStorePolicyApplicator} provides a common implementation of the instance store
 * policy setting application. Creates three commodities:
 * <ul>
 *     <li>Disk number;</li>
 *     <li>Single disk capacity;</li>
 *     <li>Access commodity for disk type.</li>
 * </ul>
 *
 * @param <B> type of the commodity builder which will be used to add commodities to
 *                 entity.
 */
public abstract class InstanceStorePolicyApplicator<B> extends SingleSettingApplicator {
    private static final Collection<Integer> INSTANCE_COMMODITY_TYPES =
                    ImmutableSet.of(CommodityDTO.CommodityType.INSTANCE_DISK_SIZE_VALUE,
                                    CommodityDTO.CommodityType.NUM_DISK_VALUE,
                                    CommodityDTO.CommodityType.INSTANCE_DISK_TYPE_VALUE);
    private static final Collection<Predicate<ComputeTierInfo>> INSTANCE_STORE_PREDICATES =
                    ImmutableSet.of(ComputeTierInfo::hasInstanceDiskType,
                                    ComputeTierInfo::hasNumInstanceDisks,
                                    ComputeTierInfo::hasInstanceDiskSizeGb);
    private final Logger logger = LogManager.getLogger(getClass());

    private final EntityType entityType;
    private final Supplier<B> builderCreator;
    private final BiConsumer<B, CommodityType> commodityTypeSetter;
    private final BiConsumer<B, Number> valueSetter;
    private final Function<B, B> builderTransformer;
    private final Function<B, Integer> commodityTypeGetter;

    /**
     * Creates {@link InstanceStorePolicyApplicator} instance.
     *
     * @param entityType type of the entity for which this applicator should work.
     * @param builderCreator creates commodity builder that will be added to the
     *                 entity.
     * @param commodityTypeSetter sets appropriate commodity type for the specific
     *                 commodity builder instance.
     * @param valueSetter sets value for the specific commodity builder instance.
     * @param builderTransformer transforms commodity builder after its creation,
     *                 should do operations specific to descendant applicators.
     * @param commodityTypeGetter extracts information about commodity type.
     */
    protected InstanceStorePolicyApplicator(@Nonnull EntityType entityType,
                    @Nonnull Supplier<B> builderCreator,
                    @Nonnull BiConsumer<B, CommodityType> commodityTypeSetter,
                    @Nonnull BiConsumer<B, Number> valueSetter,
                    @Nonnull Function<B, B> builderTransformer,
                    @Nonnull Function<B, Integer> commodityTypeGetter) {
        super(EntitySettingSpecs.InstanceStoreAwareScaling);
        this.entityType = Objects.requireNonNull(entityType);
        this.builderCreator = Objects.requireNonNull(builderCreator);
        this.commodityTypeSetter = Objects.requireNonNull(commodityTypeSetter);
        this.valueSetter = Objects.requireNonNull(valueSetter);
        this.builderTransformer = Objects.requireNonNull(builderTransformer);
        this.commodityTypeGetter = Objects.requireNonNull(commodityTypeGetter);
    }

    /**
     * Applies setting for the specified entity. In case the setting is applicable for the entity.
     *
     * @param entity for which we want to try to apply the setting.
     * @param setting that we want to apply for the entity.
     */
    @Override
    protected void apply(@Nonnull Builder entity, @Nonnull Setting setting) {
        if (isApplicable(entity, setting)) {
            populateInstanceStoreCommodities(entity);
        }
    }

    /**
     * Checks whether specified setting is applicable for the entity.
     *
     * @param entity for which we want to try to apply the setting.
     * @param setting that we want to apply for the entity.
     * @return {@code true} in case setting is applicable for entity, otherwise returns
     *                 {@code false}.
     */
    protected boolean isApplicable(@Nonnull Builder entity, @Nonnull Setting setting) {
        // Check that setting is active and enabled
        if (!setting.hasBooleanSettingValue()) {
            return false;
        }
        if (!setting.getBooleanSettingValue().getValue()) {
            return false;
        }
        // Check whether entity has the proper type
        return entity.getEntityType() == entityType.getNumber();
    }

    /**
     * Checks whether entity has a compute tier info related to it or not.
     *
     * @param entity which we want to check availability of the compute tier info.
     * @return {@code true} in case entity has compute tier info associated with it,
     *                 otherwise returns {@code false}.
     */
    protected static boolean hasComputeTierInfo(@Nonnull Builder entity) {
        // Check that entity has type specific info
        if (!entity.hasTypeSpecificInfo()) {
            return false;
        }
        // Checks that entity has appropriate type specific info instance
        if (!entity.getTypeSpecificInfo().hasComputeTier()) {
            return false;
        }
        final ComputeTierInfo computeTier = entity.getTypeSpecificInfo().getComputeTier();
        return INSTANCE_STORE_PREDICATES.stream().allMatch(p -> p.test(computeTier));
    }

    /**
     * Adds commodities to the consumer, which should update collection in entityToUpdate instance.
     *
     * @param commoditiesExtractor function pointer to populate required commodities
     *                 collection in the entityToUpdate.
     * @param entityToUpdate entity which commodities collection need to be
     *                 updated.
     * @param commodityAdder adds commodities to entity which we are going to update.
     * @param computeTierInfo information about computer tier which will provide
     *                 information required for instance store commodity creation process.
     * @param <E> type of the entity which commodities collection will be updated.
     */
    protected <E> void addCommodities(@Nonnull Function<E, Collection<B>> commoditiesExtractor,
                    @Nonnull BiConsumer<E, B> commodityAdder,
                    @Nonnull E entityToUpdate, @Nonnull ComputeTierInfo computeTierInfo) {
        final Collection<B> commodities = commoditiesExtractor.apply(entityToUpdate);
        if (commodities.stream().anyMatch(c -> INSTANCE_COMMODITY_TYPES
                        .contains(commodityTypeGetter.apply(c)))) {
            return;
        }
        getInstanceStoreDiskSize(computeTierInfo).ifPresent(value -> commodityAdder
                        .accept(entityToUpdate, createCommodityBuilder(
                                        CommodityDTO.CommodityType.INSTANCE_DISK_SIZE, null,
                                        value.doubleValue())));
        getNumInstanceDisks(computeTierInfo).ifPresent(value -> commodityAdder
                        .accept(entityToUpdate,
                                        createCommodityBuilder(CommodityDTO.CommodityType.NUM_DISK,
                                                        null, value.doubleValue())));
        getInstanceStoreDiskType(computeTierInfo).ifPresent(value -> commodityAdder
                        .accept(entityToUpdate, createCommodityBuilder(
                                        CommodityDTO.CommodityType.INSTANCE_DISK_TYPE, value,
                                        null)));
    }

    /**
     * Populates instance store related commodities into the entity.
     *
     * @param entity which should be populated with instance store related
     *                 commodities.
     */
    protected abstract void populateInstanceStoreCommodities(@Nonnull Builder entity);

    /**
     * Returns logger instance available for applicator.
     *
     * @return logger instance.
     */
    @Nonnull
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Returns size of the disk, extracted from {@link ComputeTierInfo} instance.
     *
     * @param computeTierInfo which reflects template information.
     * @return size of the disk.
     */
    private static Optional<? extends Number> getInstanceStoreDiskSize(
                    @Nonnull ComputeTierInfo computeTierInfo) {
        if (computeTierInfo.hasInstanceDiskSizeGb()) {
            return Optional.of(computeTierInfo.getInstanceDiskSizeGb())
                            .map(sizeGb -> ValueWithUnitFactory.gigaBytes(sizeGb)
                                            .convertTo(MemoryUnit.MegaByte));
        }
        return Optional.empty();
    }

    /**
     * Returns number of the disks, available for the {@link ComputeTierInfo} instance.
     *
     * @param computeTierInfo which reflects template information.
     * @return number of the disks, available for template.
     */
    @Nonnull
    private static Optional<? extends Number> getNumInstanceDisks(
                    @Nonnull ComputeTierInfo computeTierInfo) {
        if (computeTierInfo.hasNumInstanceDisks()) {
            return Optional.of(computeTierInfo.getNumInstanceDisks());
        }
        return Optional.empty();
    }

    /**
     * Returns disk type, available for the {@link ComputeTierInfo} instance.
     *
     * @param computeTierInfo which reflects template information.
     * @return disk type, available for template.
     */
    @Nonnull
    private static Optional<String> getInstanceStoreDiskType(
                    @Nonnull ComputeTierInfo computeTierInfo) {
        final Optional<InstanceDiskType> result = computeTierInfo.hasInstanceDiskType() ?
                        Optional.of(computeTierInfo.getInstanceDiskType()) :
                        Optional.empty();
        return result.map(Enum::name);
    }

    /**
     * Creates commodity builder with populated commodity type, key and value(which could be
     * usage/capacity depending on the implementation).
     *
     * @param commodityType type of the commodity which we want to create.
     * @param key key for the commodity.
     * @param value that will be set as usage/capacity
     * @return commodity builder with populated fields.
     */
    @Nonnull
    private B createCommodityBuilder(@Nonnull CommodityDTO.CommodityType commodityType,
                    @Nullable String key, @Nullable Number value) {
        final B builder = builderCreator.get();
        commodityTypeSetter.accept(builder, createCommodityType(commodityType, key));
        if (value != null) {
            valueSetter.accept(builder, value);
        }
        return builderTransformer.apply(builder);
    }

    @Nonnull
    private static CommodityType createCommodityType(
                    @Nonnull CommodityDTO.CommodityType commodityType, @Nullable String key) {
        final CommodityType.Builder builder =
                        CommodityType.newBuilder().setType(commodityType.getNumber());
        if (StringUtils.isNotBlank(key)) {
            builder.setKey(key);
        }
        return builder.build();
    }

}
