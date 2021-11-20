/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.mediation.util.units.MemoryUnit;
import com.vmturbo.mediation.util.units.ValueWithUnitFactory;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;

/**
 * {@link InstanceStoreCommoditiesCreator} provides a common implementation of the instance store
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
public abstract class InstanceStoreCommoditiesCreator<B> {
    /**
     * Checks whether {@link ComputeTierInfo} instance has information about instance disk type.
     */
    protected static final Predicate<ComputeTierInfo> INSTANCE_DISK_TYPE_PREDICATE =
                    (cti) -> cti.hasInstanceDiskType()
                                    && cti.getInstanceDiskType() != InstanceDiskType.NONE;
    /**
     * Checks whether {@link ComputeTierInfo} instance has information about number of instance
     * disks.
     */
    protected static final Predicate<ComputeTierInfo> INSTANCE_DISK_COUNTS_PREDICATE =
                    (cti) -> cti.getInstanceDiskCountsCount() > 0;
    /**
     * Checks whether {@link ComputeTierInfo} instance has information about instance disk size.
     */
    protected static final Predicate<ComputeTierInfo> INSTANCE_DISK_SIZE_PREDICATE =
                    (cti) -> cti.hasInstanceDiskSizeGb() && cti.getInstanceDiskSizeGb() > 0;
    private static final Collection<Integer> INSTANCE_COMMODITY_TYPES =
                    ImmutableSet.of(CommodityDTO.CommodityType.INSTANCE_DISK_SIZE_VALUE,
                                    CommodityDTO.CommodityType.INSTANCE_DISK_TYPE_VALUE);

    private final Supplier<B> builderCreator;
    private final BiConsumer<B, CommodityType> commodityTypeSetter;
    private final BiConsumer<B, Number> valueSetter;
    private final Function<B, B> builderTransformer;
    private final Function<B, Integer> commodityTypeGetter;

    /**
     * Creates {@link InstanceStoreCommoditiesCreator} instance.
     *
     * @param builderCreator creates commodity builder that will be added to the
     *                 entity.
     * @param commodityTypeSetter sets appropriate commodity type for the specific
     *                 commodity builder instance.
     * @param valueSetter sets value for the specific commodity builder instance.
     * @param builderTransformer transforms commodity builder after its creation,
     *                 should do operations specific to descendant applicators.
     * @param commodityTypeGetter extracts information about commodity type.
     */
    protected InstanceStoreCommoditiesCreator(@Nonnull Supplier<B> builderCreator,
                    @Nonnull BiConsumer<B, CommodityType> commodityTypeSetter,
                    @Nonnull BiConsumer<B, Number> valueSetter,
                    @Nonnull Function<B, B> builderTransformer,
                    @Nonnull Function<B, Integer> commodityTypeGetter) {
        this.builderCreator = Objects.requireNonNull(builderCreator);
        this.commodityTypeSetter = Objects.requireNonNull(commodityTypeSetter);
        this.valueSetter = Objects.requireNonNull(valueSetter);
        this.builderTransformer = Objects.requireNonNull(builderTransformer);
        this.commodityTypeGetter = Objects.requireNonNull(commodityTypeGetter);
    }

    /**
     * Adds commodities to the consumer, which should update collection in entityToUpdate instance.
     *
     * @param commoditiesExtractor function pointer to populate required commodities
     *                 collection in the entityToUpdate.
     * @param entityToUpdate entity which commodities collection need to be
     *                 updated.
     * @param computeTierInfo information about computer tier which will provide
     *                 information required for instance store commodity creation process.
     * @param <E> type of the entity which commodities collection will be updated.
     * @param usedEphemeralDisks If non null (for VMs that currently have attached ephemeral disks),
     *      this has the count of such disks, and are used for creating VM's bought commodities.
     *      For sold commodity creation, this is *always* null, we use the tier's supported disk
     *      counts to create the commodities.
     * @return number of commodity builders that have been created.
     */
    @Nonnull
    protected <E> Collection<B> create(@Nonnull Function<E, Collection<B>> commoditiesExtractor,
                    @Nonnull E entityToUpdate, @Nonnull ComputeTierInfo computeTierInfo,
            @Nullable final Integer usedEphemeralDisks) {
        final Collection<B> commodities = commoditiesExtractor.apply(entityToUpdate);
        if (commodities.stream().anyMatch(c -> INSTANCE_COMMODITY_TYPES
                        .contains(commodityTypeGetter.apply(c)))) {
            return Collections.emptySet();
        }
        final Collection<B> result = new HashSet<>();
        getInstanceStoreDiskSize(computeTierInfo).ifPresent(value -> result
                        .add(createCommodityBuilder(CommodityDTO.CommodityType.INSTANCE_DISK_SIZE,
                                        null, value.doubleValue())));
        if (usedEphemeralDisks != null) {
            // When creating bought commodities, create it based on count of disks actually used,
            // rather than what the compute tier supports (which could be different for GCP).
            result.add(createCommodityBuilder(CommodityDTO.CommodityType.INSTANCE_DISK_COUNT,
                    String.valueOf(usedEphemeralDisks), usedEphemeralDisks.doubleValue()));
        } else {
            // Creating sold commodities, create based on all distinct disk counts that are
            // supported by the tier. E.g '2, 4, 8' for GCP.
            getInstanceDiskCounts(computeTierInfo).forEach(value -> result.add(
                    createCommodityBuilder(CommodityDTO.CommodityType.INSTANCE_DISK_COUNT,
                            String.valueOf(value), SDKConstants.ACCESS_COMMODITY_CAPACITY)));
        }
        getInstanceStoreDiskType(computeTierInfo).ifPresent(value -> result
                        .add(createCommodityBuilder(CommodityDTO.CommodityType.INSTANCE_DISK_TYPE,
                                        value, SDKConstants.ACCESS_COMMODITY_CAPACITY)));
        return Collections.unmodifiableCollection(result);
    }

    /**
     * Returns size of the disk, extracted from {@link ComputeTierInfo} instance.
     *
     * @param computeTierInfo which reflects template information.
     * @return size of the disk.
     */
    private static Optional<? extends Number> getInstanceStoreDiskSize(
                    @Nonnull ComputeTierInfo computeTierInfo) {
        return Optional.of(computeTierInfo).filter(INSTANCE_DISK_SIZE_PREDICATE)
                        .map(ComputeTierInfo::getInstanceDiskSizeGb)
                        .map(sizeGb -> ValueWithUnitFactory.gigaBytes(sizeGb)
                                        .convertTo(MemoryUnit.MegaByte));
    }

    /**
     * Returns number of the disks, available for the {@link ComputeTierInfo} instance.
     * Normally 1 disk count is returned (e.g for AWS instance store), but could be multiple counts
     * as well as in case of GCP.
     *
     * @param computeTierInfo which reflects template information.
     * @return number of the disks, available for template.
     */
    @Nonnull
    private static List<Integer> getInstanceDiskCounts(
                    @Nonnull ComputeTierInfo computeTierInfo) {
        return Optional.of(computeTierInfo).filter(INSTANCE_DISK_COUNTS_PREDICATE)
                        .map(ComputeTierInfo::getInstanceDiskCountsList)
                .orElse(Collections.emptyList());
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
        return Optional.of(computeTierInfo).filter(INSTANCE_DISK_TYPE_PREDICATE)
                        .map(ComputeTierInfo::getInstanceDiskType).map(Enum::name);
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
