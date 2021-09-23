package com.vmturbo.stitching.fabric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CommoditiesBought;

/**
 * Checks that {@link FabricPMStitchingOperation} works as expected.
 */
public class FabricPMStitchingOperationTest {
    private static final String FABRIC_DC_ID = "fabricDcId";
    private static final String HYPERVISOR_DC_ID = "hypervisorDcId";
    private static final String CHASSIS_ID = "chassisId";

    /**
     * Checks that in case there is no chassis then hypervisor host should keep commodities bought
     * from DC even for the case when DC is going to be removed by successful stitching.
     */
    @Test
    public void checkChassisAbsentHostConsumesFromHypervisorDc() {
        final StitchingEntity fabricDcSe = mockEntity(EntityType.DATACENTER, FABRIC_DC_ID);
        checkFabricPmStitching(fabricDcSe,
                        mockEntity(EntityType.DATACENTER, HYPERVISOR_DC_ID),
                        null,
                        ImmutableSet.of(CommodityType.DATACENTER, CommodityType.POWER), false);
    }

    /**
     * Checks that in case there is no chassis then hypervisor host should keep commodities bought
     * from DC.
     */
    @Test
    public void checkChassisAbsentHypervisorDcHasToBeKept() {
        final StitchingEntity fabricDcSe = mockEntity(EntityType.DATACENTER, FABRIC_DC_ID);
        checkFabricPmStitching(fabricDcSe,
                        mockEntity(EntityType.DATACENTER, HYPERVISOR_DC_ID),
                        null,
                        ImmutableSet.of(CommodityType.DATACENTER, CommodityType.POWER), true);
    }

    /**
     * Checks that in case there is chassis then hypervisor host should start to buy non-access
     * commodities from chassis and stop to buy those commodities from DC. DC should be removed as a
     * provider from hypervisor host.
     */
    @Test
    public void checksChassisPresentDcHasToBeRemoved() {
        final StitchingEntity fabricDcSe = mockEntity(EntityType.DATACENTER, FABRIC_DC_ID);
        checkFabricPmStitching(fabricDcSe, mockEntity(EntityType.DATACENTER, HYPERVISOR_DC_ID),
                        mockEntity(EntityType.CHASSIS, CHASSIS_ID), Collections.emptySet(), false);
    }

    /**
     * Checks that in case there is chassis and DC has to be kept have to switch non-access
     * commodities from DC to Chassis instance. DC has to be connected to Chassis. Hypervisor host
     * should keep access commodity bought from DC, because it will be used in merge DC policy.
     */
    @Test
    public void checksChassisPresentDcHasToBeKept() {
        final StitchingEntity chassis = mockEntity(EntityType.CHASSIS, CHASSIS_ID);
        final StitchingEntity fabricDcSe = mockEntity(EntityType.DATACENTER, FABRIC_DC_ID);
        final StitchingEntity hypervisorDcSe = mockEntity(EntityType.DATACENTER, HYPERVISOR_DC_ID);
        checkFabricPmStitching(fabricDcSe, hypervisorDcSe,
                        chassis,
                        Collections.singleton(CommodityType.DATACENTER), true);
        Mockito.verify(chassis, Mockito.times(1)).addConnectedTo(ConnectionType.NORMAL_CONNECTION,
                        Collections.singleton(hypervisorDcSe));
    }

    private static void checkFabricPmStitching(StitchingEntity fabricDcSe, StitchingEntity hypervisorDcSe,
                    @Nullable StitchingEntity chassis, Collection<CommodityType> commodityTypesFromDc,
                    boolean keepDCsAfterFabricStitching) {
        // WHEN
        final FabricPMStitchingOperation stitchingOperation =
                        new FabricPMStitchingOperation(keepDCsAfterFabricStitching);
        final StitchingChangesBuilder<StitchingEntity> changesBuilder = mockChangesBuilder();
        final StitchingEntity fabricPmSe = Mockito.mock(StitchingEntity.class);
        final StitchingEntity hypervisorPmSe = Mockito.mock(StitchingEntity.class);
        final List<CommodityDTO.Builder> boughtFromChassis = createBoughtCommodities(chassis, 70D);
        final Collection<StitchingEntity> fabricPmProviders;
        if (chassis != null) {
            final CommoditiesBought boughtFromChassisWrapper =
                            new CommoditiesBought(boughtFromChassis);
            Mockito.doReturn(Collections.singletonMap(chassis,
                                            Collections.singletonList(boughtFromChassisWrapper))).when(fabricPmSe)
                            .getCommodityBoughtListByProvider();
            Mockito.doReturn(Optional.empty()).when(hypervisorPmSe)
                            .getMatchingCommoditiesBought(chassis, boughtFromChassisWrapper);
            fabricPmProviders = ImmutableSet.of(fabricDcSe, chassis);
        } else {
            fabricPmProviders = Collections.singleton(fabricDcSe);
        }
        Mockito.doReturn(fabricPmProviders).when(fabricPmSe).getProviders();
        final CommoditiesBought boughtFromDcWrap =
                        new CommoditiesBought(createBoughtCommodities(hypervisorDcSe, 1D));
        final Map<StitchingEntity, List<CommoditiesBought>> hypervisorPmBoughts = new HashMap<>();
        hypervisorPmBoughts.put(hypervisorDcSe, Collections.singletonList(boughtFromDcWrap));
        Mockito.doAnswer(invocation -> {
            hypervisorPmBoughts.remove(hypervisorDcSe);
            return null;
        }).when(hypervisorPmSe).removeProvider(hypervisorDcSe);
        Mockito.doReturn(hypervisorPmBoughts).when(hypervisorPmSe)
                        .getCommodityBoughtListByProvider();
        Mockito.doReturn(Collections.singleton(hypervisorDcSe)).when(hypervisorPmSe).getProviders();
        final StitchingPoint stitchingPoint =
                        new StitchingPoint(fabricPmSe, Collections.singleton(hypervisorPmSe));
        // ACT
        stitchingOperation.stitch(stitchingPoint, changesBuilder);
        // CHECK
        Assert.assertThat(getCommodities(hypervisorPmBoughts, hypervisorDcSe,
                                        CommodityDTO.Builder::getCommodityType),
                        CoreMatchers.is(commodityTypesFromDc));
        Assert.assertThat(getCommodities(hypervisorPmBoughts, chassis, CommodityDTO.Builder::build),
                        CoreMatchers.is(boughtFromChassis.stream().map(CommodityDTO.Builder::build)
                                        .collect(Collectors.toSet())));
        Mockito.verify(fabricPmSe, Mockito.times(1)).removeProvider(fabricDcSe);
    }

    private static List<CommodityDTO.Builder> createBoughtCommodities(
                    @Nullable StitchingEntity provider, double powerUsage) {
        if (provider == null) {
            return Collections.emptyList();
        }
        final List<CommodityDTO.Builder> result = new ArrayList<>(2);
        result.add(createCommodity(provider, CommodityType.POWER, powerUsage));
        result.add(createCommodity(provider, CommodityType.DATACENTER, 1D));
        return result;
    }

    @Nonnull
    private static <T> Collection<T> getCommodities(
                    @Nonnull Map<StitchingEntity, List<CommoditiesBought>> providerToBoughts,
                    @Nullable StitchingEntity provider, @Nonnull Function<Builder, T> converter) {
        return Optional.ofNullable(providerToBoughts.get(provider))
                        .map(boughts -> boughts.iterator().next().getBoughtList().stream()
                                        .map(converter).collect(Collectors.toSet()))
                        .orElse(Collections.emptySet());
    }

    @Nonnull
    private static CommodityDTO.Builder createCommodity(StitchingEntity provider, CommodityType commodityType,
                    double usage) {
        return CommodityBuilders.commodity(commodityType).from(provider.getLocalId()).used(usage)
                        .key(provider.getLocalId()).build().toBuilder();
    }

    private static StitchingChangesBuilder<StitchingEntity> mockChangesBuilder() {
        @SuppressWarnings("unchecked")
        final StitchingChangesBuilder<StitchingEntity> result =
                        Mockito.mock(StitchingChangesBuilder.class);
        Mockito.doAnswer(invocation -> {
            final StitchingEntity entity = invocation.getArgumentAt(0, StitchingEntity.class);
            @SuppressWarnings("unchecked")
            final Consumer<StitchingEntity> consumer = invocation.getArgumentAt(1, Consumer.class);
            consumer.accept(entity);
            return result;
        }).when(result).queueChangeRelationships(Mockito.any(), Mockito.any());
        return result;
    }

    private static StitchingEntity mockEntity(EntityType entityType, String localId) {
        final StitchingEntity result = Mockito.mock(StitchingEntity.class);
        Mockito.doReturn(localId).when(result).getLocalId();
        Mockito.doReturn(entityType).when(result).getEntityType();
        return result;
    }
}
