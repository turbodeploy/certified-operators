package com.vmturbo.stitching.fabric;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
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
    private static final String SERIAL = "ServiceTag";

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
        Mockito.verify(chassis, Mockito.times(1)).addAllConnectedTo(ConnectionType.NORMAL_CONNECTION,
                        Collections.singleton(hypervisorDcSe));
    }

    /**
     * If there is no match in the serial numbers, we should fall back to stitching to the hypervisor
     * pm with the smallest OID.
     */
    @Test
    public void checkMultipleExternalEntitiesStitchingWithLargestOid() {
        final StitchingEntity fabricPm = createFabricPm("a");
        final StitchingEntity hypervisorA = createHypervisorPm("y");
        final StitchingEntity hypervisorB = createHypervisorPm("z");
        Mockito.doReturn((long)2).when(hypervisorA).getOid();
        Mockito.doReturn((long)1).when(hypervisorB).getOid();

        stitch(new StitchingPoint(fabricPm, Arrays.asList(hypervisorA, hypervisorB)));

        // Make sure that the pm with the smallest oid gets chosen when we don't have a matching
        // serial number.
        Mockito.verify(hypervisorA, Mockito.times(0)).getProviders();
        Mockito.verify(hypervisorB, Mockito.times(1)).getProviders();
    }

    /**
     * Internal entity should be stitched to the external entity having the same serial number.
     */
    @Test
    public void checkMultipleExternalEntitiesStitchingWithMatchingSerial() {
        final StitchingEntity fabricPm = createFabricPm("x");
        final StitchingEntity hypervisorA = createHypervisorPm("x");
        final StitchingEntity hypervisorB = createHypervisorPm("y");

        stitch(new StitchingPoint(fabricPm, Arrays.asList(hypervisorA, hypervisorB)));

        // Make sure that the pm with the matching serial is processed
        Mockito.verify(hypervisorA, Mockito.times(1)).getProviders();
        Mockito.verify(hypervisorB, Mockito.times(0)).getProviders();
    }

    private static void checkFabricPmStitching(StitchingEntity fabricDcSe, StitchingEntity hypervisorDcSe,
                    @Nullable StitchingEntity chassis, Collection<CommodityType> commodityTypesFromDc,
                    boolean keepDCsAfterFabricStitching) {
        // WHEN
        final FabricPMStitchingOperation stitchingOperation =
                        new FabricPMStitchingOperation(keepDCsAfterFabricStitching);
        final StitchingChangesBuilder<StitchingEntity> changesBuilder = mockChangesBuilder();
        final StitchingEntity fabricPmSe = mockPmWithSerial(null);
        final StitchingEntity hypervisorPmSe = mockPmWithSerial(null);
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

    private static void addSerial(StitchingEntity entity, @Nonnull String serial) {
        EntityDTO.Builder pmBuilder = EntityDTO.newBuilder()
                .addEntityProperties(EntityBuilders.entityProperty().named(SERIAL).withValue(serial).build());
        Mockito.when(entity.getEntityBuilder()).thenReturn(pmBuilder);
    }

    private static StitchingEntity mockPmWithSerial(@Nullable String serial) {
        StitchingEntity pm = Mockito.mock(StitchingEntity.class);
        addSerial(pm, serial != null ? serial : "hellothere");
        return pm;
    }

    private static StitchingEntity createFabricPm(@Nullable String serial) {
        final StitchingEntity fabricPm = mockPmWithSerial(serial);
        final StitchingEntity fabricDcSe = mockEntity(EntityType.DATACENTER, FABRIC_DC_ID);
        final StitchingEntity chassis = mockEntity(EntityType.CHASSIS, CHASSIS_ID);

        Mockito.doReturn(ImmutableSet.of(fabricDcSe, chassis)).when(fabricPm).getProviders();

        return fabricPm;
    }

    private static StitchingEntity createHypervisorPm(@Nullable String serial) {
        final StitchingEntity hypervisorPm = mockPmWithSerial(serial);
        final StitchingEntity hypervisorDcSe = mockEntity(EntityType.DATACENTER, HYPERVISOR_DC_ID);

        // Mock providers
        Mockito.doReturn(Collections.singleton(hypervisorDcSe)).when(hypervisorPm).getProviders();
        Mockito.doReturn(new HashMap<>()).when(hypervisorPm)
                .getCommodityBoughtListByProvider();

        return hypervisorPm;
    }

    private static void stitch(@Nonnull StitchingPoint stitchingPoint) {
        final FabricPMStitchingOperation stitchingOperation =
                new FabricPMStitchingOperation(true);
        final StitchingChangesBuilder<StitchingEntity> changesBuilder = mockChangesBuilder();
        stitchingOperation.stitch(stitchingPoint, changesBuilder);
    }
}
