package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit test for {@link GuestLoadAppPostStitchingOperation}.
 */
public class GuestLoadAppPostStitchingOperationTest {

    private final GuestLoadAppPostStitchingOperation guestLoadOperation =
        new GuestLoadAppPostStitchingOperation();

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private final EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    private static final long vmOid1 = 11L;
    private static final long vmOid2 = 12L;
    private static final long appOid1 = 21L;
    private static final long appOid2 = 22L;
    private static final long appOid3 = 23L;

    private static TopologyEntity guestLoadAppEntity1;
    private static TopologyEntity realAppEntity1;
    private static TopologyEntity guestLoadAppEntity2;

    /**
     * Set up the topology before each unit test.
     */
    @Before
    public void setup() {
        final TopologyEntity.Builder guestLoadApp1 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(appOid1)
                .setDisplayName("GuestLoad[vm1]")
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .putEntityPropertyMap(
                    GuestLoadAppPostStitchingOperation.APPLICATION_TYPE_PATH,
                    SupplyChainConstants.GUEST_LOAD)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(vmOid1)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE))
                        .setUsed(150))
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VMEM_VALUE))
                        .setUsed(300))
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VSTORAGE_VALUE)
                            .setKey("VirtualMachine::123"))
                        .setUsed(1000))
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VSTORAGE_VALUE)
                            .setKey("VirtualMachine::456"))
                        .setUsed(1500))
                ));

        final TopologyEntity.Builder realApp1 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(appOid2)
                .setDisplayName("RealApp")
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(vmOid1)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE))
                        .setUsed(250))
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VMEM_VALUE))
                        .setUsed(400))
                ));

        final TopologyEntity.Builder vm1 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(vmOid1)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VCPU_VALUE))
                    .setUsed(300))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VMEM_VALUE))
                    .setUsed(500))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VSTORAGE_VALUE)
                        .setKey("VirtualMachine::123"))
                    .setUsed(1000))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VSTORAGE_VALUE)
                        .setKey("VirtualMachine::456"))
                    .setUsed(1500))
        );

        final TopologyEntity.Builder guestLoadApp2 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(appOid3)
                .setDisplayName("GuestLoad[vm2]")
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .putEntityPropertyMap(
                    GuestLoadAppPostStitchingOperation.APPLICATION_TYPE_PATH,
                    SupplyChainConstants.GUEST_LOAD)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(vmOid2)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE))
                        .setUsed(200))
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VMEM_VALUE))
                        .setUsed(400))));

        final TopologyEntity.Builder vm2 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(vmOid2)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VCPU_VALUE))
                    .setUsed(200))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VMEM_VALUE))
                    .setUsed(400)));

        // realApp1 is stitched to vm1, and original guestLoadApp1 is still there
        guestLoadApp1.addProvider(vm1);
        realApp1.addProvider(vm1);
        vm1.addConsumer(guestLoadApp1);
        vm1.addConsumer(realApp1);

        guestLoadApp2.addProvider(vm2);
        vm2.addConsumer(guestLoadApp2);

        guestLoadAppEntity1 = guestLoadApp1.build();
        realAppEntity1 = realApp1.build();
        guestLoadAppEntity2 = guestLoadApp2.build();
    }

    /**
     * Test that the GuestLoad application's commodities used values are adjusted if there is
     * other applications stitched to the provider VM.
     * The new used value should be: VM sold - total other apps' used.
     */
    @Test
    public void testGuestLoadUsedAdjusted() {
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        guestLoadOperation.performOperation(Stream.of(guestLoadAppEntity1, realAppEntity1,
            guestLoadAppEntity2), settingsCollection, resultBuilder);

        assertEquals(2, resultBuilder.getChanges().size());
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        // verify guestLoadAppEntity1's used are changed
        verifyBoughtCommodityUsed(guestLoadAppEntity1, vmOid1, CommodityType.VCPU_VALUE, "", 50D);
        verifyBoughtCommodityUsed(guestLoadAppEntity1, vmOid1, CommodityType.VMEM_VALUE, "", 100D);
        verifyBoughtCommodityUsed(guestLoadAppEntity1, vmOid1, CommodityType.VSTORAGE_VALUE,
            "VirtualMachine::123", 1000D);
        verifyBoughtCommodityUsed(guestLoadAppEntity1, vmOid1, CommodityType.VSTORAGE_VALUE,
            "VirtualMachine::456", 1500D);

        // verify real app's used is not changed
        verifyBoughtCommodityUsed(realAppEntity1, vmOid1, CommodityType.VCPU_VALUE, "", 250D);
        verifyBoughtCommodityUsed(realAppEntity1, vmOid1, CommodityType.VMEM_VALUE, "", 400D);

        // verify guestLoadAppEntity2's used is not changed, since no app is stitched to its VM
        verifyBoughtCommodityUsed(guestLoadAppEntity2, vmOid2, CommodityType.VCPU_VALUE, "", 200D);
        verifyBoughtCommodityUsed(guestLoadAppEntity2, vmOid2, CommodityType.VMEM_VALUE, "", 400D);
    }

    /**
     * Verify the entity buys one commodity of give type, key and used value from the given
     * provider entity.
     *
     * @param entity the entity to check
     * @param providerId provider entity
     * @param commodityType type of commodity
     * @param key key of commodity
     * @param used used value of commodity
     */
    private void verifyBoughtCommodityUsed(TopologyEntity entity, long providerId,
                                           Integer commodityType, String key, Double used) {
        List<CommodityBoughtDTO.Builder> commodity = entity.getTopologyEntityDtoBuilder()
            .getCommoditiesBoughtFromProvidersBuilderList().stream()
            .filter(cb -> cb.getProviderId() == providerId)
            .flatMap(cb -> cb.getCommodityBoughtBuilderList().stream())
            .filter(comm -> comm.getCommodityType().getType() == commodityType)
            .filter(comm -> comm.getCommodityType().getKey().equals(key))
            .collect(Collectors.toList());

        assertEquals(1, commodity.size());
        assertEquals(used, commodity.get(0).getUsed(), 0.0);
    }
}
