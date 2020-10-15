package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.platform.common.builders.ConsumerPolicyBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.UtilizationData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.VCpuData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;
import com.vmturbo.stitching.DTOFieldSpec;

public class DTOFieldAndPropertyHandlerTest {

    private final ConsumerPolicyBuilder conPolBuilderFalse =
            ConsumerPolicyBuilder.consumer().controllable(false);

    private final ConsumerPolicyBuilder conPolBuilderTrue =
            ConsumerPolicyBuilder.consumer().controllable(true);

    private final EntityDTO.Builder vmFoo = virtualMachine("fooTest")
            .displayName("foo")
            .property("prop1", "fooValue1")
            .powerState(PowerState.SUSPENDED)
            .withPolicy(conPolBuilderFalse)
            .profileId("fooProfile")
            .build().toBuilder();

    private final EntityDTO.Builder vmBar = virtualMachine("barTest")
            .displayName("bar")
            .property("prop1", "barValue1")
            .property("prop2", "barValue2")
            .powerState(PowerState.POWERED_ON)
            .withPolicy(conPolBuilderTrue)
            .profileId("barProfile")
            .build().toBuilder();

    @Test
    public void testGetProperty() {
        assertEquals("barValue1",
                DTOFieldAndPropertyHandler.getPropertyFromEntity(vmBar, "prop1"));
    }

    @Test
    public void testSetProperty() {
        // set a new property and make sure it is added
        DTOFieldAndPropertyHandler.setPropertyOfEntity(vmFoo, "testProp", "setTest");
        assertEquals("setTest", vmFoo.getEntityProperties(1).getValue());
        // set an existing property and make sure it has the new value
        assertEquals("fooValue1", vmFoo.getEntityProperties(0).getValue());
        DTOFieldAndPropertyHandler.setPropertyOfEntity(vmFoo, "prop1", "prop1test");
        assertEquals("prop1test", vmFoo.getEntityProperties(0).getValue());
    }

    @Test
    public void testGetField() throws NoSuchFieldException {
        assertEquals(PowerState.POWERED_ON.toString(), DTOFieldAndPropertyHandler
                .getFieldFromMessageOrBuilder(vmBar, "powerState").toString());
        assertEquals(vmFoo.getConsumerPolicy().getControllable(),
                (Boolean) DTOFieldAndPropertyHandler.getValueFromFieldSpec(vmFoo,
                        new DTOFieldSpec() {
                            @Nonnull
                            @Override
                            public String getFieldName() {
                                return "controllable";
                            }

                            @Nonnull
                            @Override
                            public List<String> getMessagePath() {
                                return Lists.newArrayList("consumerPolicy");
                            }
                        }));

    }

    @Test(expected = NoSuchFieldException.class)
    public void testNoSuchField() throws NoSuchFieldException {
        DTOFieldAndPropertyHandler.getFieldFromMessageOrBuilder(vmBar, "madeupfieldname");
    }

    @Test
    public void testSetField() throws NoSuchFieldException{
        assertFalse(vmFoo.getConsumerPolicy().getControllable());
        DTOFieldAndPropertyHandler.setValueToFieldSpec(vmFoo,
                new DTOFieldSpec() {
                    @Nonnull
                    @Override
                    public String getFieldName() {
                        return "controllable";
                    }

                    @Nonnull
                    @Override
                    public List<String> getMessagePath() {
                        return Lists.newArrayList("consumerPolicy");
                    }
                }, true);
        assertTrue(vmFoo.getConsumerPolicy().getControllable());
    }

    @Test
    public void testMergeCommodities() {
        // try to copy the same commodity from source to destination
        // The number of commodities should not change, but the value of used should get written to
        // the destination commodity
        CommodityDTO.Builder comm1 = vCpuMHz().used(10.0D).capacity(20.0D).active(true)
                .utilizationData(UtilizationData.newBuilder().addPoint(10.0D).addPoint(20.0D)
                        .setIntervalMs(100).setLastPointTimestampMs(2121221L).build())
                .build().toBuilder();

        CommodityDTO.Builder comm2 = vCpuMHz().peak(100.0D).build().toBuilder();
        DTOFieldAndPropertyHandler.mergeBuilders(comm1, comm2, Collections.emptyList());
        assertEquals(10.0D, comm2.getUsed(), 0.1D);
        assertEquals(100, comm2.getUtilizationData().getIntervalMs());
        assertEquals(100.0D, comm2.getPeak(), 0.1D);
    }

    @Test
    public void testMergeCommoditiesOnlySpecifiedFields() {
        final CommodityDTO.Builder comm1 = getCommodityBuilder(900, CommodityType.VCPU).setUsed(50).setVcpuData(
                VCpuData.newBuilder().setHotAddSupported(true).setHotRemoveSupported(true));
        final CommodityDTO.Builder comm2 = getCommodityBuilder(1000, CommodityType.VCPU).setUsed(10);

        List<DTOFieldSpec> dtoFieldSpecs = ImmutableList.of(
            new DTOFieldSpec() {
                @Nonnull
                @Override
                public String getFieldName() {
                    return "used";
                }

                @Nonnull
                @Override
                public List<String> getMessagePath() {
                    return Collections.emptyList();
                }
            },
            new DTOFieldSpec() {
                @Nonnull
                @Override
                public String getFieldName() {
                    return "hotAddSupported";
                }

                @Nonnull
                @Override
                public List<String> getMessagePath() {
                    return ImmutableList.of("vcpu_data");
                }
            }
        );

        DTOFieldAndPropertyHandler.mergeBuilders(comm1, comm2, dtoFieldSpecs);
        assertEquals(50, comm2.getUsed(), 0);
        assertEquals(1000, comm2.getCapacity(), 0);
        assertTrue(comm2.getVcpuData().getHotAddSupported());
        assertFalse(comm2.getVcpuData().hasHotRemoveSupported());
    }

    /**
     * Tests the case when two probes are stitched. First probe contains used, the second - used and
     * utilizationData.
     * "Used" and "utilizationData" are two fields that depend on each other, so when we
     * change "used", "utilizationData" must also change, and if probe contains information only about
     * "used" and overwrites the information of another probe, then "utilizationData" should be cleared.
     */
    @Test
    public void testMergeCommoditiesUtilizationData() {
        final int usedCommodity1 = 1000;
        final int capacityCommodity1 = 900;
        final CommodityDTO.Builder commodity1 = getCommodityBuilder(capacityCommodity1,
                CommodityType.VMEM).setUsed(usedCommodity1);
        final CommodityDTO.Builder commodity2 = getCommodityBuilder(500, CommodityType.VMEM).setUsed(10).setUtilizationData(
                UtilizationData.newBuilder()
                        .setLastPointTimestampMs(100)
                        .setIntervalMs(10)
                        .addPoint(123)
                        .build());
        DTOFieldAndPropertyHandler.mergeBuilders(commodity1, commodity2, Collections.emptyList());
        assertEquals(usedCommodity1, commodity2.getUsed(), 0);
        assertEquals(capacityCommodity1, commodity2.getCapacity(), 0);
        assertFalse(commodity2.hasUtilizationData());
        assertEquals(CommodityDTO.UtilizationData.getDefaultInstance(),
                commodity2.getUtilizationData());
    }

    /**
     * Tests the case when two probes are stitched. First probe contains utilizationData, the second - utilizationData and
     * used.
     * "Used" and "utilizationData" are two fields that depend on each other, so when we
     * change "utilizationData", "used" must also change, and if probe contains information only about
     * "utilizationData" and overwrites the information of another probe, then "used" should be cleared.
     */
    @Test
    public void testMergeCommoditiesUsed() {
        final int capacityCommodity1 = 900;
        final UtilizationData utilizationData = UtilizationData.newBuilder()
                .setLastPointTimestampMs(100)
                .setIntervalMs(10)
                .addPoint(123)
                .build();
        final CommodityDTO.Builder commodity1 = getCommodityBuilder(capacityCommodity1,
                CommodityType.VMEM).setUtilizationData(utilizationData);
        final CommodityDTO.Builder commodity2 = CommodityDTO.newBuilder().setCommodityType(
                CommodityType.VMEM).setUsed(10).setCapacity(500).setUtilizationData(
                utilizationData);
        DTOFieldAndPropertyHandler.mergeBuilders(commodity1, commodity2, Collections.emptyList());
        assertEquals(utilizationData, commodity2.getUtilizationData());
        assertEquals(capacityCommodity1, commodity2.getCapacity(), 0);
        assertFalse(commodity2.hasUsed());
        assertEquals(0, commodity2.getUsed(), 0);
    }

    private Builder getCommodityBuilder(int capacity, CommodityType commodityType) {
        return CommodityDTO.newBuilder().setCommodityType(commodityType).setCapacity(capacity);
    }
}
