package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.platform.common.builders.ConsumerPolicyBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
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
        CommodityDTO.Builder comm1 = CommodityDTO.newBuilder()
            .setCommodityType(CommodityType.VCPU)
            .setUsed(50)
            .setCapacity(900)
            .setVcpuData(VCpuData.newBuilder()
                .setHotAddSupported(true)
                .setHotRemoveSupported(true));
        CommodityDTO.Builder comm2 = CommodityDTO.newBuilder()
            .setCommodityType(CommodityType.VCPU)
            .setUsed(10)
            .setCapacity(1000);

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
}
