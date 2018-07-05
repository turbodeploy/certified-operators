package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.platform.common.builders.ConsumerPolicyBuilder;
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
                            @Override
                            public String getFieldName() {
                                return "controllable";
                            }

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
                    @Override
                    public String getFieldName() {
                        return "controllable";
                    }

                    @Override
                    public List<String> getMessagePath() {
                        return Lists.newArrayList("consumerPolicy");
                    }
                }, true);
        assertTrue(vmFoo.getConsumerPolicy().getControllable());
    }
}