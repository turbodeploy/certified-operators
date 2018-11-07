package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.EntityBuilders.businessAccount;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.common.builders.ConsumerPolicyBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.stitching.DTOFieldSpec;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.utilities.EntityFieldMergers.EntityFieldMerger;

public class EntityFieldMergersTest {
    private final ConsumerPolicyBuilder conPolBuilderFalse =
            ConsumerPolicyBuilder.consumer().controllable(false);

    private final ConsumerPolicyBuilder conPolBuilderTrue =
            ConsumerPolicyBuilder.consumer().controllable(true);

    private final EntityDTO.Builder vmFoo = virtualMachine("foo")
            .displayName("foo")
            .property("prop1", "fooValue1")
            .powerState(PowerState.SUSPENDED)
            .withPolicy(conPolBuilderFalse)
            .profileId("fooProfile")
            .build().toBuilder();

    private final EntityDTO.Builder vmBar = virtualMachine("bar")
            .displayName("bar")
            .property("prop1", "barValue1")
            .property("prop2", "barValue2")
            .powerState(PowerState.POWERED_ON)
            .withPolicy(conPolBuilderTrue)
            .profileId("barProfile")
            .build().toBuilder();

    private final StitchingEntity foo = new TestStitchingEntity(vmFoo);
    private final StitchingEntity bar = new TestStitchingEntity(vmBar);

    @Test
    public void testMergeDisplayName() {
        // Merge from bar onto foo
        EntityFieldMergers.DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST
            .merge(bar, foo);

        // After merging the displayName should now be "bar" because it comes before "foo" in the alphabet.
        assertEquals("bar", foo.getDisplayName());
    }

    @Test
    public void testCustomMerge() {
        final EntityFieldMerger<PowerState> powerStateMerger = EntityFieldMergers
            .merge(EntityDTOOrBuilder::getPowerState, EntityDTO.Builder::setPowerState)
            .withMethod((fromEntity, fromField, ontoEntity, ontoField) -> {
                if (fromEntity.getLocalId().equals("foo")) {
                    return PowerState.POWERED_OFF;
                } else {
                    return PowerState.POWERSTATE_UNKNOWN;
                }
            });

        powerStateMerger.merge(foo, bar);
        assertEquals(PowerState.POWERED_OFF, bar.getEntityBuilder().getPowerState());

        powerStateMerger.merge(bar, foo);
        assertEquals(PowerState.POWERSTATE_UNKNOWN, foo.getEntityBuilder().getPowerState());
    }

    @Test
    public void testPropertyMerge() {
        // merge prop1 from foo to bar.  Expect bar to get foo's property value
        EntityFieldMergers.getPropertyFieldMerger("prop1").merge(foo, bar);
        assertEquals("fooValue1",
                bar.getEntityBuilder().getEntityProperties(0).getValue());

        // merge prop2 from foo to bar.  Expect bar to keep its property value since foo does not
        // have this property
        EntityFieldMergers.getPropertyFieldMerger("prop2").merge(foo, bar);
        assertEquals("barValue2",
                bar.getEntityBuilder().getEntityProperties(1).getValue());
    }

    @Test
    public void testAttributeFieldMerger() {
        final EntityFieldMerger<Object> consumerPolicyMerger = EntityFieldMergers.getAttributeFieldMerger(
                new DTOFieldSpec() {
                    @Override
                    public String getFieldName() {
                        return "controllable";
                    }

                    @Override
                    public List<String> getMessagePath() {
                        return ImmutableList.of("consumerPolicy");
                    }

                });

        final EntityFieldMerger<Object> profileIdMerger = EntityFieldMergers.getAttributeFieldMerger(
                new DTOFieldSpec() {
                    @Override
                    public String getFieldName() {
                        return "profileId";
                    }

                    @Override
                    public List<String> getMessagePath() {
                        return new ArrayList<>();
                    }

                });

        assertTrue(bar.getEntityBuilder().getConsumerPolicy().getControllable());
        consumerPolicyMerger.merge(foo, bar);
        assertFalse(bar.getEntityBuilder().getConsumerPolicy().getControllable());

        assertEquals("barProfile", bar.getEntityBuilder().getProfileId());
        profileIdMerger.merge(foo, bar);
        assertEquals("fooProfile", bar.getEntityBuilder().getProfileId());
    }

    @Test
    public void testMergeBusinessAccountFields() {
        final String subAccountName = "subAccount1";
        // sub account (discovered from master account target) with displayName, but no consistsOf
        EntityDTO.Builder baDTO1 = businessAccount("baId1")
                .displayName(subAccountName)
                .build().toBuilder();
        // sub account (discovered from sub account target) with consistsOf, but no displayName
        EntityDTO.Builder baDTO2 = businessAccount("baId1")
                .consistsOf("vm1")
                .consistsOf("vm2")
                .build().toBuilder();

        StitchingEntity ba1 = new TestStitchingEntity(baDTO1);
        StitchingEntity ba2 = new TestStitchingEntity(baDTO2);

        final EntityFieldMerger<Object> displayNameMerger = EntityFieldMergers.getAttributeFieldMerger(
                new DTOFieldSpec() {
                    @Override
                    public String getFieldName() {
                        return "displayName";
                    }

                    @Override
                    public List<String> getMessagePath() {
                        return Collections.emptyList();
                    }
                });

        final EntityFieldMerger<Object> consistsOfMerger = EntityFieldMergers.getAttributeFieldMerger(
                new DTOFieldSpec() {
                    @Override
                    public String getFieldName() {
                        return "consistsOf";
                    }

                    @Override
                    public List<String> getMessagePath() {
                        return Collections.emptyList();
                    }
                });

        // check that displayName of ba1 is not overwritten
        assertEquals(subAccountName, ba1.getEntityBuilder().getDisplayName());
        displayNameMerger.merge(ba2, ba1);
        assertEquals(subAccountName, ba1.getEntityBuilder().getDisplayName());

        // check that consistsOf is patched from ba2 to ba1
        assertEquals(0, ba1.getEntityBuilder().getConsistsOfCount());
        consistsOfMerger.merge(ba2, ba1);
        assertEquals(2, ba1.getEntityBuilder().getConsistsOfCount());
    }
}