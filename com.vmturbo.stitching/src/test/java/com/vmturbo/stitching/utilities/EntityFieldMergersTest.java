package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.utilities.EntityFieldMergers.EntityFieldMerger;

public class EntityFieldMergersTest {
    private final EntityDTO.Builder vmFoo = virtualMachine("foo")
        .displayName("foo")
        .powerState(PowerState.SUSPENDED)
        .build().toBuilder();

    private final EntityDTO.Builder vmBar = virtualMachine("bar")
        .displayName("bar")
        .powerState(PowerState.POWERED_ON)
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
}