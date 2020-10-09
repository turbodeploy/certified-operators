package com.vmturbo.common.protobuf.action;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ActionEnvironmentType}.
 */
public class ActionEnvironmentTypeTest {

    @Test
    public void testForCloudAction() throws UnsupportedActionException {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(7)
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.CLOUD))))
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

        assertThat(ActionEnvironmentType.forAction(action), is(ActionEnvironmentType.CLOUD));
    }

    @Test
    public void testForOnPremAction() throws UnsupportedActionException {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(7)
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.ON_PREM))))
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

        assertThat(ActionEnvironmentType.forAction(action), is(ActionEnvironmentType.ON_PREM));
    }

    @Test
    public void testForHybridAction() throws UnsupportedActionException {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(7)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.ON_PREM))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(2)
                            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setEnvironmentType(EnvironmentType.ON_PREM))
                        .setDestination(ActionEntity.newBuilder()
                            .setId(3)
                            .setType(EntityType.COMPUTE_TIER_VALUE)
                            .setEnvironmentType(EnvironmentType.CLOUD)))))
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

        assertThat(ActionEnvironmentType.forAction(action), is(ActionEnvironmentType.ON_PREM_AND_CLOUD));

        final ActionDTO.Action hybridAction = ActionDTO.Action.newBuilder()
            .setId(7)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.HYBRID))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(2)
                            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setEnvironmentType(EnvironmentType.HYBRID))
                        .setDestination(ActionEntity.newBuilder()
                            .setId(3)
                            .setType(EntityType.COMPUTE_TIER_VALUE)
                            .setEnvironmentType(EnvironmentType.HYBRID)))))
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

        assertThat(ActionEnvironmentType.forAction(hybridAction), is(ActionEnvironmentType.ON_PREM_AND_CLOUD));
    }

    @Test
    public void testEnvTypeMatch() {
        assertTrue(ActionEnvironmentType.ON_PREM.matchesEnvType(EnvironmentType.ON_PREM));
        assertTrue(ActionEnvironmentType.ON_PREM.matchesEnvType(EnvironmentType.HYBRID));
        assertTrue(ActionEnvironmentType.CLOUD.matchesEnvType(EnvironmentType.CLOUD));
        assertTrue(ActionEnvironmentType.CLOUD.matchesEnvType(EnvironmentType.HYBRID));
        assertTrue(ActionEnvironmentType.ON_PREM_AND_CLOUD.matchesEnvType(EnvironmentType.ON_PREM));
        assertTrue(ActionEnvironmentType.ON_PREM_AND_CLOUD.matchesEnvType(EnvironmentType.CLOUD));
        assertTrue(ActionEnvironmentType.ON_PREM_AND_CLOUD.matchesEnvType(EnvironmentType.HYBRID));
    }

    @Test
    public void testEnvTypeNoMatch() {
        assertFalse(ActionEnvironmentType.ON_PREM.matchesEnvType(EnvironmentType.CLOUD));
        assertFalse(ActionEnvironmentType.ON_PREM.matchesEnvType(EnvironmentType.UNKNOWN_ENV));
        assertFalse(ActionEnvironmentType.CLOUD.matchesEnvType(EnvironmentType.ON_PREM));
        assertFalse(ActionEnvironmentType.CLOUD.matchesEnvType(EnvironmentType.UNKNOWN_ENV));
        assertFalse(ActionEnvironmentType.ON_PREM_AND_CLOUD.matchesEnvType(EnvironmentType.UNKNOWN_ENV));
    }
}
