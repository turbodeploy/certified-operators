package com.vmturbo.action.orchestrator.store.identity;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Unit test for {@link ActionInfoModelCreator}.
 */
public class ActionInfoModelCreatorTest {
    /**
     * Tests that JSON no-pretty-print result is the same as was in the previous versions.
     * If this this fails, update the constants in comparison AND write a data migration.
     */
    @Test
    public void testJsonIsCompatible() {
        final ActionInfo move = ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(createActionEntity(6L))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(createActionEntity(1L))
                                .setDestination(createActionEntity(2L))
                                .setResource(createActionEntity(3L)))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(createActionEntity(4L))
                                .setDestination(createActionEntity(5L))))
                .build();
        final ActionInfoModel model = new ActionInfoModelCreator().apply(move);
        Assert.assertEquals("[{\"sourceId\":\"1\",\"destinationId\":\"2\",\"resourceId\":\"3\"},"
                + "{\"sourceId\":\"4\",\"destinationId\":\"5\"}]",
                model.getDetails().get());
    }

    @Nonnull
    private ActionEntity createActionEntity(long oid) {
        return ActionEntity.newBuilder()
                .setId(oid)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .setType(15)
                .build();
    }
}
