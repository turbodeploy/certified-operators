package com.vmturbo.action.orchestrator.store.identity;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Unit test for {@link ActionInfoModelCreator}.
 */
public class ActionInfoModelCreatorTest {

    private ActionInfoModelCreator modelCreator;
    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        this.modelCreator = new ActionInfoModelCreator();
    }

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
        final ActionInfoModel model = modelCreator.apply(move);
        Assert.assertEquals(Optional.of(
                Sets.newHashSet("{\"sourceId\":\"1\",\"destinationId\":\"2\",\"resourceId\":\"3\"}",
                        "{\"sourceId\":\"4\",\"destinationId\":\"5\"}")),
                model.getAdditionalDetails());
    }

    /**
     * Tests that all the available action types are supported by {@link ActionInfoModelCreator}.
     */
    @Test
    public void testAllActionTypesCovered() {
        final Set<ActionInfo> actions = new HashSet<>();
        actions.add(createMove());
        actions.add(createActivate());
        actions.add(createAllocate());
        actions.add(createReconfigure());
        actions.add(createProvision());
        actions.add(createResize());
        actions.add(createDeactivate());
        actions.add(createBuyRi());
        actions.add(createDelete());
        actions.add(createScale());
        final Set<ActionTypeCase> uncoveredActionTypes = EnumSet.allOf(ActionTypeCase.class);
        for (ActionInfo action : actions) {
            uncoveredActionTypes.remove(action.getActionTypeCase());
        }
        uncoveredActionTypes.remove(ActionTypeCase.ACTIONTYPE_NOT_SET);
        Assert.assertEquals(
                "New action types should be added to this test: " + uncoveredActionTypes,
                Collections.emptySet(), uncoveredActionTypes);
        final Set<ActionInfoModel> models = new HashSet<>();
        for (ActionInfo action : actions) {
            models.add(modelCreator.apply(action));
        }
        Assert.assertEquals(actions.size(), models.size());
    }

    /**
     * Tests the case when action type is undefined.
     */
    @Test
    public void testInvalidActionType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Could not find a suitable field extractor");
        modelCreator.apply(ActionInfo.newBuilder().build());
    }

    @Nonnull
    private ActionInfo createMove() {
        return ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(createActionEntity(6L))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(createActionEntity(1L))
                                .setDestination(createActionEntity(2L))))
                .build();
    }

    @Nonnull
    private ActionInfo createReconfigure() {
        return ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                        .setSource(createActionEntity(1))
                        .setTarget(createActionEntity(2)))
                .build();
    }

    @Nonnull
    private ActionInfo createProvision() {
        return ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                        .setEntityToClone(createActionEntity(1))
                        .setProvisionedSeller(33))
                .build();
    }

    @Nonnull
    private ActionInfo createResize() {
        return ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(createActionEntity(1))
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(1))
                        .setCommodityAttribute(CommodityAttribute.CAPACITY))
                .build();
    }

    @Nonnull
    private ActionInfo createActivate() {
        return ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                        .setTarget(createActionEntity(1)))
                .build();
    }

    @Nonnull
    private ActionInfo createDeactivate() {
        return ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder()
                        .setTarget(createActionEntity(1)))
                .build();
    }

    @Nonnull
    private ActionInfo createDelete() {
        return ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder()
                        .setTarget(createActionEntity(1)))
                .build();
    }

    @Nonnull
    private ActionInfo createBuyRi() {
        return ActionInfo.newBuilder()
                .setBuyRi(BuyRI.newBuilder()
                        .setBuyRiId(1)
                        .setCount(2)
                        .setMasterAccount(createActionEntity(1)))
                .build();
    }

    @Nonnull
    private ActionInfo createScale() {
        return ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(createActionEntity(1))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(createActionEntity(2))
                                .setDestination(createActionEntity(3))))
                .build();
    }

    @Nonnull
    private ActionInfo createAllocate() {
        return ActionInfo.newBuilder()
                .setAllocate(Allocate.newBuilder()
                        .setTarget(createActionEntity(1))
                        .setWorkloadTier(createActionEntity(2)))
                .build();
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
