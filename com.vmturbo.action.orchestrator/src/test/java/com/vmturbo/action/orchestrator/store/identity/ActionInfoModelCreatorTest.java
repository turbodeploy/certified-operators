package com.vmturbo.action.orchestrator.store.identity;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.action.orchestrator.store.identity.ActionInfoModelCreator.AtomicResizeChange;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModelCreator.ResizeChangeByTarget;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.ComponentGsonFactory;

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
        actions.add(createAtomicResize());
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
     * Test the ActionInfoModel for atomic resize
     * with the same execution target but multiple resizes in different order
     * for different de-duplication targets.
     */
    @Test
    public void testAtomicResizeActionTypeWithMultipleDeDuplicationTargets() {
        ActionInfo resize1 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(createResizeInfo(2, 1))
                        .addResizes(createResizeInfo(3, 1))
                        .build())
                .build();

        ActionInfoModel model1 = modelCreator.apply(resize1);

        ActionInfo resize2 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(createResizeInfo(3, 1))
                        .addResizes(createResizeInfo(2, 1))
                        .build())
                .build();
        ActionInfoModel model2 = modelCreator.apply(resize2);
        Assert.assertEquals(model1, model2);

        ActionInfo resize3 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(createResizeInfo(2, 1))
                        .build())
                .build();
        ActionInfoModel model3 = modelCreator.apply(resize3);
        Assert.assertNotEquals(model1, model3);

        ActionInfo resize4 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(createResizeInfo(3, 1))
                        .build())
                .build();
        ActionInfoModel model4 = modelCreator.apply(resize4);
        Assert.assertNotEquals(model1, model4);
    }

    private ResizeInfo.Builder createResizeInfo(long id, int commType) {
        return ResizeInfo.newBuilder()
                .setTarget(createActionEntity(id))
                .setCommodityType(CommodityType.newBuilder().setType(commType))
                .setCommodityAttribute(CommodityAttribute.CAPACITY)
                .setOldCapacity(124).setNewCapacity(456);
    }

    /**
     * Test the ActionInfoModel for atomic resize
     * with the same execution target but multiple commodity resizes in different order.
     */
    @Test
    public void testAtomicResizeActionTypeWithMultipleCommodities() {
        ActionInfo resize1 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(createResizeInfo(2, 1))
                        .addResizes(createResizeInfo(2, 2))
                        .build())
                .build();
        ActionInfoModel model1 = modelCreator.apply(resize1);

        ActionInfo resize2 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(createResizeInfo(2, 2))
                        .addResizes(createResizeInfo(2, 1))
                        .build())
                .build();
        ActionInfoModel model2 = modelCreator.apply(resize2);
        Assert.assertEquals(model2, model1);
    }

    /**
     * Test that the ActionInfoModel details for atomic resize
     * are not truncate when there are multiple resizes for multiple de-duplication targets
     * and commodities.
     */
    @Test
    public void testAtomicResizeActionTypeWithManyDeDuplicationTargetsAndCommodities() {
        ActionInfo resize1 = ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(createResizeInfo(73617269445251L, 26))
                        .addResizes(createResizeInfo(73617269445251L, 53))
                        .addResizes(createResizeInfo(73617269445251L, 100))
                        .addResizes(createResizeInfo(73617269445251L, 101))
                        .addResizes(createResizeInfo(73617269445252L, 26))
                        .addResizes(createResizeInfo(73617269445252L, 53))
                        .addResizes(createResizeInfo(73617269445252L, 100))
                        .addResizes(createResizeInfo(73617269445252L, 101))
                        .addResizes(createResizeInfo(73617269445253L, 26))
                        .addResizes(createResizeInfo(73617269445253L, 53))
                        .addResizes(createResizeInfo(73617269445253L, 100))
                        .addResizes(createResizeInfo(73617269445253L, 101))
                        .addResizes(createResizeInfo(73617269445254L, 26))
                        .addResizes(createResizeInfo(73617269445254L, 53))
                        .addResizes(createResizeInfo(73617269445254L, 100))
                        .addResizes(createResizeInfo(73617269445254L, 101))
                        .addResizes(createResizeInfo(73617269445255L, 26))
                        .addResizes(createResizeInfo(73617269445255L, 53))
                        .addResizes(createResizeInfo(73617269445255L, 100))
                        .addResizes(createResizeInfo(73617269445255L, 101))
                        .addResizes(createResizeInfo(73617269445256L, 26))
                        .addResizes(createResizeInfo(73617269445256L, 53))
                        .addResizes(createResizeInfo(73617269445256L, 100))
                        .addResizes(createResizeInfo(73617269445256L, 101))
                        .addResizes(createResizeInfo(73617269445257L, 26))
                        .addResizes(createResizeInfo(73617269445257L, 53))
                        .addResizes(createResizeInfo(73617269445257L, 100))
                        .addResizes(createResizeInfo(73617269445257L, 101))
                        .build())
                .build();
        ActionInfoModel model1 = modelCreator.apply(resize1);

        final List<ResizeChangeByTarget> resizeChangeByTarget
                = resize1.getAtomicResize().getResizesList()
                .stream()
                .collect(Collectors.groupingBy(resize -> resize.getTarget().getId()))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new ResizeChangeByTarget(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        AtomicResizeChange resizeChange = new AtomicResizeChange(resizeChangeByTarget);

        final String changesString = gson.toJson(resizeChange);

        System.out.println(changesString + "[" + changesString.length() + "]");

        Assert.assertEquals(changesString, model1.getDetails().get());
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

    /**
     * Tests that infinite values are correctly converted to JSON (without any errors).
     */
    @Test
    public void testInfiniteResizeCommodity() {
        final ActionInfo infoFin = createResize();
        final ActionInfo infoInf = ActionInfo.newBuilder(infoFin)
                .setResize(Resize.newBuilder()
                        .setNewCapacity(Float.NEGATIVE_INFINITY)
                        .setOldCapacity(Float.POSITIVE_INFINITY)
                        .setTarget(createActionEntity(1))
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(1))
                        .setCommodityAttribute(CommodityAttribute.CAPACITY))
                .build();
        final ActionInfo infoNan = ActionInfo.newBuilder(infoFin)
                .setResize(Resize.newBuilder()
                        .setNewCapacity(Float.NaN)
                        .setOldCapacity(5)
                        .setTarget(createActionEntity(1))
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(1))
                        .setCommodityAttribute(CommodityAttribute.CAPACITY))
                .build();
        final ActionInfoModel modelInf = modelCreator.apply(infoInf);
        final ActionInfoModel modelFin = modelCreator.apply(infoFin);
        final ActionInfoModel modelNan = modelCreator.apply(infoNan);
        Assert.assertNotEquals(modelInf, modelFin);
        Assert.assertNotEquals(modelNan, modelFin);
        Assert.assertNotEquals(modelInf, modelNan);
    }

    /**
     * Tests that provision action info mode.
     */
    @Test
    public void testProvisionActionInfo() {
        final ActionInfo provision1 =  ActionInfo.newBuilder()
            .setProvision(Provision.newBuilder()
                .setEntityToClone(createActionEntity(1))
                .setProvisionIndex(0)
                .setProvisionedSeller(33))
            .build();
        final ActionInfo provision2 = ActionInfo.newBuilder()
            .setProvision(Provision.newBuilder()
                .setEntityToClone(createActionEntity(1))
                .setProvisionIndex(0)
                .setProvisionedSeller(44))
            .build();
        final ActionInfo provision3 =  ActionInfo.newBuilder()
            .setProvision(Provision.newBuilder()
                .setEntityToClone(createActionEntity(1))
                .setProvisionIndex(1)
                .setProvisionedSeller(33))
            .build();
        final ActionInfoModel model1 = modelCreator.apply(provision1);
        final ActionInfoModel model2 = modelCreator.apply(provision2);
        final ActionInfoModel model3 = modelCreator.apply(provision3);
        Assert.assertEquals(model1, model2);
        Assert.assertNotEquals(model1, model3);
    }

    /**
     * Test for the Delete action info mode.
     */
    @Test
    public void testDeleteActionInfo() {
        final String filePath1 = "/test/path1/file1";
        final String filePath2 = "/test/path2/file2";

        final ActionInfo delete1 = ActionInfo.newBuilder()
            .setDelete(Delete.newBuilder()
                .setTarget(createActionEntity(1))
                .setFilePath(filePath1))
            .build();

        final ActionInfo delete2 = ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder()
                    .setTarget(createActionEntity(1))
                    .setFilePath(filePath2))
                .build();

        final ActionInfo delete3 = ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder()
                    .setTarget(createActionEntity(1))
                    .setFilePath(filePath1))
                .build();

        final ActionInfoModel model1 = modelCreator.apply(delete1);
        final ActionInfoModel model2 = modelCreator.apply(delete2);
        final ActionInfoModel model3 = modelCreator.apply(delete3);

        Assert.assertEquals(model1, model3);
        Assert.assertNotEquals(model1, model2);
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
                        .setTarget(createActionEntity(2))
                        .setIsProvider(false))
                .build();
    }

    @Nonnull
    private ActionInfo createProvision() {
        return ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                        .setEntityToClone(createActionEntity(1))
                        .setProvisionIndex(0)
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
                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                        .setOldCapacity(124)
                        .setNewCapacity(456))
                .build();
    }

    @Nonnull
    private ActionInfo createAtomicResize() {
        return ActionInfo.newBuilder()
                .setAtomicResize(AtomicResize.newBuilder()
                        .setExecutionTarget(createActionEntity(1))
                        .addResizes(ResizeInfo.newBuilder()
                                    .setTarget(createActionEntity(2))
                                    .setCommodityType(CommodityType.newBuilder().setType(1))
                                    .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                    .setOldCapacity(124)
                                    .setNewCapacity(456)
                        )
                        .build())
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
