package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link Action}s.
 */
public class ActionTest {

    private final long actionPlanId = 1;
    private ActionDTO.Action moveRecommendation;
    private Action moveAction;
    private ActionDTO.Action vmResizeRecommendation;
    private ActionDTO.Action storageResizeRecommendation;
    private Action resizeStorageAction;
    private Action resizeVmAction;
    private ActionDTO.Action deactivateRecommendation;
    private Action deactivateAction;
    private ActionDTO.Action activateRecommendation;
    private Action activateAction;
    private ActionDTO.Action storageMoveRecommendation;
    private Action storageMoveAction;
    private ActionDTO.Action reconfigureRecommendation;
    private Action reconfigureAction;
    private EntitiesAndSettingsSnapshot entitySettingsCache = mock(EntitiesAndSettingsSnapshot.class);

    private ActionModeCalculator actionModeCalculator = spy(new ActionModeCalculator());

    @Before
    public void setup() throws UnsupportedActionException {
        IdentityGenerator.initPrefix(0);

        moveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 22L/*srcId*/, 1/*srcType*/, 33L/*destId*/, 1/*destType*/),
                            SupportLevel.SUPPORTED).build();
        vmResizeRecommendation = makeRec(makeVmResizeInfo(11L), SupportLevel.SUPPORTED).build();
        storageResizeRecommendation =
            makeRec(makeStorageResizeInfo(11L), SupportLevel.SUPPORTED).build();
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(11L), SupportLevel.SUPPORTED).build();
        activateRecommendation = makeRec(makeActivateInfo(11L), SupportLevel.SUPPORTED).build();
        storageMoveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 44L/*srcId*/, EntityType.STORAGE.getNumber()/*srcType*/,
                            55L/*destId*/, EntityType.STORAGE.getNumber()/*destType*/),
                    SupportLevel.SUPPORTED).build();
        reconfigureRecommendation = makeRec(makeReconfigureInfo(11L, 22L), SupportLevel.SUPPORTED).build();


        when(entitySettingsCache.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        setEntitiesOIDs();
        moveAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        moveAction.getActionTranslation().setPassthroughTranslationSuccess();
        moveAction.refreshAction(entitySettingsCache);
        resizeStorageAction = new Action(storageResizeRecommendation, actionPlanId,
            actionModeCalculator);
        resizeStorageAction.getActionTranslation().setPassthroughTranslationSuccess();
        resizeStorageAction.refreshAction(entitySettingsCache);
        resizeVmAction = new Action(vmResizeRecommendation, actionPlanId, actionModeCalculator);
        resizeVmAction.getActionTranslation().setPassthroughTranslationSuccess();
        resizeVmAction.refreshAction(entitySettingsCache);
        deactivateAction = new Action(deactivateRecommendation, actionPlanId, actionModeCalculator);
        deactivateAction.getActionTranslation().setPassthroughTranslationSuccess();
        deactivateAction.refreshAction(entitySettingsCache);
        activateAction = new Action(activateRecommendation, actionPlanId, actionModeCalculator);
        activateAction.getActionTranslation().setPassthroughTranslationSuccess();
        activateAction.refreshAction(entitySettingsCache);
        storageMoveAction =
                new Action(storageMoveRecommendation, actionPlanId, actionModeCalculator);
        storageMoveAction.getActionTranslation().setPassthroughTranslationSuccess();
        storageMoveAction.refreshAction(entitySettingsCache);
        reconfigureAction =
                new Action(reconfigureRecommendation, actionPlanId, actionModeCalculator);
        reconfigureAction.refreshAction(entitySettingsCache);
        reconfigureAction.getActionTranslation().setPassthroughTranslationSuccess();
    }

    private void setEntitiesOIDs() {
        when(entitySettingsCache.getEntityFromOid(eq(11L)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(11L,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(entitySettingsCache.getEntityFromOid(eq(22L)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(22L,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(entitySettingsCache.getEntityFromOid(eq(33L)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(33L,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(entitySettingsCache.getEntityFromOid(eq(44L)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(44L,
                EntityType.STORAGE.getNumber()));
        when(entitySettingsCache.getEntityFromOid(eq(55L)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(55L,
                EntityType.STORAGE.getNumber()));
    }

    @Test
    public void testGetRecommendation() {
        assertEquals(moveRecommendation, moveAction.getRecommendation());
    }

    @Test
    public void testGetActionPlanId() {
        assertEquals(actionPlanId, moveAction.getActionPlanId());
    }

    @Test
    public void testInitialState() {
        assertEquals(ActionState.READY, moveAction.getState());
    }

    @Test
    public void testIsReady() {
        assertTrue(moveAction.isReady());
        moveAction.receive(new ActionEvent.NotRecommendedEvent(5));
        assertFalse(moveAction.isReady());
    }

    @Test
    public void testGetId() {
        assertEquals(moveRecommendation.getId(), moveAction.getId());
    }

    @Test
    public void testExecutionCreatesExecutionStep() {
        final long targetId = 7;
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
            .thenReturn(ImmutableMap.of("move", makeSetting("move", ActionMode.MANUAL)));
        moveAction.receive(new ManualAcceptanceEvent("0", targetId));
        Assert.assertTrue(moveAction.getCurrentExecutableStep().isPresent());
        Assert.assertEquals(targetId, moveAction.getCurrentExecutableStep().get().getTargetId());
    }

    @Test
    public void testBeginExecutionEventStartsExecute() {
        final long targetId = 7;
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(ImmutableMap.of("move", makeSetting("move", ActionMode.MANUAL)));

        moveAction.receive(new ManualAcceptanceEvent("0", targetId));
        moveAction.receive(new PrepareExecutionEvent());
        moveAction.receive(new BeginExecutionEvent());
    }

    @Test
    public void testDetermineExecutabilityReady() {
        assertTrue(moveAction.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityInProgress() {
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(ImmutableMap.of("move", makeSetting("move", ActionMode.MANUAL)));

        moveAction.receive(new ManualAcceptanceEvent("0", 24L));

        assertFalse(moveAction.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityNotExecutable() {
        final ActionDTO.Action recommendation = moveAction.getRecommendation().toBuilder()
            .setExecutable(false).build();
        final Action notExecutable = new Action(recommendation, 1, actionModeCalculator);

        assertFalse(notExecutable.determineExecutability());
    }

    @Test
    public void testInvalidateAction() throws UnsupportedActionException {
        doReturn(ActionMode.MANUAL).when(actionModeCalculator)
            .calculateActionMode(moveAction, entitySettingsCache);

        assertThat(moveAction.getMode(), is(ActionMode.MANUAL));

        // Action mode calculator should have been called the first call to getMode()
        verify(actionModeCalculator, times(1))
            .calculateActionMode(moveAction, entitySettingsCache);

        // Subsequent calls to getMode shouldn't fall through to the action mode calculator.
        assertThat(moveAction.getMode(), is(ActionMode.MANUAL));
        assertThat(moveAction.getMode(), is(ActionMode.MANUAL));
        verify(actionModeCalculator, times(1))
            .calculateActionMode(moveAction, entitySettingsCache);

        // Invalidate
        moveAction.refreshAction(entitySettingsCache);

        // The next call to getMode() should result in another call to actionModeCalculator
        assertThat(moveAction.getMode(), is(ActionMode.MANUAL));
        verify(actionModeCalculator, times(2)).calculateActionMode(moveAction, entitySettingsCache);
    }

    @Test
    public void testGetModeDefault() {
        //default is MANUAL
        assertThat(moveAction.getMode(), is(ActionMode.MANUAL));
        assertThat(activateAction.getMode(), is(ActionMode.MANUAL));
        assertThat(deactivateAction.getMode(), is(ActionMode.MANUAL));
        assertThat(reconfigureAction.getMode(), is(ActionMode.RECOMMEND));
        assertThat(resizeStorageAction.getMode(), is(ActionMode.MANUAL));
        //default is RECOMMEND for stMove
        assertThat(storageMoveAction.getMode(), is(ActionMode.RECOMMEND));
        // For vms and commodity types that are not vmem, mem, cpu or vcpu the default is RECOMMEND
        assertThat(resizeVmAction.getMode(), is(ActionMode.RECOMMEND));
    }

    @Test
    public void testGetModeSupportLevelShowOnly() {
        //SHOW ONLY support level - no modes above RECOMMEND even though set to AUTOMATIC
        moveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 22L, 1, 33L, 1),
                        SupportLevel.SHOW_ONLY).build();
        moveAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(11L), SupportLevel.SHOW_ONLY).build();
        deactivateAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        activateRecommendation =
                makeRec(makeActivateInfo(11L), SupportLevel.SHOW_ONLY).build();
        activateAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        storageResizeRecommendation =
                makeRec(makeStorageResizeInfo(11L), SupportLevel.SHOW_ONLY).build();
        resizeStorageAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        storageMoveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 44L, 2, 55L, 2),
                        SupportLevel.SHOW_ONLY).build();
        storageMoveAction =
                new Action(storageMoveRecommendation, actionPlanId, actionModeCalculator);
        reconfigureRecommendation =
                makeRec(makeReconfigureInfo(11L, 22L), SupportLevel.SHOW_ONLY).build();
        reconfigureAction = new Action(reconfigureRecommendation, actionPlanId, actionModeCalculator);

        Map<String, Setting> settings = ImmutableMap.<String, Setting>builder()
                .put("resize", makeSetting("resize", ActionMode.AUTOMATIC))
                .put("move", makeSetting("move", ActionMode.AUTOMATIC))
                .put("storageMove", makeSetting("storageMove", ActionMode.AUTOMATIC))
                .put("activate", makeSetting("activate", ActionMode.AUTOMATIC))
                .put("reconfigure", makeSetting("reconfigure", ActionMode.RECOMMEND))
                .put("suspend", makeSetting("suspend", ActionMode.AUTOMATIC))
                .build();

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(settings);
        assertEquals(moveAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(resizeStorageAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(activateAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(deactivateAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(storageMoveAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(reconfigureAction.getMode(), ActionMode.RECOMMEND);
    }

    @Test
    public void testGetModeSupportLevelUnsupported() throws UnsupportedActionException {
        //UNSUPPORTED support level - no modes above DISABLED even though set to RECOMMEND
        moveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 22L, 1, 33L, 1),
                        SupportLevel.UNSUPPORTED).build();
        moveAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(11L), SupportLevel.UNSUPPORTED).build();
        deactivateAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        activateRecommendation =
                makeRec(makeActivateInfo(11L), SupportLevel.UNSUPPORTED).build();
        activateAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        storageResizeRecommendation = makeRec(makeStorageResizeInfo(11L), SupportLevel.UNSUPPORTED).build();
        resizeStorageAction = new Action(moveRecommendation, actionPlanId, actionModeCalculator);
        storageMoveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 44L, 2, 55L, 2),
                        SupportLevel.UNSUPPORTED).build();
        storageMoveAction =
                new Action(storageMoveRecommendation, actionPlanId, actionModeCalculator);
        reconfigureRecommendation =
                makeRec(makeReconfigureInfo(11L, 22L), SupportLevel.UNSUPPORTED).build();
        reconfigureAction =
                new Action(reconfigureRecommendation, actionPlanId, actionModeCalculator);

        Map<String, Setting> settings = ImmutableMap.<String, Setting>builder()
                .put("resize", makeSetting("resize", ActionMode.RECOMMEND))
                .put("move", makeSetting("move", ActionMode.RECOMMEND))
                .put("storageMove", makeSetting("storageMove", ActionMode.RECOMMEND))
                .put("activate", makeSetting("activate", ActionMode.RECOMMEND))
                .put("reconfigure", makeSetting("reconfigure", ActionMode.RECOMMEND))
                .put("suspend", makeSetting("suspend", ActionMode.RECOMMEND))
                .build();

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(settings);

        moveAction.refreshAction(entitySettingsCache);
        storageMoveAction.refreshAction(entitySettingsCache);
        activateAction.refreshAction(entitySettingsCache);
        deactivateAction.refreshAction(entitySettingsCache);
        storageMoveAction.refreshAction(entitySettingsCache);
        reconfigureAction.refreshAction(entitySettingsCache);
        assertEquals(moveAction.getMode(), ActionMode.DISABLED);
        assertEquals(storageMoveAction.getMode(), ActionMode.DISABLED);
        assertEquals(activateAction.getMode(), ActionMode.DISABLED);
        assertEquals(deactivateAction.getMode(), ActionMode.DISABLED);
        assertEquals(storageMoveAction.getMode(), ActionMode.DISABLED);
        assertEquals(reconfigureAction.getMode(), ActionMode.DISABLED);
    }

    @Test
    public void testGetModeSupportLevelSupported() throws UnsupportedActionException {
        //SUPPORTED support level - all modes work

        Map<String, Setting> settings = ImmutableMap.<String, Setting>builder()
            .put("resize", makeSetting("resize", ActionMode.AUTOMATIC))
            .put("move", makeSetting("move", ActionMode.AUTOMATIC))
            .put("storageMove", makeSetting("storageMove", ActionMode.AUTOMATIC))
            .put("activate", makeSetting("activate", ActionMode.AUTOMATIC))
            .put("reconfigure", makeSetting("reconfigure", ActionMode.RECOMMEND))
            .put("suspend", makeSetting("suspend", ActionMode.AUTOMATIC))
            .build();

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(settings);

        moveAction.refreshAction(entitySettingsCache);
        storageMoveAction.refreshAction(entitySettingsCache);
        activateAction.refreshAction(entitySettingsCache);
        deactivateAction.refreshAction(entitySettingsCache);
        storageMoveAction.refreshAction(entitySettingsCache);
        reconfigureAction.refreshAction(entitySettingsCache);
        assertEquals(storageMoveAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(activateAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(deactivateAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(moveAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(storageMoveAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(reconfigureAction.getMode(), ActionMode.RECOMMEND);
    }

    private static ActionDTO.Action.Builder makeRec(ActionInfo.Builder infoBuilder,
            final SupportLevel supportLevel) {
        return ActionDTO.Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setSupportingLevel(supportLevel)
                .setInfo(infoBuilder).setExplanation(Explanation.newBuilder().build());
    }

    private ActionInfo.Builder makeVmResizeInfo(long targetId) {
        return ActionInfo.newBuilder().setResize(Resize.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(0).build())
                .setNewCapacity(20)
                .setOldCapacity(10)
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId)));
    }

    private ActionInfo.Builder makeStorageResizeInfo(long targetId) {
        return ActionInfo.newBuilder().setResize(Resize.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(0).build())
            .setNewCapacity(20)
            .setOldCapacity(10)
            .setTarget(ActionEntity.newBuilder()
                .setId(targetId)
                // set some fake type for now
                .setType(EntityType.STORAGE.getNumber())
                .build()));
    }

    private ActionInfo.Builder makeDeactivateInfo(long targetId) {
        return ActionInfo.newBuilder().setDeactivate(Deactivate.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addTriggeringCommodities(CommodityType.newBuilder().setType(0).build()));
    }

    private ActionInfo.Builder makeActivateInfo(long targetId) {
        return ActionInfo.newBuilder().setActivate(Activate.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addTriggeringCommodities(CommodityType.newBuilder().setType(0).build()));
    }

    private ActionInfo.Builder makeReconfigureInfo(long targetId, long sourceId) {
        return ActionInfo.newBuilder().setReconfigure(Reconfigure.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .setSource(ActionOrchestratorTestUtils.createActionEntity(sourceId))
                .build());
    }

    private Setting makeSetting(String specName, ActionMode mode) {
        return Setting.newBuilder().setSettingSpecName(specName)
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build();
    }
}
