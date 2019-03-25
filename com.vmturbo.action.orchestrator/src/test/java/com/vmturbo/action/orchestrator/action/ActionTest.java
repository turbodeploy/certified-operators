package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.store.EntitiesCache;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
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
    private ActionDTO.Action resizeRecommendation;
    private Action resizeAction;
    private ActionDTO.Action deactivateRecommendation;
    private Action deactivateAction;
    private ActionDTO.Action activateRecommendation;
    private Action activateAction;
    private ActionDTO.Action storageMoveRecommendation;
    private Action storageMoveAction;
    private ActionDTO.Action reconfigureRecommendation;
    private Action reconfigureAction;
    private EntitiesCache entitySettingsCache = mock(EntitiesCache.class);
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(actionStream ->
            actionStream.map(action -> {
                action.getActionTranslation().setPassthroughTranslationSuccess();
                return action;
            })));
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        moveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 22L/*srcId*/, 1/*srcType*/, 33L/*destId*/, 1/*destType*/),
                            SupportLevel.SUPPORTED).build();
        resizeRecommendation = makeRec(makeResizeInfo(11L), SupportLevel.SUPPORTED).build();
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(11L), SupportLevel.SUPPORTED).build();
        activateRecommendation = makeRec(makeActivateInfo(11L), SupportLevel.SUPPORTED).build();
        storageMoveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 44L/*srcId*/, EntityType.STORAGE.getNumber()/*srcType*/,
                            55L/*destId*/, EntityType.STORAGE.getNumber()/*destType*/),
                    SupportLevel.SUPPORTED).build();
        reconfigureRecommendation = makeRec(makeReconfigureInfo(11L, 22L), SupportLevel.SUPPORTED).build();

        when(entitySettingsCache.getSettingsForEntity(anyLong()))
            .thenReturn(Collections.emptyMap());


        moveAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        resizeAction = new Action(resizeRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        deactivateAction = new Action(deactivateRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        activateAction = new Action(activateRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        storageMoveAction =
                new Action(storageMoveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        reconfigureAction =
                new Action(reconfigureRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
    }

    @Test
    public void testGetRecommendation() throws Exception {
        assertEquals(moveRecommendation, moveAction.getRecommendation());
    }

    @Test
    public void testGetActionPlanId() throws Exception {
        assertEquals(actionPlanId, moveAction.getActionPlanId());
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(ActionState.READY, moveAction.getState());
    }

    @Test
    public void testIsReady() throws Exception {
        assertTrue(moveAction.isReady());
        moveAction.receive(new ActionEvent.NotRecommendedEvent(5));
        assertFalse(moveAction.isReady());
    }

    @Test
    public void testGetId() throws Exception {
        assertEquals(moveRecommendation.getId(), moveAction.getId());
    }

    @Test
    public void testExecutionCreatesExecutionStep() throws Exception {
        final long targetId = 7;
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
            .thenReturn(ImmutableMap.of("move", makeSetting("move", ActionMode.MANUAL)));
        moveAction.receive(new ManualAcceptanceEvent("0", targetId));
        Assert.assertTrue(moveAction.getExecutableStep().isPresent());
        Assert.assertEquals(targetId, moveAction.getExecutableStep().get().getTargetId());
    }

    @Test
    public void testBeginExecutionEventStartsExecute() throws Exception {
        final long targetId = 7;
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(ImmutableMap.of("move", makeSetting("move", ActionMode.MANUAL)));

        moveAction.receive(new ManualAcceptanceEvent("0", targetId));
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
    public void testGetModeDefault() {
        //default is MANUAL
        assertThat(moveAction.getMode(), is(ActionMode.MANUAL));
        assertThat(resizeAction.getMode(), is(ActionMode.MANUAL));
        assertThat(activateAction.getMode(), is(ActionMode.MANUAL));
        assertThat(deactivateAction.getMode(), is(ActionMode.MANUAL));
        assertThat(reconfigureAction.getMode(), is(ActionMode.RECOMMEND));
        //default is RECOMMEND for stMove
        assertThat(storageMoveAction.getMode(), is(ActionMode.RECOMMEND));
    }

    @Test
    public void testGetModeSupportLevelShowOnly() {
        //SHOW ONLY support level - no modes above RECOMMEND even though set to AUTOMATIC
        moveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 22L, 1, 33L, 1),
                        SupportLevel.SHOW_ONLY).build();
        moveAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(11L), SupportLevel.SHOW_ONLY).build();
        deactivateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        activateRecommendation =
                makeRec(makeActivateInfo(11L), SupportLevel.SHOW_ONLY).build();
        activateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        resizeRecommendation =
                makeRec(makeResizeInfo(11L), SupportLevel.SHOW_ONLY).build();
        resizeAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        storageMoveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 44L, 2, 55L, 2),
                        SupportLevel.SHOW_ONLY).build();
        storageMoveAction =
                new Action(storageMoveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        reconfigureRecommendation =
                makeRec(makeReconfigureInfo(11L, 22L), SupportLevel.SHOW_ONLY).build();
        reconfigureAction = new Action(reconfigureRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);

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
        assertEquals(resizeAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(activateAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(deactivateAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(storageMoveAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(reconfigureAction.getMode(), ActionMode.RECOMMEND);
    }

    @Test
    public void testGetModeSupportLevelUnsupported(){
        //UNSUPPORTED support level - no modes above DISABLED even though set to RECOMMEND
        moveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 22L, 1, 33L, 1),
                        SupportLevel.UNSUPPORTED).build();
        moveAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        deactivateRecommendation =
                makeRec(makeDeactivateInfo(11L), SupportLevel.UNSUPPORTED).build();
        deactivateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        activateRecommendation =
                makeRec(makeActivateInfo(11L), SupportLevel.UNSUPPORTED).build();
        activateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        resizeRecommendation = makeRec(makeResizeInfo(11L), SupportLevel.UNSUPPORTED).build();
        resizeAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        storageMoveRecommendation =
                makeRec(TestActionBuilder.makeMoveInfo(11L, 44L, 2, 55L, 2),
                        SupportLevel.UNSUPPORTED).build();
        storageMoveAction =
                new Action(storageMoveRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
        reconfigureRecommendation =
                makeRec(makeReconfigureInfo(11L, 22L), SupportLevel.UNSUPPORTED).build();
        reconfigureAction =
                new Action(reconfigureRecommendation, entitySettingsCache, actionPlanId, actionModeCalculator);

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
        assertEquals(moveAction.getMode(), ActionMode.DISABLED);
        assertEquals(resizeAction.getMode(), ActionMode.DISABLED);
        assertEquals(activateAction.getMode(), ActionMode.DISABLED);
        assertEquals(deactivateAction.getMode(), ActionMode.DISABLED);
        assertEquals(storageMoveAction.getMode(), ActionMode.DISABLED);
        assertEquals(reconfigureAction.getMode(), ActionMode.DISABLED);
    }

    @Test
    public void testGetModeSupportLevelSupported() {
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

        assertEquals(resizeAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(activateAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(deactivateAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(moveAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(storageMoveAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(reconfigureAction.getMode(), ActionMode.RECOMMEND);
    }

    public static ActionDTO.Action.Builder makeRec(ActionInfo.Builder infoBuilder,
                                             final SupportLevel supportLevel) {
        return ActionDTO.Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setImportance(0)
                .setExecutable(true)
                .setSupportingLevel(supportLevel)
                .setInfo(infoBuilder).setExplanation(Explanation.newBuilder().build());
    }

    private ActionInfo.Builder makeResizeInfo(long targetId) {
        return ActionInfo.newBuilder().setResize(Resize.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(0).build())
                .setNewCapacity(20)
                .setOldCapacity(10)
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId)));
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
