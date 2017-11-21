package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Unit test for {@link Action}s.
 */
public class ActionTest {

    final long actionPlanId = 1;
    private ActionDTO.Action moveRecommendation;
    private Action moveAction;
    private ActionDTO.Action resizeRecommendation;
    private Action resizeAction;
    private ActionDTO.Action deactivateRecommendation;
    private Action deactivateAction;
    private ActionDTO.Action activateRecommendation;
    private Action activateAction;
    private EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        moveRecommendation = makeAction(makeMoveInfo( 11L, 22L, 33L), SupportLevel.SUPPORTED).build();
        resizeRecommendation = makeAction(makeResizeInfo(11L), SupportLevel.SUPPORTED).build();
        deactivateRecommendation = makeAction(makeDeactivateInfo(11L), SupportLevel.SUPPORTED).build();
        activateRecommendation = makeAction(makeActivateInfo(11L), SupportLevel.SUPPORTED).build();

        when(entitySettingsCache.getSettingsForEntity(anyLong()))
            .thenReturn(Collections.emptyList());
        moveAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);
        resizeAction = new Action(resizeRecommendation, entitySettingsCache, actionPlanId);
        deactivateAction = new Action(deactivateRecommendation, entitySettingsCache, actionPlanId);
        activateAction = new Action(activateRecommendation, entitySettingsCache, actionPlanId);
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
            .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.MANUAL)));
        moveAction.receive(new ManualAcceptanceEvent(0L, targetId));
        Assert.assertTrue(moveAction.getExecutableStep().isPresent());
        Assert.assertEquals(targetId, moveAction.getExecutableStep().get().getTargetId());
    }

    @Test
    public void testBeginExecutionEventStartsExecute() throws Exception {
        final long targetId = 7;
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.MANUAL)));

        moveAction.receive(new ManualAcceptanceEvent(0L, targetId));
        moveAction.receive(new BeginExecutionEvent());
    }

    @Test
    public void testDetermineExecutabilityReady() {
        assertTrue(moveAction.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityInProgress() {
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.MANUAL)));

        moveAction.receive(new ManualAcceptanceEvent(0L, 24L));

        assertFalse(moveAction.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityNotExecutable() {
        final ActionDTO.Action recommendation = moveAction.getRecommendation().toBuilder()
            .setExecutable(false).build();
        final Action notExecutable = new Action(recommendation, 1);

        assertFalse(notExecutable.determineExecutability());
    }

    @Test
    public void testGetModeSpecNameCaseInsensitive(){
        //prefix for setting spec is case insensitive
        List<Setting> settingsList = Arrays.asList(
                makeSetting("MoVe", ActionMode.AUTOMATIC),
                makeSetting("ReSiZe", ActionMode.AUTOMATIC),
                makeSetting("SuSpEnD", ActionMode.AUTOMATIC),
                makeSetting("AcTiVaTe", ActionMode.AUTOMATIC)
        );
        when(entitySettingsCache.getSettingsForEntity(eq(11L))).thenReturn(settingsList);

        assertEquals(moveAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(resizeAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(deactivateAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(activateAction.getMode(), ActionMode.AUTOMATIC);
    }

    @Test
    public void testGetModeChoosesStrictestValidSetting(){
        //if an entity has multiple settings, the strictest applicable settings is the one that applies
        //THIS SHOULD NEVER HAPPEN
        //(but if it does, this is how we deal with it)
        List<Setting> settingsList = Arrays.asList(
                makeSetting("resize", ActionMode.RECOMMEND),
                makeSetting("resize", ActionMode.DISABLED),
                makeSetting("activate", ActionMode.MANUAL),
                makeSetting("activate", ActionMode.AUTOMATIC),
                makeSetting("suspend", ActionMode.AUTOMATIC),
                makeSetting("suspend", ActionMode.DISABLED),
                makeSetting("move", ActionMode.MANUAL),
                makeSetting("move", ActionMode.RECOMMEND)
        );
        when(entitySettingsCache.getSettingsForEntity(eq(11L))).thenReturn(settingsList);

        assertEquals(moveAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(resizeAction.getMode(), ActionMode.DISABLED);
        assertEquals(deactivateAction.getMode(), ActionMode.DISABLED);
        assertEquals(activateAction.getMode(), ActionMode.MANUAL);
    }

    @Test
    public void testGetModeDefault(){
        //default is MANUAL
        assertEquals(moveAction.getMode(), ActionMode.MANUAL);
        assertEquals(resizeAction.getMode(), ActionMode.MANUAL);
        assertEquals(activateAction.getMode(), ActionMode.MANUAL);
        assertEquals(deactivateAction.getMode(), ActionMode.MANUAL);
    }

    @Test
    public void testGetModeSupportLevelShowOnly(){
        //SHOW ONLY support level - no modes above RECOMMEND even though set to AUTOMATIC
        moveRecommendation = makeAction(makeMoveInfo(11L, 22L, 33L), SupportLevel.SHOW_ONLY).build();
        moveAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);
        deactivateRecommendation = makeAction(makeMoveInfo(11L, 22L, 33L), SupportLevel.SHOW_ONLY).build();
        deactivateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);
        activateRecommendation = makeAction(makeMoveInfo(11L, 22L, 33L), SupportLevel.SHOW_ONLY).build();
        activateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);
        resizeRecommendation = makeAction(makeMoveInfo(11L, 22L, 33L), SupportLevel.SHOW_ONLY).build();
        resizeAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);

        List<Setting> settingsList = Arrays.asList(
                makeSetting("resize", ActionMode.AUTOMATIC),
                makeSetting("move", ActionMode.AUTOMATIC),
                makeSetting("activate", ActionMode.AUTOMATIC),
                makeSetting("suspend", ActionMode.AUTOMATIC)
        );

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(settingsList);
        assertEquals(moveAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(resizeAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(activateAction.getMode(), ActionMode.RECOMMEND);
        assertEquals(deactivateAction.getMode(), ActionMode.RECOMMEND);
    }

    @Test
    public void testGetModeSupportLevelUnsupported(){
        //UNSUPPORTED support level - no modes above DISABLED even though set to RECOMMEND
        moveRecommendation = makeAction(makeMoveInfo(11L, 22L, 33L), SupportLevel.UNSUPPORTED).build();
        moveAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);
        deactivateRecommendation = makeAction(makeDeactivateInfo(11L), SupportLevel.UNSUPPORTED).build();
        deactivateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);
        activateRecommendation = makeAction(makeActivateInfo(11L), SupportLevel.UNSUPPORTED).build();
        activateAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);
        resizeRecommendation = makeAction(makeResizeInfo(11L), SupportLevel.UNSUPPORTED).build();
        resizeAction = new Action(moveRecommendation, entitySettingsCache, actionPlanId);

        List<Setting> settingsList = Arrays.asList(
                makeSetting("resize", ActionMode.RECOMMEND),
                makeSetting("move", ActionMode.RECOMMEND),
                makeSetting("activate", ActionMode.RECOMMEND),
                makeSetting("suspend", ActionMode.RECOMMEND)
        );

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(settingsList);
        assertEquals(moveAction.getMode(), ActionMode.DISABLED);
        assertEquals(resizeAction.getMode(), ActionMode.DISABLED);
        assertEquals(activateAction.getMode(), ActionMode.DISABLED);
        assertEquals(deactivateAction.getMode(), ActionMode.DISABLED);
    }

    @Test
    public void testGetModeSupportLevelSupported(){
        //SUPPORTED support level - all modes work

        List<Setting> settingsList = Arrays.asList(
            makeSetting("resize", ActionMode.AUTOMATIC),
            makeSetting("move", ActionMode.AUTOMATIC),
            makeSetting("activate", ActionMode.AUTOMATIC),
            makeSetting("suspend", ActionMode.AUTOMATIC)
        );

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(settingsList);

        assertEquals(resizeAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(activateAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(deactivateAction.getMode(), ActionMode.AUTOMATIC);
        assertEquals(moveAction.getMode(), ActionMode.AUTOMATIC);
    }

    private ActionDTO.Action.Builder makeAction(ActionInfo.Builder infoBuilder,
                                          final SupportLevel supportLevel) {
        return ActionDTO.Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setImportance(0)
                .setExecutable(true)
                .setSupportingLevel(supportLevel)
                .setInfo(infoBuilder).setExplanation(Explanation.newBuilder().build());
    }

    private ActionInfo.Builder makeMoveInfo(long targetId, long sourceId, long destinationId) {
        return ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTargetId(targetId)
                .setSourceId(sourceId)
                .setDestinationId(destinationId));
    }

    private ActionInfo.Builder makeResizeInfo(long targetId) {
        return ActionInfo.newBuilder().setResize(Resize.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(0).build())
                .setNewCapacity(20)
                .setOldCapacity(10)
                .setTargetId(targetId));
    }

    private ActionInfo.Builder makeDeactivateInfo(long targetId) {
        return ActionInfo.newBuilder().setDeactivate(Deactivate.newBuilder()
                .setTargetId(targetId)
                .addTriggeringCommodities(CommodityType.newBuilder().setType(0).build()));
    }

    private ActionInfo.Builder makeActivateInfo(long targetId) {
        return ActionInfo.newBuilder().setActivate(Activate.newBuilder()
                .setTargetId(targetId)
                .addTriggeringCommodities(CommodityType.newBuilder().setType(0).build()));
    }

    private Setting makeSetting(String specName, ActionMode mode) {
        return Setting.newBuilder().setSettingSpecName(specName)
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build();
    }
}