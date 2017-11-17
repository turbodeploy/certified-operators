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
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Unit test for {@link Action}s.
 */
public class ActionTest {

    final long actionPlanId = 1;
    private ActionDTO.Action recommendation;
    private Action action;
    private EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        recommendation = makeAction(makeMoveInfo( 11L, 22L, 33L), SupportLevel.SUPPORTED).build();
        when(entitySettingsCache.getSettingsForEntity(anyLong()))
            .thenReturn(Collections.emptyList());
        action = new Action(recommendation, entitySettingsCache, actionPlanId);
    }

    @Test
    public void testGetRecommendation() throws Exception {
        assertEquals(recommendation, action.getRecommendation());
    }

    @Test
    public void testGetActionPlanId() throws Exception {
        assertEquals(actionPlanId, action.getActionPlanId());
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(ActionState.READY, action.getState());
    }

    @Test
    public void testIsReady() throws Exception {
        assertTrue(action.isReady());
        action.receive(new ActionEvent.NotRecommendedEvent(5));
        assertFalse(action.isReady());
    }

    @Test
    public void testGetId() throws Exception {
        assertEquals(recommendation.getId(), action.getId());
    }

    @Test
    public void testExecutionCreatesExecutionStep() throws Exception {
        final long targetId = 7;
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
            .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.MANUAL)));
        action.receive(new ManualAcceptanceEvent(0L, targetId));
        Assert.assertTrue(action.getExecutableStep().isPresent());
        Assert.assertEquals(targetId, action.getExecutableStep().get().getTargetId());
    }

    @Test
    public void testBeginExecutionEventStartsExecute() throws Exception {
        final long targetId = 7;
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.MANUAL)));

        action.receive(new ManualAcceptanceEvent(0L, targetId));
        action.receive(new BeginExecutionEvent());
    }

    @Test
    public void testDetermineExecutabilityReady() {
        assertTrue(action.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityInProgress() {
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.MANUAL)));

        action.receive(new ManualAcceptanceEvent(0L, 24L));

        assertFalse(action.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityNotExecutable() {
        final ActionDTO.Action recommendation = action.getRecommendation().toBuilder()
            .setExecutable(false).build();
        final Action notExecutable = new Action(recommendation, 1);

        assertFalse(notExecutable.determineExecutability());
    }

    @Test
    public void testGetModeSpecNameCaseInsensitive(){
        //prefix for setting spec is case insensitive
        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("MoVe", ActionMode.AUTOMATIC)));

        assertEquals(action.getMode(), ActionMode.AUTOMATIC);
    }

    @Test
    public void testGetModeChoosesStrictestValidSetting(){
        //if an entity has multiple settings, the strictest applicable settings is the one that applies
        //THIS SHOULD NEVER HAPPEN
        //(but if it does, this is how we deal with it)
        List<Setting> settingsList = Arrays.asList(
                makeSetting("move", ActionMode.MANUAL),
                makeSetting("move", ActionMode.RECOMMEND)
        );
        when(entitySettingsCache.getSettingsForEntity(eq(11L))).thenReturn(settingsList);

        assertEquals(action.getMode(), ActionMode.RECOMMEND);
    }

    @Test
    public void testGetModeDefault(){
        //default is MANUAL
        assertEquals(action.getMode(), ActionMode.MANUAL);
    }

    @Test
    public void testGetModeSupportLevelShowOnly(){
        //SHOW ONLY support level - no modes above RECOMMEND even though set to AUTOMATIC
        recommendation = makeAction(makeMoveInfo(11L, 22L, 33L), SupportLevel.SHOW_ONLY).build();
        action = new Action(recommendation, entitySettingsCache, actionPlanId);

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.AUTOMATIC)));
        assertEquals(action.getMode(), ActionMode.RECOMMEND);
    }

    @Test
    public void testGetModeSupportLevelUnsupported(){
        //UNSUPPORTED support level - no modes above DISABLED even though set to RECOMMEND
        recommendation = makeAction(makeMoveInfo(11L, 22L, 33L), SupportLevel.UNSUPPORTED).build();
        action = new Action(recommendation, entitySettingsCache, actionPlanId);

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.RECOMMEND)));
        assertEquals(action.getMode(), ActionMode.DISABLED);
    }

    @Test
    public void testGetModeMoveSupportLevelSupported(){
        //SUPPORTED support level - all modes work

        when(entitySettingsCache.getSettingsForEntity(eq(11L)))
                .thenReturn(Collections.singletonList(makeSetting("move", ActionMode.AUTOMATIC)));
        assertEquals(action.getMode(), ActionMode.AUTOMATIC);
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

    private Setting makeSetting(String specName, ActionMode mode) {
        return Setting.newBuilder().setSettingSpecName(specName)
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build();
    }
}