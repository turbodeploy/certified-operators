package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
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
    private Map<Long, List<Setting>> entitySettings = new HashMap<>();

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        recommendation = move(11, 22, 33, SupportLevel.SUPPORTED).build();
        action = new Action(recommendation, entitySettings, actionPlanId);
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
        entitySettings.put(11L, Collections.singletonList(makeMoveVmSetting(ActionMode.MANUAL)));
        action.receive(new ManualAcceptanceEvent(0L, targetId));
        Assert.assertTrue(action.getExecutableStep().isPresent());
        Assert.assertEquals(targetId, action.getExecutableStep().get().getTargetId());
    }

    @Test
    public void testBeginExecutionEventStartsExecute() throws Exception {
        final long targetId = 7;
        entitySettings.put(11L, Collections.singletonList(makeMoveVmSetting(ActionMode.MANUAL)));

        action.receive(new ManualAcceptanceEvent(0L, targetId));
        action.receive(new BeginExecutionEvent());
    }

    @Test
    public void testDetermineExecutabilityReady() {
        assertTrue(action.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityInProgress() {
        entitySettings.put(11L, Collections.singletonList(makeMoveVmSetting(ActionMode.MANUAL)));

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
    public void testGetModeMoveSpecNameCaseInsensitive(){
        //move prefix for setting spec is case insensitive
        entitySettings.put(11L,
                Collections.singletonList(settingNonstandardSpec("MoVeVM", ActionMode.AUTOMATIC)));
        assertEquals(action.getMode(), ActionMode.AUTOMATIC);
    }

    @Test
    public void testGetModeMoveSpecNameNeedsValidPrefix(){
        //suffix of move spec doesn't matter
        //todo: this may change when non-moveVM settings are supported
        Setting move123 = settingNonstandardSpec("move123", ActionMode.RECOMMEND);
        entitySettings.put(11L, Collections.singletonList(move123));
        assertEquals(action.getMode(), ActionMode.RECOMMEND);
        //but an unrecognized spec name invalidates the setting
        entitySettings.put(11L,
                Arrays.asList(settingNonstandardSpec("badName", ActionMode.DISABLED), move123));
        assertEquals(action.getMode(), ActionMode.RECOMMEND);
    }

    @Test
    public void testGetModeChoosesStrictestValidSetting(){

        //when an entity has multiple settings, the strictest of the "move*" settings is the one that applies
        List<Setting> mixedSettings = Arrays.asList(makeMoveVmSetting(ActionMode.MANUAL),
                makeMoveVmSetting(ActionMode.RECOMMEND),
                settingNonstandardSpec("move123", ActionMode.AUTOMATIC),
                settingNonstandardSpec("MovE456", ActionMode.MANUAL),
                settingNonstandardSpec("bad123", ActionMode.DISABLED),
                settingNonstandardSpec("bad456", ActionMode.AUTOMATIC));
        entitySettings.put(11L, mixedSettings);
        assertEquals(action.getMode(), ActionMode.RECOMMEND);

    }

    @Test
    public void testGetModeMoveDefault(){
        //default is MANUAL
        assertEquals(action.getMode(), ActionMode.MANUAL);
    }

    @Test
    public void testGetModeMoveOneSetting(){
        entitySettings.put(11L, Collections.singletonList(makeMoveVmSetting(ActionMode.AUTOMATIC)));
        assertEquals(action.getMode(), ActionMode.AUTOMATIC);
    }

    @Test
    public void testGetModeMoveSupportLevelShowOnly(){
        //SHOW ONLY support level - no modes above RECOMMEND even though set to AUTOMATIC
        recommendation = move(11, 22, 33, SupportLevel.SHOW_ONLY).build();
        entitySettings.put(11L, Collections.singletonList(makeMoveVmSetting(ActionMode.AUTOMATIC)));
        action = new Action(recommendation, entitySettings, actionPlanId);
        assertEquals(action.getMode(), ActionMode.RECOMMEND);
    }

    @Test
    public void testGetModeMoveSupportLevelUnsupported(){
        //UNSUPPORTED support level - no modes above DISABLED even though set to RECOMMEND
        recommendation = move(11, 22, 33, SupportLevel.UNSUPPORTED).build();
        entitySettings.put(11L, Collections.singletonList(makeMoveVmSetting(ActionMode.RECOMMEND)));
        action = new Action(recommendation, entitySettings, actionPlanId);
        assertEquals(action.getMode(), ActionMode.DISABLED);
    }

    private ActionDTO.Action.Builder move(long targetId, long sourceId, long destinationId,
                                          final SupportLevel supportLevel) {
        return ActionDTO.Action.newBuilder()
            .setId(IdentityGenerator.next())
            .setImportance(0)
            .setExecutable(true)
            .setSupportingLevel(supportLevel)
            .setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
                    .setTargetId(targetId)
                    .setSourceId(sourceId)
                    .setDestinationId(destinationId)
            )).setExplanation(Explanation.newBuilder().build());
    }

    private Setting makeMoveVmSetting(ActionMode mode) {
        return Setting.newBuilder().setSettingSpecName("moveVM")
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build();
    }

    private Setting settingNonstandardSpec(String specName, ActionMode mode) {
        return Setting.newBuilder().setSettingSpecName(specName)
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build();
    }
}