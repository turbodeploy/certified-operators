package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.PlanSettingInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Unit tests for {@link SettingsManagerMappingLoader} and its internal classes.
 */
public class SettingsManagerMappingLoaderTest {

    private static final Gson GSON = ComponentGsonFactory.createGson();

    /**
     * Verify that a manager loaded from JSON file gets used to map
     * a setting spec as expected.
     */
    @Test
    public void testLoadEndToEnd() throws IOException {
        SettingsManagerMappingLoader mapper = new SettingsManagerMappingLoader("settingManagersTest.json");

        assertThat(mapper.getMapping().getManagerUuid("move").get(),
                is("automationmanager"));

        final SettingsManagerInfo mgrInfo =
                mapper.getMapping().getManagerForSetting("move").get();
        assertThat(mgrInfo.getDefaultCategory(), is("Automation"));
        assertThat(mgrInfo.getDisplayName(), is("Action Mode Settings"));
        assertTrue(mgrInfo.getPlanSettingInfo().isPresent());

        // We should return exactly the same object when querying by mgr id.
        assertThat(mapper.getMapping().getManagerInfo("automationmanager").get(), is(mgrInfo));
    }

    @Test
    public void testConvertToPlanSettingSpec() {
        // The converted plan manager
        final SettingsManagerApiDTO planMgr = new SettingsManagerApiDTO();
        planMgr.setUuid("mgr");

        final SettingApiDTO<String> isPlanRelevantSetting = new SettingApiDTO<>();
        final SettingApiDTO<String> isNotPlanRelevantSetting = new SettingApiDTO<>();

        final PlanSettingInfo planSettingInfo = mock(PlanSettingInfo.class);
        when(planSettingInfo.isPlanRelevant(eq(isPlanRelevantSetting))).thenReturn(true);
        when(planSettingInfo.isPlanRelevant(eq(isNotPlanRelevantSetting))).thenReturn(false);

        // The manager info for "mgr"
        final SettingsManagerInfo mgrInfo = mock(SettingsManagerInfo.class);
        when(mgrInfo.newApiDTO(eq("mgr"))).thenReturn(planMgr);
        when(mgrInfo.getPlanSettingInfo()).thenReturn(Optional.of(planSettingInfo));

        final SettingsManagerMapping managerMapping = new SettingsManagerMapping(
                ImmutableMap.of("mgr", mgrInfo),
                // Don't need reverse mappings here
                Collections.emptyMap());

        // The "real" settings manager - before conversion
        final SettingsManagerApiDTO apiMgr = new SettingsManagerApiDTO();
        apiMgr.setUuid("mgr");
        apiMgr.setSettings(Arrays.asList(isPlanRelevantSetting, isNotPlanRelevantSetting));

        final List<SettingsManagerApiDTO> convertedMgrs =
                managerMapping.convertToPlanSettingSpecs(Collections.singletonList(apiMgr));

        assertThat(convertedMgrs.size(), is(1));
        // Because of the way we arranged the mocks, we'll return - and have modified - planMgr.
        assertThat(convertedMgrs.get(0), is(planMgr));
        assertThat(planMgr.getSettings().size(), is(1));
        assertThat(planMgr.getSettings().get(0), is(isPlanRelevantSetting));
    }

    @Test
    public void testPlanSettingsInfoIsPlanRelevantTrue() {
        //GIVEN
        final String json = "{ \"supportedSettingDefaults\" : { \"VirtualMachine\" : { \"resize\" : \"AUTOMATIC\" } } }";
        final PlanSettingInfo planSettingInfo = GSON.fromJson(json, PlanSettingInfo.class);
        final SettingApiDTO<String> realSetting = new SettingApiDTO<>();
        realSetting.setUuid("resize");
        realSetting.setEntityType("VirtualMachine");

        //THEN
        assertTrue(planSettingInfo.isPlanRelevant(realSetting));
    }

    /**
     * Tests that setting is not part of planSettingsInfo
     */
    @Test
    public void testPlanSettingsInfoIsPlanRelevantFalse() {
        //GIVEN
        final String json = "{ \"supportedSettingDefaults\" : { \"VirtualMachine\" : { \"resize\" : \"AUTOMATIC\" } } }";
        final PlanSettingInfo planSettingInfo = GSON.fromJson(json, PlanSettingInfo.class);
        final SettingApiDTO<String> realSetting = new SettingApiDTO<>();
        realSetting.setEntityType("VirtualMachine");
        realSetting.setUuid("suspend");

        //THEN
        assertFalse(planSettingInfo.isPlanRelevant(realSetting));
    }

    @Test
    public void testNewMgrApiDto() {
        final SettingsManagerInfo settingsManagerInfo = new SettingsManagerInfo("name",
                "category",
                Collections.emptySet(),
                mock(PlanSettingInfo.class));
        final SettingsManagerApiDTO mgr = settingsManagerInfo.newApiDTO("mgr");
        assertThat(mgr.getCategory(), is("category"));
        assertThat(mgr.getUuid(), is("mgr"));
        assertThat(mgr.getDisplayName(), is("name"));
    }
}
