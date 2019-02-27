package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
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

        final SettingApiDTO convertableSetting = new SettingApiDTO();
        final SettingApiDTO nonConvertableSetting = new SettingApiDTO();

        final PlanSettingInfo planSettingInfo = mock(PlanSettingInfo.class);
        // Suppose the convertable setting just converts to itself
        when(planSettingInfo.toPlanSetting(eq(convertableSetting))).thenReturn(Optional.of(convertableSetting));
        when(planSettingInfo.toPlanSetting(eq(nonConvertableSetting))).thenReturn(Optional.empty());

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
        apiMgr.setSettings(Arrays.asList(convertableSetting, nonConvertableSetting));

        final List<SettingsManagerApiDTO> convertedMgrs =
                managerMapping.convertToPlanSettingSpecs(Collections.singletonList(apiMgr));

        assertThat(convertedMgrs.size(), is(1));
        // Because of the way we arranged the mocks, we'll return - and have modified - planMgr.
        assertThat(convertedMgrs.get(0), is(planMgr));
        assertThat(planMgr.getSettings().size(), is(1));
        assertThat(planMgr.getSettings().get(0), is(convertableSetting));
    }

    @Test
    public void testConvertToPlanSetting() {
        final SettingApiDTO convertibleSetting = new SettingApiDTO();
        convertibleSetting.setUuid("convertibleSetting");

        // The setting that convertibleSetting will be converted to for plan
        final SettingApiDTO convertedSetting = new SettingApiDTO();

        // This setting has no conversion - it should just pass through.
        final SettingApiDTO passthroughSetting = new SettingApiDTO();
        passthroughSetting.setUuid("passthroughSetting");

        final PlanSettingInfo planSettingInfo = mock(PlanSettingInfo.class);
        when(planSettingInfo.toPlanSetting(convertibleSetting))
                .thenReturn(Optional.of(convertedSetting));
        when(planSettingInfo.toPlanSetting(passthroughSetting))
                .thenReturn(Optional.empty());

        final SettingsManagerInfo mgrInfo = mock(SettingsManagerInfo.class);
        when(mgrInfo.getPlanSettingInfo()).thenReturn(Optional.of(planSettingInfo));

        final SettingsManagerMapping managerMapping = new SettingsManagerMapping(
                ImmutableMap.of("mgr", mgrInfo),
                ImmutableMap.of("convertibleSetting", "mgr",
                        "passthroughSetting", "mgr"));

        final List<SettingApiDTO> convertedSettings =
            managerMapping.convertToPlanSetting(Arrays.asList(convertibleSetting, passthroughSetting));
        assertThat(convertedSettings, containsInAnyOrder(convertedSetting, passthroughSetting));
    }

    @Test
    public void testConvertFromPlanSetting() {
        // This test is similar to convertToPlanSetting - just a different method
        // called on planSettings
        final SettingApiDTO convertibleSetting = new SettingApiDTO();
        convertibleSetting.setUuid("convertibleSetting");

        // The setting that convertibleSetting will be converted to for plan
        final SettingApiDTO convertedSetting = new SettingApiDTO();

        // This setting has no conversion - it should just pass through.
        final SettingApiDTO passthroughSetting = new SettingApiDTO();
        passthroughSetting.setUuid("passthroughSetting");

        final PlanSettingInfo planSettingInfo = mock(PlanSettingInfo.class);
        when(planSettingInfo.fromPlanSetting(convertibleSetting))
                .thenReturn(Optional.of(convertedSetting));
        when(planSettingInfo.fromPlanSetting(passthroughSetting))
                .thenReturn(Optional.empty());

        final SettingsManagerInfo mgrInfo = mock(SettingsManagerInfo.class);
        when(mgrInfo.getPlanSettingInfo()).thenReturn(Optional.of(planSettingInfo));

        final SettingsManagerMapping managerMapping = new SettingsManagerMapping(
                ImmutableMap.of("mgr", mgrInfo),
                ImmutableMap.of("convertibleSetting", "mgr",
                        "passthroughSetting", "mgr"));

        final List<SettingApiDTO> convertedSettings =
                managerMapping.convertFromPlanSetting(Arrays.asList(convertibleSetting, passthroughSetting));
        assertThat(convertedSettings, containsInAnyOrder(convertedSetting, passthroughSetting));
    }

    @Test
    public void testPlanSettingsOutbound() {
        final String json = "{ \"uiToXlValueConversion\" : { \"true\" : \"FOO\", \"false\" : \"BAR\" }," +
                " \"supportedSettingDefaults\" : { \"VirtualMachine\" : { \"setting\" : \"true\" } } }";
        final PlanSettingInfo planSettingInfo = GSON.fromJson(json, PlanSettingInfo.class);
        final SettingApiDTO realSetting = new SettingApiDTO();
        realSetting.setUuid("setting");
        realSetting.setEntityType("VirtualMachine");
        realSetting.setDisplayName("Blah");
        realSetting.setValue("bar");

        final SettingApiDTO planSetting = planSettingInfo.toPlanSetting(realSetting).get();
        assertThat(planSetting.getUuid(), is(realSetting.getUuid()));
        assertThat(planSetting.getEntityType(), is(realSetting.getEntityType()));
        assertThat(planSetting.getDisplayName(), is(realSetting.getDisplayName()));
        assertThat(planSetting.getValue(), is("false"));
        assertThat(planSetting.getDefaultValue(), is("true"));
    }

    @Test
    public void testPlanSettingsInbound() {
        final String json = "{ \"uiToXlValueConversion\" : { \"true\" : \"FOO\", \"false\" : \"BAR\" }," +
                " \"supportedSettingDefaults\" : { \"VirtualMachine\" : { \"setting\" : \"true\" } } }";
        final PlanSettingInfo planSettingInfo = GSON.fromJson(json, PlanSettingInfo.class);

        final SettingApiDTO planSetting = new SettingApiDTO();
        planSetting.setUuid("setting");
        planSetting.setEntityType("VirtualMachine");
        planSetting.setDisplayName("Blah");
        planSetting.setValue("false");

        final SettingApiDTO realSetting = planSettingInfo.fromPlanSetting(planSetting).get();
        assertThat(realSetting.getUuid(), is(realSetting.getUuid()));
        assertThat(realSetting.getEntityType(), is(realSetting.getEntityType()));
        assertThat(realSetting.getDisplayName(), is(realSetting.getDisplayName()));
        assertThat(realSetting.getValue(), is("BAR"));
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
