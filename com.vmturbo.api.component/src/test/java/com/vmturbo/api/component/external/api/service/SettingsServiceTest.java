package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SingleSettingSpecRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class SettingsServiceTest {

    /**
     * Used to test the expected exception type and the exception detail.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SettingServiceMole settingRpcServiceSpy = spy(new SettingServiceMole());

    private SettingServiceBlockingStub settingServiceStub;

    private StatsHistoryServiceBlockingStub statsServiceClient;

    private SettingsService settingsService;

    private StatsHistoryServiceMole statsRpcSpy = spy(new StatsHistoryServiceMole());

    private SettingsMapper settingsMapper = mock(SettingsMapper.class);

    private SettingsManagerMapping settingsManagerMapping = mock(SettingsManagerMapping.class);

    private SettingsPoliciesService settingsPoliciesService = Mockito.mock(SettingsPoliciesService.class);

    private static final int ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;

    private final SettingSpec vmSettingSpec = SettingSpec.newBuilder()
            .setName("moveVM")
            .setDisplayName("Move")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                    .setTiebreaker(SettingTiebreaker.SMALLER)
                    .setEntitySettingScope(EntitySettingScope.newBuilder()
                            .setEntityTypeSet(EntityTypeSet.newBuilder().addEntityType(ENTITY_TYPE))))
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                    .addAllEnumValues(Arrays.asList("DISABLED", "MANUAL"))
                    .setDefault("MANUAL"))
            .build();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(settingRpcServiceSpy, statsRpcSpy);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        settingServiceStub = SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        statsServiceClient = StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());

        settingsService = spy(new SettingsService(settingServiceStub, statsServiceClient,
                settingsMapper, settingsManagerMapping, settingsPoliciesService,
                false, false));

        when(settingRpcServiceSpy.searchSettingSpecs(any()))
                .thenReturn(Collections.singletonList(vmSettingSpec));
    }

    @Captor
    private ArgumentCaptor<List<SettingSpec>> specCaptor;

    @Captor
    ArgumentCaptor<Setting> settingArgumentCaptor;

    /**
     * Verify that a request without a specific manager UUID calls the appropriate mapping method.
     */
    @Test
    public void testGetAllSpecs() throws Exception {
        SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");

        when(settingsMapper.toManagerDtos(anyCollectionOf(SettingSpec.class), any(), any()))
            .thenReturn(Collections.singletonList(mgrDto));

        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(null, null, false);
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getUuid());

        verify(settingsMapper).toManagerDtos(specCaptor.capture(), eq(Optional.empty()), any());
        assertThat(specCaptor.getValue(), containsInAnyOrder(vmSettingSpec));
    }

    /**
     * Verify that a request for specs with a specific manager UUID calls the appropriate mapping
     * method.
     */
    @Test
    public void testGetSingleMgrSpecs() throws Exception {
        final SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");

        final String mgrId = "mgrId";
        when(settingsMapper.toManagerDto(any(), any(), eq(mgrId), any()))
            .thenReturn(Optional.of(mgrDto));
        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(mgrId, null, false);
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getUuid());
        verify(settingsMapper).toManagerDto(specCaptor.capture(), eq(Optional.empty()), eq(mgrId), any());
        assertThat(specCaptor.getValue(), containsInAnyOrder(vmSettingSpec));
    }

    /**
     * Verify that a request for specs with an entity type ignores specs for other entity types.
     */
    @Test
    public void testGetSingleEntityTypeSpecs() throws Exception {
        final SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");
        when(settingsMapper.toManagerDtos(anyCollectionOf(SettingSpec.class), any(), any()))
                .thenReturn(Collections.singletonList(mgrDto));

        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(null, "Container", false);
        verify(settingsMapper).toManagerDtos(specCaptor.capture(), eq(Optional.of("Container")),
            any());
        assertTrue(specCaptor.getValue().isEmpty());
    }

    @Test
    public void testSettingMatchEntityType() {
        final SettingSpec allSettingSpec = SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setAllEntityType(AllEntityType.getDefaultInstance())))
                .build();
        final SettingSpec vmSettingSpec = SettingSpec.newBuilder()
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                    .setEntitySettingScope(EntitySettingScope.newBuilder()
                            .setEntityTypeSet(EntityTypeSet.newBuilder()
                                    .addEntityType(10))))
            .build();

        assertTrue(SettingsService.settingMatchEntityType(vmSettingSpec, null));

        assertTrue(SettingsService.settingMatchEntityType(vmSettingSpec, "VirtualMachine"));

        assertTrue(SettingsService.settingMatchEntityType(allSettingSpec, "VirtualMachine"));

        assertFalse(SettingsService.settingMatchEntityType(
            SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .build(),
            "VirtualMachine"));

        assertFalse(SettingsService.settingMatchEntityType(
            SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
                .build(), "VirtualMachine"));
    }

    /**
     * Test the invocation of the getSettingsByUuid API.
     *
     * @throws Exception exception thrown if anything goes wrong
     */
    @Test
    public void testGetSettingsByUuidGlobalSettings() throws Exception {
        final String settingSpecName = "globalSetting";
        final String managerName = "emailmanager";
        final Setting globalSetting = Setting.newBuilder()
                .setSettingSpecName(settingSpecName)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("no one cares"))
                .build();
        final SettingApiDTO<String> mappedDto = new SettingApiDTO<>();
        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        when(possibilities.getGlobalSetting()).thenReturn(Optional.of(mappedDto));
        when(settingsMapper.toSettingApiDto(globalSetting)).thenReturn(possibilities);

        final SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);

        when(settingsManagerMapping.getManagerInfo(eq(managerName))).thenReturn(Optional.of(managerInfo));
        when(settingsPoliciesService.getSettingsPolicies(true, Collections.emptyList()))
                .thenReturn(Collections.emptyList());
        when(settingRpcServiceSpy.getMultipleGlobalSettings(GetMultipleGlobalSettingsRequest.getDefaultInstance()))
                .thenReturn(Collections.singletonList(globalSetting));

        List<? extends SettingApiDTO<?>> settingApiDTOList = settingsService.getSettingsByUuid(managerName);
        assertThat(settingApiDTOList, containsInAnyOrder(mappedDto));
        verify(settingRpcServiceSpy).getMultipleGlobalSettings(any(GetMultipleGlobalSettingsRequest.class));
    }

    /**
     * Test the invocation of the getSettingsByUuid API for getting default settings.
     * settingsPoliciesService for default settings is invoked, but rpc service
     * (settingRpcServiceSpy) for global settings is not invoked.
     *
     * @throws Exception if any exception happens
     */
    @Test
    public void testGetSettingsByUuidDefaultSettings() throws Exception {
        final String managerName = "marketsettingsmanager";
        final SettingApiDTO<String> mappedDto = new SettingApiDTO<>();
        mappedDto.setUuid("usedIncrement_VMEM");
        mappedDto.setValue("1024.0");
        mappedDto.setEntityType("VirtualMachine");

        final SettingsManagerApiDTO settingsManagerApiDTO = new SettingsManagerApiDTO();
        settingsManagerApiDTO.setUuid(managerName);
        settingsManagerApiDTO.setSettings(Lists.newArrayList(mappedDto));

        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();
        settingsPolicyApiDTO.setSettingsManagers(Lists.newArrayList(settingsManagerApiDTO));

        final SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerMapping.getManagerInfo(eq(managerName))).thenReturn(Optional.of(managerInfo));
        when(settingsPoliciesService.getSettingsPolicies(true, Collections.emptySet(),
                Sets.newHashSet(managerName))).thenReturn(Lists.newArrayList(settingsPolicyApiDTO));

        List<? extends SettingApiDTO<?>> settingApiDTOList = settingsService.getSettingsByUuid(managerName);
        // verify result is as expected
        assertThat(settingApiDTOList, containsInAnyOrder(mappedDto));
        // verify settingsPoliciesService is invoked
        verify(settingsPoliciesService).getSettingsPolicies(eq(true), eq(Collections.emptySet()),
                eq(Sets.newHashSet(managerName)));
        // verify settingRpcServiceSpy is not invoked
        verifyZeroInteractions(settingRpcServiceSpy);
        verifyZeroInteractions(settingsMapper);
    }


    /**
     * Test getSettingsByUuidAndName. getSettingsByUuid should be invoked. if the setting does not
     * exist, IllegalArgumentException should be thrown.
     *
     * @throws Exception if any exception happens
     */
    @Test
    public void testGetSettingsByUuidAndName() throws Exception {
        final String managerName = "marketsettingsmanager";
        final String settingName = "utilTarget";
        final String settingValue = "70.0";
        final SettingApiDTO<String> mappedDto = new SettingApiDTO<>();
        mappedDto.setUuid(settingName);
        mappedDto.setValue(settingValue);

        final SettingsManagerApiDTO settingsManagerApiDTO = new SettingsManagerApiDTO();
        settingsManagerApiDTO.setUuid(managerName);
        settingsManagerApiDTO.setSettings(Lists.newArrayList(mappedDto));

        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();
        settingsPolicyApiDTO.setSettingsManagers(Lists.newArrayList(settingsManagerApiDTO));

        final SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerMapping.getManagerInfo(eq(managerName))).thenReturn(Optional.of(managerInfo));
        when(settingsPoliciesService.getSettingsPolicies(true, Collections.emptySet(),
                Sets.newHashSet(managerName))).thenReturn(Lists.newArrayList(settingsPolicyApiDTO));

        SettingApiDTO<String> result = settingsService.getSettingByUuidAndName(managerName, settingName);

        // verify getSettingsByUuid is invoked
        verify(settingsService).getSettingsByUuid(managerName);
        // verify result is as expected
        assertThat(result.getUuid(), is(settingName));
        assertThat(result.getValue(), is(settingValue));

        // verify IllegalArgumentException is thrown since the setting does not exist
        when(settingsPoliciesService.getSettingsPolicies(true, Collections.emptyList()))
                .thenReturn(Collections.emptyList());
        expectedException.expect(IllegalArgumentException.class);
        settingsService.getSettingByUuidAndName(managerName, "foo");
    }

    /**
     * Test case when settingsService has special configuration (hideExecutionScheduleSettings =
     * true), so all methods should filter out executionSchedule settings).
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testSettingsServiceHidesCertainSettings() throws Exception {
        final SettingsService settingServiceHidesCertainSettings =
                spy(new SettingsService(settingServiceStub, statsServiceClient, settingsMapper,
                        settingsManagerMapping, settingsPoliciesService,
                        true, true));
        final SettingApiDTO<String> actionModeSetting = createActionModeSetting();
        final SettingApiDTO<String> executionScheduleSetting = createExecutionScheduleSetting();

        final SettingsManagerApiDTO automationSettingsManager =
                createAutomationManages(Arrays.asList(actionModeSetting, executionScheduleSetting));
        final String managerName = automationSettingsManager.getUuid();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();
        settingsPolicyApiDTO.setSettingsManagers(Lists.newArrayList(automationSettingsManager));

        final SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerMapping.getManagerInfo(
                eq(automationSettingsManager.getUuid()))).thenReturn(Optional.of(managerInfo));
        when(settingsPoliciesService.getSettingsPolicies(true, Collections.emptySet(),
                Sets.newHashSet(managerName))).thenReturn(Lists.newArrayList(settingsPolicyApiDTO));
        when(settingsMapper.toManagerDto(any(), any(), eq(managerName), any())).thenReturn(
                Optional.of(automationSettingsManager));
        when(settingRpcServiceSpy.searchSettingSpecs(any())).thenReturn(
                Arrays.asList(EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingSpec(),
                    ActionSettingSpecs.getSettingSpec(
                        ActionSettingSpecs.getSubSettingFromActionModeSetting(
                                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds,
                                ActionSettingType.SCHEDULE))));

        final List<? extends SettingApiDTO<?>> filteredSettings =
                settingServiceHidesCertainSettings.getSettingsByUuid(managerName);
        final List<SettingsManagerApiDTO> filteredSettingsSpec =
                settingServiceHidesCertainSettings.getSettingsSpecs(managerName, null, false);

        Assert.assertEquals(1, filteredSettings.size());
        Assert.assertEquals(actionModeSetting.getValue(),
                filteredSettings.iterator().next().getValue());

        // check settings specs
        assertEquals(1, filteredSettingsSpec.size());
        assertEquals(managerName, filteredSettingsSpec.get(0).getUuid());
        verify(settingsMapper).toManagerDto(specCaptor.capture(), eq(Optional.empty()),
                eq(managerName), any());
        assertThat(specCaptor.getValue(), containsInAnyOrder(
                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingSpec()));
    }

    /**
     * Test case when settingsService has configuration when hideExecutionScheduleSettings = false,
     * so all methods should return all settings (including executionSchedule settings).
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testSettingsServiceWithoutRestrictions() throws Exception {
        final SettingApiDTO<String> actionModeSetting = createActionModeSetting();
        final SettingApiDTO<String> executionScheduleSetting = createExecutionScheduleSetting();

        final SettingsManagerApiDTO automationSettingsManager =
                createAutomationManages(Arrays.asList(actionModeSetting, executionScheduleSetting));
        final String managerName = automationSettingsManager.getUuid();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();
        settingsPolicyApiDTO.setSettingsManagers(Lists.newArrayList(automationSettingsManager));

        final SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerMapping.getManagerInfo(eq(managerName))).thenReturn(
                Optional.of(managerInfo));
        when(settingsPoliciesService.getSettingsPolicies(true, Collections.emptySet(),
                Sets.newHashSet(managerName))).thenReturn(Lists.newArrayList(settingsPolicyApiDTO));
        when(settingsMapper.toManagerDto(any(), any(), eq(managerName), any())).thenReturn(
                Optional.of(automationSettingsManager));
        when(settingRpcServiceSpy.searchSettingSpecs(any())).thenReturn(
                Arrays.asList(EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingSpec(),
                    ActionSettingSpecs.getSettingSpec(
                        ActionSettingSpecs.getSubSettingFromActionModeSetting(
                                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds,
                                ActionSettingType.SCHEDULE))));

        final List<? extends SettingApiDTO<?>> allSettings =
                settingsService.getSettingsByUuid(managerName);
        final List<SettingsManagerApiDTO> allSettingsSpecs =
                settingsService.getSettingsSpecs(managerName, null, false);

        Assert.assertEquals(2, allSettings.size());
        Assert.assertEquals(
                Sets.newHashSet(actionModeSetting.getUuid(), executionScheduleSetting.getUuid()),
                allSettings.stream().map(BaseApiDTO::getUuid).collect(Collectors.toSet()));

        // check settings specs
        assertEquals(1, allSettingsSpecs.size());
        assertEquals(managerName, allSettingsSpecs.get(0).getUuid());
        verify(settingsMapper).toManagerDto(specCaptor.capture(), eq(Optional.empty()),
                eq(managerName), any());
        assertThat(specCaptor.getValue(), containsInAnyOrder(
                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingSpec(),
                ActionSettingSpecs.getSettingSpec(
                    ActionSettingSpecs.getSubSettingFromActionModeSetting(
                            EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds,
                            ActionSettingType.SCHEDULE))));
    }

    private static SettingsManagerApiDTO createAutomationManages(List<SettingApiDTO<?>> settings) {
        final String managerName = "automationmanager";
        final SettingsManagerApiDTO settingsManagerApiDTO = new SettingsManagerApiDTO();
        settingsManagerApiDTO.setUuid(managerName);
        settingsManagerApiDTO.setSettings(settings);
        return settingsManagerApiDTO;
    }

    private static SettingApiDTO<String> createActionModeSetting() {
        final String actionModeSettingName =
                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.getSettingName();
        final String actionModeSettingValue = "MANUAL";
        final SettingApiDTO<String> actionModeSetting = new SettingApiDTO<>();
        actionModeSetting.setUuid(actionModeSettingName);
        actionModeSetting.setValue(actionModeSettingValue);
        return actionModeSetting;
    }

    private static SettingApiDTO<String> createExecutionScheduleSetting() {
        final String executionScheduleSettingName =
                ActionSettingSpecs.getSubSettingFromActionModeSetting(
                    EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds, ActionSettingType.SCHEDULE);
        final String executionScheduleSettingValue = "124";
        final SettingApiDTO<String> executionScheduleSetting = new SettingApiDTO<>();
        executionScheduleSetting.setUuid(executionScheduleSettingName);
        executionScheduleSetting.setValue(executionScheduleSettingValue);
        return executionScheduleSetting;
    }

    /**
     * Test the invocation of the putSettingByUuidAndName API.
     *
     * @throws Exception
     */
    @Test
    public void testPutSettingByUuidAndName() throws Exception {
        final String managerName = "emailmanager";
        final String settingSpecName = "smtpPort";
        final String settingValue = "25";

        final SettingApiDTO<String> settingInput = new SettingApiDTO<>();
        settingInput.setValue(settingValue);

        final Setting setting = Setting.newBuilder()
            .setSettingSpecName(settingSpecName)
            .setStringSettingValue(StringSettingValue.newBuilder()
                    .setValue(settingValue))
            .build();
        final SettingApiDTO<String> mappedDto = new SettingApiDTO<>();
        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        when(possibilities.getGlobalSetting()).thenReturn(Optional.of(mappedDto));
        when(settingsMapper.toSettingApiDto(setting)).thenReturn(possibilities);

        when(settingRpcServiceSpy.getGlobalSetting(any())).thenReturn(
            GetGlobalSettingResponse.newBuilder().setSetting(Setting.newBuilder()
                .setSettingSpecName(settingSpecName)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(settingValue)
                        .build()))
                .build());

        when (settingRpcServiceSpy.getSettingSpec(SingleSettingSpecRequest.newBuilder()
                .setSettingSpecName(settingSpecName)
                .build())).thenReturn(
                        SettingSpec.newBuilder()
                                .setNumericSettingValueType(NumericSettingValueType.newBuilder())
                                .build());

        settingsService.putSettingByUuidAndName(managerName, settingSpecName, settingInput);

        verify(settingRpcServiceSpy).getSettingSpec(SingleSettingSpecRequest.newBuilder()
                .setSettingSpecName(settingSpecName)
                .build());
        verify(settingRpcServiceSpy).updateGlobalSetting(UpdateGlobalSettingRequest.newBuilder()
            .addSetting(Setting.newBuilder().setSettingSpecName(settingSpecName)
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(Float.parseFloat(settingValue))))
                .build());
    }

    @Test
    public void testPutSettingByUuidAndNameWithNumericValueException() throws Exception {
        String managerName = "emailmanager";
        String settingSpecName = "smtpPort";
        String settingValue = "abc";
        boolean exceptionThrown = false;

        SettingApiDTO<String> settingInput = new SettingApiDTO<>();
        settingInput.setValue(settingValue);

        when(settingRpcServiceSpy.getGlobalSetting(any())).thenReturn(
            GetGlobalSettingResponse.newBuilder().setSetting(Setting.newBuilder()
                .setSettingSpecName(settingSpecName)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(settingValue)
                        .build()))
                .build());

        when (settingRpcServiceSpy.getSettingSpec(SingleSettingSpecRequest.newBuilder()
                .setSettingSpecName(settingSpecName)
                .build())).thenReturn(
                SettingSpec.newBuilder()
                        .setNumericSettingValueType(NumericSettingValueType.newBuilder())
                        .build());

        try {
            settingsService.putSettingByUuidAndName(managerName, settingSpecName, settingInput);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            exceptionThrown = true;
        }

        assertEquals(true, exceptionThrown);
    }

    @Test
    public void testPutSettingByUuidAndNameWithBooleanValueException() throws Exception {
        String managerName = "emailmanager";
        String settingSpecName = "booleanSetting";
        String settingValue = "abc";
        boolean exceptionThrown = false;

        SettingApiDTO<String> settingInput = new SettingApiDTO<>();
        settingInput.setValue(settingValue);

        when(settingRpcServiceSpy.getGlobalSetting(any())).thenReturn(
            GetGlobalSettingResponse.newBuilder().setSetting(Setting.newBuilder()
                .setSettingSpecName(settingSpecName)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(settingValue)
                        .build()))
                .build());

        when (settingRpcServiceSpy.getSettingSpec(SingleSettingSpecRequest.newBuilder()
                .setSettingSpecName(settingSpecName)
                .build())).thenReturn(
                        SettingSpec.newBuilder()
                                .setBooleanSettingValueType(BooleanSettingValueType.newBuilder())
                                .build());

        try {
            settingsService.putSettingByUuidAndName(managerName, settingSpecName, settingInput);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            e.printStackTrace();
            exceptionThrown = true;
        }

        assertEquals(true, exceptionThrown);
    }
}
