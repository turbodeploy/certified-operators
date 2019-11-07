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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetStatsDataRetentionSettingsRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
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
                settingsMapper, settingsManagerMapping, settingsPoliciesService));

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
                .setSettingSpecName(settingSpecName)
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(Float.parseFloat(settingValue)))
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

    @Test
    public void testGetSettingsByUuidPersistenceManager() throws Exception {

        final String managerName = SettingsService.PERSISTENCE_MANAGER;
        final Setting statSetting =
            Setting.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.StatsRetentionDays.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(10)
                    .build())
                .build();
        final SettingApiDTO<String> mappedStatSetting = mock(SettingApiDTO.class);
        final SettingApiDTOPossibilities statPossibilities = mock(SettingApiDTOPossibilities.class);
        when(statPossibilities.getGlobalSetting()).thenReturn(Optional.ofNullable(mappedStatSetting));

        final Setting auditSetting =
            Setting.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.AuditLogRetentionDays.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(10)
                    .build())
                .build();
        final SettingApiDTO<String> mappedAuditSetting = mock(SettingApiDTO.class);
        final SettingApiDTOPossibilities auditPossibilities = mock(SettingApiDTOPossibilities.class);
        when(auditPossibilities.getGlobalSetting()).thenReturn(Optional.ofNullable(mappedAuditSetting));

        when(settingsMapper.toSettingApiDto(statSetting)).thenReturn(statPossibilities);
        when(settingsMapper.toSettingApiDto(auditSetting)).thenReturn(auditPossibilities);

        when (statsRpcSpy.getStatsDataRetentionSettings(
                GetStatsDataRetentionSettingsRequest.getDefaultInstance()))
            .thenReturn(Collections.singletonList(statSetting));

        when (statsRpcSpy.getAuditLogDataRetentionSetting(
                GetAuditLogDataRetentionSettingRequest.getDefaultInstance()))
            .thenReturn(GetAuditLogDataRetentionSettingResponse.newBuilder()
                .setAuditLogRetentionSetting(auditSetting).build());

        SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerMapping.getManagerInfo(eq(managerName))).thenReturn(Optional.of(managerInfo));

        List<? extends SettingApiDTO<?>> settingApiDTOList = settingsService.getSettingsByUuid(managerName);
        assertThat(settingApiDTOList, containsInAnyOrder(mappedStatSetting, mappedAuditSetting));
    }
}
