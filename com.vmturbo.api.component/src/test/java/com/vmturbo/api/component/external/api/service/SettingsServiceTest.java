package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingApiInputDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
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
import com.vmturbo.components.common.mail.MailConfiguration;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class SettingsServiceTest {

    private SettingServiceMole settingRpcServiceSpy = spy(new SettingServiceMole());

    private SettingServiceBlockingStub settingServiceStub;

    private StatsHistoryServiceBlockingStub statsServiceClient;

    private SettingsService settingsService;

    private StatsHistoryServiceMole statsRpcSpy = spy(new StatsHistoryServiceMole());

    private SettingsMapper settingsMapper = mock(SettingsMapper.class);

    private SettingsManagerMapping settingsManagerMapping = mock(SettingsManagerMapping.class);

    private static final int ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final String ENTITY_TYPE_STR = ServiceEntityMapper.toUIEntityType(ENTITY_TYPE);

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

        settingsService =
            new SettingsService(settingServiceStub, statsServiceClient, settingsMapper,
                settingsManagerMapping);

        when(settingRpcServiceSpy.searchSettingSpecs(any()))
                .thenReturn(Collections.singletonList(vmSettingSpec));
    }

    @Captor
    private ArgumentCaptor<List<SettingSpec>> specCaptor;

    @Captor
    ArgumentCaptor<Setting> settingArgumentCaptor;

    @Test
    public void testGetSpecsEntityTypeApplied() throws Exception {
        final SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");
        final SettingApiDTO settingDto = new SettingApiDTO();
        mgrDto.setSettings(Collections.singletonList(settingDto));

        when(settingsMapper.toManagerDtos(any()))
                .thenReturn(Collections.singletonList(mgrDto));

        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(null, ENTITY_TYPE_STR, false);
        assertThat(result.size(), is(1));
        SettingsManagerApiDTO retDto = result.get(0);
        assertThat(retDto.getSettings(), notNullValue());
        assertThat(retDto.getSettings().get(0).getEntityType(), is(ENTITY_TYPE_STR));
    }

    /**
     * Verify that a request without a specific manager UUID calls the appropriate mapping method.
     */
    @Test
    public void testGetAllSpecs() throws Exception {
        SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");

        when(settingsMapper.toManagerDtos(any()))
            .thenReturn(Collections.singletonList(mgrDto));

        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(null, null, false);
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getUuid());

        verify(settingsMapper).toManagerDtos(specCaptor.capture());
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
        when(settingsMapper.toManagerDto(any(), eq(mgrId)))
            .thenReturn(Optional.of(mgrDto));
        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(mgrId, null, false);
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getUuid());
        verify(settingsMapper).toManagerDto(specCaptor.capture(), eq(mgrId));
        assertThat(specCaptor.getValue(), containsInAnyOrder(vmSettingSpec));
    }

    /**
     * Verify that a request for specs with an entity type ignores specs for other entity types.
     */
    @Test
    public void testGetSingleEntityTypeSpecs() throws Exception {
        final SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");
        when(settingsMapper.toManagerDtos(any()))
                .thenReturn(Collections.singletonList(mgrDto));

        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(null, "Container", false);
        verify(settingsMapper).toManagerDtos(specCaptor.capture());
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
     * @throws Exception
     */
    @Test
    public void testGetSettingsByUuid() throws Exception {
        String managerName = "emailmanager";
        List<Setting> settings = getSettingsList();

        SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);

        when(settingsManagerMapping.getManagerInfo(eq(managerName))).thenReturn(Optional.of(managerInfo));

        when(settingRpcServiceSpy.getMultipleGlobalSettings(GetMultipleGlobalSettingsRequest.getDefaultInstance()))
                .thenReturn(settings);

        List<SettingApiDTO> settingApiDTOList = settingsService.getSettingsByUuid(managerName);
        assertEquals(settings.size(), settingApiDTOList.size());
        verify(settingRpcServiceSpy).getMultipleGlobalSettings(any(GetMultipleGlobalSettingsRequest.class));
    }

    private List<Setting> getSettingsList() {
        List<Setting> settingsList = new ArrayList<>();

        settingsList.add(Setting.newBuilder().setSettingSpecName("smtpServer")
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("smtp.gmail.com")
                        .build())
                .build());

        settingsList.add(Setting.newBuilder().setSettingSpecName("smtpPort")
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(25f)
                        .build())
                .build());

        settingsList.add(Setting.newBuilder().setSettingSpecName("SMTP_ENCRYPTION")
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(MailConfiguration.EncryptionType.SSL.name())
                        .build())
                .build());

        return settingsList;
    }

    /**
     * Test the invocation of the putSettingByUuidAndName API.
     *
     * @throws Exception
     */
    @Test
    public void testPutSettingByUuidAndName() throws Exception {
        String managerName = "emailmanager";
        String settingSpecName = "smtpPort";
        String settingValue = "25";

        SettingApiInputDTO settingInput = new SettingApiInputDTO();
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

        SettingApiInputDTO settingInput = new SettingApiInputDTO();
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

        SettingApiInputDTO settingInput = new SettingApiInputDTO();
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

        String managerName = SettingsService.PERSISTENCE_MANAGER;
        Setting statSetting =
            Setting.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.StatsRetentionDays.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(10)
                    .build())
                .build();
        Setting auditSetting =
            Setting.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.AuditLogRetentionDays.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(10)
                    .build())
                .build();
        when (statsRpcSpy.getStatsDataRetentionSettings(
                GetStatsDataRetentionSettingsRequest.getDefaultInstance()))
            .thenReturn(Collections.singletonList(statSetting));

        when (statsRpcSpy.getAuditLogDataRetentionSetting(
                GetAuditLogDataRetentionSettingRequest.getDefaultInstance()))
            .thenReturn(GetAuditLogDataRetentionSettingResponse.newBuilder()
                .setAuditLogRetentionSetting(auditSetting).build());

        SettingsManagerInfo managerInfo = mock(SettingsManagerInfo.class);
        when(settingsManagerMapping.getManagerInfo(eq(managerName))).thenReturn(Optional.of(managerInfo));

        List<SettingApiDTO> settingApiDTOList = settingsService.getSettingsByUuid(managerName);
        assertThat(settingApiDTOList.size(), equalTo(2));
    }
}
