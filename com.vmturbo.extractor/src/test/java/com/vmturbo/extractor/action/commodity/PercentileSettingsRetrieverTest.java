package com.vmturbo.extractor.action.commodity;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Optional;

import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.PercentileSettingSpecs;
import com.vmturbo.extractor.action.commodity.PercentileSettingsRetriever.PercentileSettings;
import com.vmturbo.extractor.action.commodity.PercentileSettingsRetriever.PercentileSettings.PercentileSetting;

/**
 * Unit tests for {@link PercentileSettingsRetriever}.
 */
public class PercentileSettingsRetrieverTest {

    private SettingPolicyServiceMole spMole = spy(SettingPolicyServiceMole.class);

    /**
     * GRPC test server.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(spMole);

    private PercentileSettingsRetriever percentileSettingsRetriever;

    /**
     * Common setup before each test.
     */
    @Before
    public void setup() {
        percentileSettingsRetriever = new PercentileSettingsRetriever(
                SettingPolicyServiceGrpc.newBlockingStub(server.getChannel()));
    }

    /**
     * Test retrieving and processing the settings.
     */
    @Test
    public void testRetrieveSettings() {
        final long vmId = 1L;
        final LongSet inputIds = LongSets.singleton(vmId);

        final EntitySettingGroup settingGroup1 = EntitySettingGroup.newBuilder()
            .addEntityOids(vmId)
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.MaxObservationPeriodVirtualMachine.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(30)))
            .build();

        final EntitySettingGroup settingGroup2 = EntitySettingGroup.newBuilder()
            .addEntityOids(vmId)
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.PercentileAggressivenessVirtualMachine.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                    .setValue(50)))
            .build();

        doReturn(Collections.singletonList(GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(settingGroup1)
            .addSettingGroup(settingGroup2)
            .build())).when(spMole).getEntitySettings(any());

        final PercentileSettings retrievedSettings = percentileSettingsRetriever.getPercentileSettingsData(inputIds);
        final Optional<PercentileSetting> settings =
            retrievedSettings.getEntitySettings(1L, ApiEntityType.VIRTUAL_MACHINE);
        assertThat(settings.get().getAggresiveness(), is(50));
        assertThat(settings.get().getObservationPeriod(), is(30));

        // Check that the remote request contains the right entity id and setting names.
        final ArgumentCaptor<GetEntitySettingsRequest> reqCaptor = ArgumentCaptor.forClass(GetEntitySettingsRequest.class);
        verify(spMole).getEntitySettings(reqCaptor.capture());
        final GetEntitySettingsRequest req = reqCaptor.getValue();
        assertThat(req.getSettingFilter().getEntitiesList(), contains(1L));
        assertThat(req.getSettingFilter().getSettingNameList(), containsInAnyOrder(PercentileSettingSpecs.settingNames().toArray()));
    }

}