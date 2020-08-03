package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for {@link ReversibilitySettingFetcher}.
 */
public class ReversibilitySettingFetcherTest {

    private SettingPolicyServiceMole settingPolicyServiceMole = spy(new SettingPolicyServiceMole());

    /**
     * GRPC test server which can be used to mock RPC calls.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingPolicyServiceMole);

    private ReversibilitySettingFetcher settingFetcher;

    /**
     * Setup test environment.
     *
     * @throws IOException If there is an error starting up the GRPC test server.
     */
    @Before
    public void setup() throws IOException {
        grpcTestServer.start();
        final SettingPolicyServiceBlockingStub settingPolicyService =
                SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        settingFetcher = new ReversibilitySettingFetcher(settingPolicyService);
    }

    /**
     * Unit test for {@link ReversibilitySettingFetcher#getEntityOidsWithReversibilityPreferred()}
     * method.
     */
    @Test
    public void testGetEntityOidsWithReversibilityPreferred() {
        // Arrange
        final long oidWithReversibilityOn = 1L;
        final long oidWithReversibilityOff = 2L;
        final GetEntitySettingsResponse response = GetEntitySettingsResponse.newBuilder()
                .addSettingGroup(EntitySettingGroup.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                        .setValue(false)
                                        .build())
                                .build())
                        .addEntityOids(oidWithReversibilityOn)
                        .build())
                .addSettingGroup(EntitySettingGroup.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                        .setValue(true)
                                        .build())
                                .build())
                        .addEntityOids(oidWithReversibilityOff)
                        .build())
                .build();
        when(settingPolicyServiceMole.getEntitySettings(any()))
                .thenReturn(ImmutableList.of(response));

        // Act
        final Set<Long> result = settingFetcher.getEntityOidsWithReversibilityPreferred();

        // Assert
        final Set<Long> expected = ImmutableSet.of(1L);
        assertEquals(expected, result);
    }
}
