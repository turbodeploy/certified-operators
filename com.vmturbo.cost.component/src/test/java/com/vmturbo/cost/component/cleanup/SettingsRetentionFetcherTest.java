package com.vmturbo.cost.component.cleanup;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.cost.component.cleanup.RetentionDurationFetcher.BoundedDuration;

public class SettingsRetentionFetcherTest {

    /**
     * Setting the grpc test server.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private final SettingServiceMole settingServiceMole = spy(new SettingServiceMole());

    private final SettingSpec settingSpec = SettingSpec.newBuilder()
            .setName("settingRetentionFetcherTestName")
            .setDisplayName("settingRetentionFetcherTestDisplayName")
            .setGlobalSettingSpec(GlobalSettingSpec.newBuilder())
            .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                    .setMin(1f)
                    .setMax(90f)
                    .setDefault(30f))
            .build();

    private SettingServiceBlockingStub settingService;

    @Before
    public void setup() throws Exception {
        // Generate a unique in-process server name.
        final String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(
                InProcessServerBuilder
                        .forName(serverName)
                        .directExecutor()
                        .addService(settingServiceMole)
                        .build()
                        .start());

        // Create a client channel and register for automatic graceful shutdown.
        final ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder
                        .forName(serverName)
                        .directExecutor()
                        .build());

        settingService = SettingServiceGrpc.newBlockingStub(channel);
    }

    @Test
    public void testNoCaching() {

        // setup the fetcher
        final TemporalUnit retentionUnit = ChronoUnit.DAYS;
        final SettingsRetentionFetcher retentionFetcher = new SettingsRetentionFetcher(
                settingService,
                settingSpec,
                retentionUnit,
                Duration.ZERO);

        // setup the setting service response
        final float settingResponseValue = 45f;
        final GetGlobalSettingResponse globalSettingResponse = GetGlobalSettingResponse.newBuilder()
                .setSetting(Setting.newBuilder()
                        .setSettingSpecName(settingSpec.getName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(settingResponseValue)))
                .build();
        when(settingServiceMole.getGlobalSetting(any())).thenReturn(globalSettingResponse);

        final BoundedDuration actualRetentionDuration = retentionFetcher.getRetentionDuration();
        final BoundedDuration expectedRetentionDuration = ImmutableBoundedDuration.builder()
                .unit(retentionUnit)
                .amount((long)settingResponseValue)
                .build();

        assertThat(actualRetentionDuration, equalTo(expectedRetentionDuration));
    }
}
