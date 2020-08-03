package com.vmturbo.components.common.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

public class RetentionPeriodFetcherTest {

    private SettingServiceMole settingBackend = spy(new SettingServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingBackend);

    private RetentionPeriodFetcher retentionPeriodFetcher;

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private static final int UPDATE_RETENTION_S = 10;

    private static final int NUM_RETAINED_MINS = 130;

    @Before
    public void setup() {
        retentionPeriodFetcher = new RetentionPeriodFetcher(clock, UPDATE_RETENTION_S,
            TimeUnit.SECONDS, NUM_RETAINED_MINS,
            SettingServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    @Test
    public void testInitialKeepDefaultsWhenSettingsUnavailable() {
        // The first "get" call always triggers a remote call. Return an error, so we know
        // we'll be returning whatever the "initial" values are.
        doReturn(Optional.of(Status.UNAVAILABLE.asException()))
            .when(settingBackend).getMultipleGlobalSettingsError(any());

        final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();

        assertThat(retentionPeriods.latestRetentionMinutes(), is(NUM_RETAINED_MINS));
        assertThat(retentionPeriods.hourlyRetentionHours(),
            is((int)GlobalSettingSpecs.StatsRetentionHours.createSettingSpec()
                .getNumericSettingValueType().getDefault()));
        assertThat(retentionPeriods.dailyRetentionDays(),
            is((int)GlobalSettingSpecs.StatsRetentionDays.createSettingSpec()
                .getNumericSettingValueType().getDefault()));
        assertThat(retentionPeriods.monthlyRetentionMonths(),
            is((int)GlobalSettingSpecs.StatsRetentionMonths.createSettingSpec()
                .getNumericSettingValueType().getDefault()));
    }

    @Test
    public void testUpdate() {
        final int hours = 12;
        final int days = 14;
        final int months = 4;
        doReturn(Arrays.asList(
            retentionSetting(GlobalSettingSpecs.StatsRetentionHours, hours),
            retentionSetting(GlobalSettingSpecs.StatsRetentionDays, days),
            retentionSetting(GlobalSettingSpecs.StatsRetentionMonths, months)))
                .when(settingBackend).getMultipleGlobalSettings(any());

        final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();
        assertThat(retentionPeriods.latestRetentionMinutes(), is(NUM_RETAINED_MINS));
        assertThat(retentionPeriods.hourlyRetentionHours(), is(hours));
        assertThat(retentionPeriods.dailyRetentionDays(), is(days));
        assertThat(retentionPeriods.monthlyRetentionMonths(), is(months));

        final ArgumentCaptor<GetMultipleGlobalSettingsRequest> reqCaptor =
            ArgumentCaptor.forClass(GetMultipleGlobalSettingsRequest.class);
        verify(settingBackend).getMultipleGlobalSettings(reqCaptor.capture());
        GetMultipleGlobalSettingsRequest request = reqCaptor.getValue();
        assertThat(request.getSettingSpecNameList(), containsInAnyOrder(
            GlobalSettingSpecs.StatsRetentionHours.getSettingName(),
            GlobalSettingSpecs.StatsRetentionDays.getSettingName(),
            GlobalSettingSpecs.StatsRetentionMonths.getSettingName()));

    }

    @Test
    public void testUpdatePartialReturn() {
        final int hours = 12;
        final int days = 14;
        // Suppose that for whatever reason the server doesn't return a "months" default.
        doReturn(Arrays.asList(
            retentionSetting(GlobalSettingSpecs.StatsRetentionHours, hours),
            retentionSetting(GlobalSettingSpecs.StatsRetentionDays, days)))
                .when(settingBackend).getMultipleGlobalSettings(any());

        final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();
        assertThat(retentionPeriods.latestRetentionMinutes(), is(NUM_RETAINED_MINS));
        assertThat(retentionPeriods.hourlyRetentionHours(), is(hours));
        assertThat(retentionPeriods.dailyRetentionDays(), is(days));
        assertThat(retentionPeriods.monthlyRetentionMonths(), is(
            (int)GlobalSettingSpecs.StatsRetentionMonths.createSettingSpec()
                .getNumericSettingValueType().getDefault()));
    }

    @Test
    public void testNoUpdateWithinRetentionInterval() {
        final int hours = 12;
        final int days = 14;
        final int months = 4;
        doReturn(Arrays.asList(
            retentionSetting(GlobalSettingSpecs.StatsRetentionHours, hours),
            retentionSetting(GlobalSettingSpecs.StatsRetentionDays, days),
            retentionSetting(GlobalSettingSpecs.StatsRetentionMonths, months)))
                .when(settingBackend).getMultipleGlobalSettings(any());

        // Initial call
        retentionPeriodFetcher.getRetentionPeriods();

        verify(settingBackend).getMultipleGlobalSettings(any());
        // Add some time, but not enough to trigger an update.
        clock.addTime(UPDATE_RETENTION_S - 1, ChronoUnit.SECONDS);

        retentionPeriodFetcher.getRetentionPeriods();
        // Backend should only have gotten called once.
        verify(settingBackend, times(1)).getMultipleGlobalSettings(any());
    }

    @Test
    public void testUpdateAfterRetentionInterval() {
        final int hours = 12;
        final int days = 14;
        final int months = 4;
        doReturn(Arrays.asList(
            retentionSetting(GlobalSettingSpecs.StatsRetentionHours, hours),
            retentionSetting(GlobalSettingSpecs.StatsRetentionDays, days),
            retentionSetting(GlobalSettingSpecs.StatsRetentionMonths, months)))
                .when(settingBackend).getMultipleGlobalSettings(any());

        // Initial call
        retentionPeriodFetcher.getRetentionPeriods();

        verify(settingBackend).getMultipleGlobalSettings(any());
        // Add enough time to trigger an update.
        clock.addTime(UPDATE_RETENTION_S + 1, ChronoUnit.SECONDS);

        retentionPeriodFetcher.getRetentionPeriods();
        // Backend should have gotten called twice now.
        verify(settingBackend, times(2)).getMultipleGlobalSettings(any());
    }

    @Test
    public void testKeepLatestWhenSettingsUnavailable() {
        final int hours = 12;
        final int days = 14;
        final int months = 4;
        doReturn(Arrays.asList(
            retentionSetting(GlobalSettingSpecs.StatsRetentionHours, hours),
            retentionSetting(GlobalSettingSpecs.StatsRetentionDays, days),
            retentionSetting(GlobalSettingSpecs.StatsRetentionMonths, months)))
                .when(settingBackend).getMultipleGlobalSettings(any());

        // Initial call
        retentionPeriodFetcher.getRetentionPeriods();

        // Next RPC should return error.
        doReturn(Optional.of(Status.UNAVAILABLE.asException()))
            .when(settingBackend).getMultipleGlobalSettingsError(any());
        // Make sure enough time passed.
        clock.addTime(UPDATE_RETENTION_S + 1, ChronoUnit.SECONDS);

        final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();
        assertThat(retentionPeriods.latestRetentionMinutes(), is(NUM_RETAINED_MINS));
        assertThat(retentionPeriods.hourlyRetentionHours(), is(hours));
        assertThat(retentionPeriods.dailyRetentionDays(), is(days));
        assertThat(retentionPeriods.monthlyRetentionMonths(), is(months));

    }

    private Setting retentionSetting(@Nonnull final GlobalSettingSpecs spec, int value) {
        return Setting.newBuilder()
            .setSettingSpecName(spec.getSettingName())
            .setNumericSettingValue(NumericSettingValue.newBuilder()
                .setValue(value))
            .build();
    }
}
