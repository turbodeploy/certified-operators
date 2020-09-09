package com.vmturbo.cost.component.cleanup;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;

/**
 * An implementation of {@link RetentionDurationFetcher}, in which the retention duration is retrieved
 * from the settings service and is cached for a configurable duration.
 */
public class SettingsRetentionFetcher implements RetentionDurationFetcher {

    private final Logger logger = LogManager.getLogger();

    private final SettingServiceBlockingStub settingServiceClient;

    private final SettingSpec retentionSpec;

    private final Duration cachingDuration;

    private BoundedDuration cachedRetention;

    private Instant lastUpdate;

    /**
     * Constructs a new retention fetcher.
     * @param settingServiceClient The settings service client.
     * @param retentionSpec The setting spec to fetch from the settings service.
     * @param retentionUnit The time unit of the numerical value of the target setting spec.
     * @param cachingDuration Indicates how long the retrieved setting value should be cached. After
     *                        expiration, the setting will be refetched form the settings service.
     */
    public SettingsRetentionFetcher(@Nonnull SettingServiceBlockingStub settingServiceClient,
                                  @Nonnull SettingSpec retentionSpec,
                                  @Nonnull TemporalUnit retentionUnit,
                                  @Nonnull Duration cachingDuration) {
        this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
        this.retentionSpec = Objects.requireNonNull(retentionSpec);
        this.cachingDuration = Objects.requireNonNull(cachingDuration);

        this.cachedRetention = ImmutableBoundedDuration.builder()
                .amount((long)retentionSpec.getNumericSettingValueType().getDefault())
                .unit(retentionUnit)
                .build();
        this.lastUpdate = Instant.now();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public synchronized BoundedDuration getRetentionDuration() {
        final Duration durationSinceLastUpdate = Duration.between(lastUpdate, Instant.now());

        if (durationSinceLastUpdate.compareTo(cachingDuration) >= 0) {

            logger.info("Cache has expired. Fetching {} from the settings repository",
                    retentionSpec.getDisplayName());

            final int retentionDuration = fetchRetentionSetting();
            cachedRetention = ImmutableBoundedDuration.builder()
                    .amount(retentionDuration)
                    .unit(cachedRetention.unit())
                    .build();
            lastUpdate = Instant.now();

            logger.info("Refreshed {} retention setting [{}]",
                    retentionSpec.getDisplayName(),
                    cachedRetention);
        }

        return cachedRetention;
    }

    private int fetchRetentionSetting() {
        final GetSingleGlobalSettingRequest request = GetSingleGlobalSettingRequest.newBuilder()
                .setSettingSpecName(retentionSpec.getName())
                .build();

        final GetGlobalSettingResponse response = settingServiceClient.getGlobalSetting(request);

        final boolean validRetentionSetting = response.hasSetting()
                && response.getSetting().hasNumericSettingValue()
                && response.getSetting().getNumericSettingValue().hasValue();

        return validRetentionSetting
                ? (int)response.getSetting().getNumericSettingValue().getValue()
                : (int)retentionSpec.getNumericSettingValueType().getDefault();
    }
}