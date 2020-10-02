package com.vmturbo.market.topology.conversions;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;

/**
 * A factory for creating {@link ReversibilitySettingFetcher} instances.
 */
public interface ReversibilitySettingFetcherFactory {

    /**
     * Create a new {@link ReversibilitySettingFetcher}.
     *
     * @return The {@link ReversibilitySettingFetcher} object.
     */
    @Nonnull
    ReversibilitySettingFetcher newReversibilitySettingRetriever();

    /**
     * The default implementation of {@link ReversibilitySettingFetcherFactory}.
     */
    class DefaultReversibilitySettingFetcherFactory implements ReversibilitySettingFetcherFactory {

        private final SettingPolicyServiceBlockingStub settingPolicyService;

        /**
         * Constructs new {@code DefaultReversibilitySettingFetcherFactory} instance.
         *
         * @param settingPolicyService Setting policy service client (used to fetch policy settings
         *      * from Group component).
         */
        public DefaultReversibilitySettingFetcherFactory(
                @Nonnull final SettingPolicyServiceBlockingStub settingPolicyService) {
            this.settingPolicyService = settingPolicyService;
        }

        @Nonnull
        @Override
        public ReversibilitySettingFetcher newReversibilitySettingRetriever() {
            return new ReversibilitySettingFetcher(settingPolicyService);
        }
    }
}
