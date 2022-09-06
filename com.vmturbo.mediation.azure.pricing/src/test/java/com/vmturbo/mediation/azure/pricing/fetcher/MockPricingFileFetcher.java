package com.vmturbo.mediation.azure.pricing.fetcher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * A mock PricingFileFetcher for testing CachingPricingFetcher and FetcherStage.
 */
public class MockPricingFileFetcher implements PricingFileFetcher<MockAccount> {
    private Callable<Pair<Path, String>> resultCallable;

    /**
     * Construct the mock fetcher.
     */
    public MockPricingFileFetcher() {}

    @NotNull
    @Override
    public Object getCacheKey(@NotNull MockAccount account) {
        return new MockKey(account);
    }

    @Nullable
    @Override
    public Pair<Path, String> fetchPricing(@Nonnull MockAccount account,
            @Nonnull IPropertyProvider propertyProvider) throws Exception {
        Pair<Path, String> result = resultCallable.call();

        // Create a file for the cache to clean up.

        Files.createFile(result.getFirst());

        return result;
    }

    /**
     * Tell the mocked fetcher what to do on the next fetch attempt. The callable can
     * either return a Pair of downloaded file path and status, or can throw an exception.
     *
     * @param resultCallable called to get the return value, or error, for the next fetch
     *   attempt.
     */
    public void setResult(@Nonnull Callable<Pair<Path, String>> resultCallable) {
        this.resultCallable = resultCallable;
    }

    /**
     * A key corresponding to MockAccount. Only the account matters for comparison,
     * the plan is irrelevant.
     */
    private static class MockKey {
        private MockAccount account;

        MockKey(@Nonnull MockAccount account) {
            this.account = account;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MockKey mockKey = (MockKey)o;
            return account.accountId.equals(mockKey.account.accountId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(account.accountId);
        }

        @Override
        public String toString() {
            return String.format("Account %s Plan %s", account.accountId, account.planId);
        }
    }
}
