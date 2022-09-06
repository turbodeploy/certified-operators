package com.vmturbo.mediation.azure.pricing.fetcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Tests for CachingPricingFetcher.
 */
public class CachingPricingFetcherTest {
    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Allocates and cleans up a directopry for temporary files for this test.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Test getCacheKey and the mocked fetcher.
     */
    @Test
    public void tesGetCacheKey() {
        MockPricingFileFetcher wrappedFetcher = new MockPricingFileFetcher();

        CachingPricingFetcher<MockAccount> cachingFetcher = CachingPricingFetcher.<MockAccount>newBuilder()
                .build(wrappedFetcher);

        final MockAccount a1p1 = new MockAccount("1", "0001");
        final MockAccount a1p2 = new MockAccount("1", "0002");
        final MockAccount a2p1 = new MockAccount("2", "0001");
        final MockAccount a2p2 = new MockAccount("2", "0002");

        // Verify that the cache returns the same key as the wrapped fetcher

        assertEquals(wrappedFetcher.getCacheKey(a1p1), cachingFetcher.getCacheKey(a1p1));
        assertEquals(wrappedFetcher.getCacheKey(a1p2), cachingFetcher.getCacheKey(a1p2));
        assertEquals(wrappedFetcher.getCacheKey(a2p1), cachingFetcher.getCacheKey(a2p1));
        assertEquals(wrappedFetcher.getCacheKey(a2p2), cachingFetcher.getCacheKey(a2p2));

        // Check that the test implementation's keys behave as expected
        assertEquals(cachingFetcher.getCacheKey(a1p1), cachingFetcher.getCacheKey(a1p2));
        assertEquals(cachingFetcher.getCacheKey(a2p1), cachingFetcher.getCacheKey(a2p2));
        assertNotEquals(cachingFetcher.getCacheKey(a1p1), cachingFetcher.getCacheKey(a2p2));
    }

    /**
     * Tests the caching logic.
     *
     * @throws Exception should not happen and indicates a test failure
     */
    @Ignore("Test is inherently timing-sensitive")
    @Test
    public void testCaching() throws Exception {
        final int refreshSeconds = 10;
        final int expireSeconds = 20;

        final MockAccount a1p1 = new MockAccount("1", "0001");
        final Path tmpPath = tmpFolder.newFolder("pricesheet").toPath();
        final MockPricingFileFetcher wrappedFetcher = new MockPricingFileFetcher();
        final IPropertyProvider propertyProvider = IProbePropertySpec::getDefaultValue;

        final CachingPricingFetcher<MockAccount> cachingFetcher = CachingPricingFetcher.<MockAccount>newBuilder()
                .refreshAfter(Duration.ofSeconds(refreshSeconds))
                .expireAfter(Duration.ofSeconds(expireSeconds))
                .build(wrappedFetcher);

        final Object key = cachingFetcher.getCacheKey(a1p1);

        // If key is not in cache and wrapped fetcher is unable to fetch, the lookup fails.

        LOGGER.info("Initial failed fetch");

        wrappedFetcher.setResult(() -> {
            throw new RuntimeException("ERR1");
        });

        assertThrows(RuntimeException.class, () -> {
            cachingFetcher.fetchPricing(a1p1, propertyProvider);
        });

        // If the fetch succeeds, the lookup succeeds

        LOGGER.info("Initial successful fetch");

        final Path file1 = tmpPath.resolve("file1");

        wrappedFetcher.setResult(() -> new Pair<>(file1, "Fetch OK"));
        Pair<Path, String> fetched1 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched1.getFirst(), fetched1.getSecond());
        assertEquals(fetched1.getFirst(), file1);

        // For another fetch within the refresh time, the wrapped fetcher isn't called again, so we
        // get the same cached path. Also verifies that a different account that has the same key.

        LOGGER.info("Fast re-fetch, should get cached");

        AtomicBoolean didTryFetch = new AtomicBoolean(false);
        final Path file2 = tmpPath.resolve("file2");

        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            return new Pair<>(file2, "Fetch OK");
        });
        Pair<Path, String> fetched2 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched2.getFirst(), fetched2.getSecond());

        assertEquals(fetched1.getFirst(), fetched2.getFirst());
        assertTrue(fetched2.getSecond().contains("cached"));
        assertFalse(didTryFetch.get());

        // Wait until the refresh interval has happened, and fetch again. This time the fetcher
        // should have been called.

        LOGGER.info("Re-fetch after refresh window");

        Thread.sleep((refreshSeconds + 1) * 1000);

        Pair<Path, String> fetched3 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched3.getFirst(), fetched3.getSecond());
        assertEquals(file2, fetched3.getFirst());
        assertEquals("Fetch OK", fetched3.getSecond());
        assertTrue(didTryFetch.get());

        // Wait until the expiry interval has happened, and fetch again. The fetcher
        // should have been called.

        LOGGER.info("Re-fetch after expiry");

        final Path file3 = tmpPath.resolve("file3");

        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            return new Pair<>(file3, "Fetch OK");
        });

        Thread.sleep((expireSeconds + 1) * 1000);

        Pair<Path, String> fetched4 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched4.getFirst(), fetched4.getSecond());
        assertEquals(file3, fetched4.getFirst());
        assertTrue(didTryFetch.get());

        // If refresh fails, we get the old value, not a failure

        LOGGER.info("Failed fetch during refresh");

        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            throw new RuntimeException("ERR2");
        });

        Thread.sleep((refreshSeconds + 1) * 1000);

        Pair<Path, String> fetched5 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched5.getFirst(), fetched5.getSecond());
        assertEquals(file3, fetched4.getFirst());
        assertTrue(didTryFetch.get());

        // But after expiry, failed fetch is an error

        LOGGER.info("Failed fetch after expiry");

        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            throw new RuntimeException("ERR3");
        });

        Thread.sleep((expireSeconds - refreshSeconds + 1) * 1000);

        assertThrows(RuntimeException.class, () -> {
            cachingFetcher.fetchPricing(a1p1, propertyProvider);
        });

        assertTrue(didTryFetch.get());

        // Successful fetch to load the cache

        LOGGER.info("Successful fetch to load the cache");

        final Path file4 = tmpPath.resolve("file4");

        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            return new Pair<>(file4, "Fetch OK");
        });
        Pair<Path, String> fetched6 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched6.getFirst(), fetched6.getSecond());
        assertEquals(fetched6.getFirst(), file4);
        assertTrue(didTryFetch.get());

        // Remove the cached file and test that the fetch fails rather than returning a path
        // that doesn't exist.

        LOGGER.info("Cached file removed externally, verify failure");

        Files.delete(fetched6.getFirst());
        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            throw new RuntimeException("ERR4");
        });
        assertThrows(FileNotFoundException.class, () -> {
            cachingFetcher.fetchPricing(a1p1, propertyProvider);
        });
        assertFalse(didTryFetch.get());

        // Successful fetch again, since the cache was cleared by the failure

        LOGGER.info("Successful fetch since the cache was cleared by the failure");

        final Path file5 = tmpPath.resolve("file5");

        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            return new Pair<>(file5, "Fetch OK");
        });
        Pair<Path, String> fetched7 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched7.getFirst(), fetched7.getSecond());
        assertEquals(fetched7.getFirst(), file5);
        assertTrue(didTryFetch.get());

        // Remove the cached file and test that the fetch fails rather than returning a path
        // that doesn't exist.

        LOGGER.info("Cached file removed externally, verify failure after failed refresh");

        Thread.sleep((refreshSeconds + 1) * 1000);

        Files.delete(fetched7.getFirst());
        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            throw new RuntimeException("ERR5");
        });
        assertThrows(FileNotFoundException.class, () -> {
            cachingFetcher.fetchPricing(a1p1, propertyProvider);
        });
        assertTrue(didTryFetch.get());

        // Successful fetch again, since the cache was cleared by the failure

        LOGGER.info("Successful fetch since the cache was cleared by the failure");

        final Path file6 = tmpPath.resolve("file6");

        didTryFetch.set(false);
        wrappedFetcher.setResult(() -> {
            didTryFetch.set(true);
            return new Pair<>(file6, "Fetch OK");
        });
        Pair<Path, String> fetched8 = cachingFetcher.fetchPricing(a1p1, propertyProvider);
        LOGGER.info("Result: {} \"{}\"", fetched8.getFirst(), fetched8.getSecond());
        assertEquals(fetched8.getFirst(), file6);
        assertTrue(didTryFetch.get());
    }
}
