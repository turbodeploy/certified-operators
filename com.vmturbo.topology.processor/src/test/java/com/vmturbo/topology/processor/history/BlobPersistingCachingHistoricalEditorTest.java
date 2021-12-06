package com.vmturbo.topology.processor.history;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.ZipStreamBuilder;
import com.vmturbo.components.common.utils.ThrowingFunction;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.history.exceptions.HistoryCalculationException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;

/**
 * Tests for {@link BlobPersistingCachingHistoricalEditorTest}.
 */
public class BlobPersistingCachingHistoricalEditorTest {

    private static final Logger logger = LogManager.getLogger();
    private static final long TOPOLOGY_ID = 777777L;
    private static final Clock clock = mock(Clock.class);
    private static final KVConfig kvConfig = mock(KVConfig.class);
    private static final CachingHistoricalEditorConfig CONFIG = new CachingHistoricalEditorConfig(
        2, 3, TOPOLOGY_ID, clock, kvConfig) {
        @Override
        protected String getDiagnosticsEnabledPropertyName() {
            return null;
        }
    };

    private static final SystemNotificationProducer systemNotificationProducer = Mockito.mock(
        SystemNotificationProducer.class);

    private static final String FOO_BINARY = "foo.binary";
    private static final String BAR_BINARY = "bar.binary";

    private static final long OID1 = 12345L;
    private static final long OID2 = 23456L;

    private static final CommodityType VCPU_COMMODITY_TYPE = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.VCPU_VALUE)
        .build();

    private static final EntityCommodityFieldReference VCPU_REF1 =
        new EntityCommodityFieldReference(OID1, VCPU_COMMODITY_TYPE, null, CommodityField.USED);
    private static final EntityCommodityFieldReference VCPU_REF2 =
        new EntityCommodityFieldReference(OID2, VCPU_COMMODITY_TYPE, null, CommodityField.USED);

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    /**
     * Test correct close of all InputStreams if an exception occurs during diags restore.
     *
     * @throws IOException on exception
     * @throws DiagnosticsException on exception
     */
    @Test
    public void testMultipleDiagsExceptionHandling() throws IOException, DiagnosticsException {
        final ExceptionThrowingBlobPersister blobPersister =
            new ExceptionThrowingBlobPersister(clock, identityProvider);

        ZipStreamBuilder builder = ZipStreamBuilder.builder()
            .withTextFile(FOO_BINARY, "a", "b")
            .withTextFile(BAR_BINARY, "c", "d");

        // We expect exceptions
        try {
            blobPersister.restoreDiags(builder.getBytes(), null);
        } catch (DiagnosticsException e) {
            final StringWriter stackTrace = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTrace));

            // We expect to see errors for both foo and bar
            assertThat(stackTrace.toString(), containsString(FOO_BINARY));
            assertThat(stackTrace.toString(), containsString(BAR_BINARY));
            return;
        }

        // We should enver get here because we should exit after the exception is thrown above.
        fail();
    }

    /**
     * Test that we expire references from the cache to OID's no longer known to the identity provider.
     *
     * @throws IdentityUninitializedException if identity provider uninitialized.
     * @throws HistoryCalculationException if unable to expire stale OIDs.
     */
    @Test
    public void testOidExpiration() throws IdentityUninitializedException, HistoryCalculationException {
        final TestBlobPersister blobPersister =
            new TestBlobPersister(clock, identityProvider);
        final LongSet longSet = new LongArraySet(1);
        longSet.add(VCPU_REF1.getEntityOid());

        blobPersister.getCache().put(VCPU_REF1, new TestHistoryCommodityData());
        blobPersister.getCache().put(VCPU_REF2, new TestHistoryCommodityData());
        when(identityProvider.getCurrentOidsInIdentityCache()).thenReturn(longSet);

        assertEquals(2, blobPersister.getCache().size());
        blobPersister.expireStaleOidsFromCache();
        assertEquals(1, blobPersister.getCache().size());

        longSet.clear();
        blobPersister.expireStaleOidsFromCache();
        assertEquals(0, blobPersister.getCache().size());
    }

    /**
     * Create a configuration in the key-value store.
     *
     * @param key The key to create. The key's value is set to true.
     * @return The created KVConfig.
     */
    public static KVConfig createKvConfig(@Nonnull final String key) {
        final KVConfig result = mock(KVConfig.class);
        final KeyValueStore kvStore = mock(KeyValueStore.class);
        when(result.keyValueStore()).thenReturn(kvStore);
        when(kvStore.get(Mockito.any())).thenAnswer(invocation -> {
            final String parameter = invocation.getArgumentAt(0, String.class);
            if (parameter.equals(key)) {
                return Optional.of("true");
            }
            return Optional.empty();
        });
        return result;
    }

    /**
     * A blob persister for testing. Has some fixed exporting functions.
     */
    private static class TestBlobPersister extends BlobPersistingCachingHistoricalEditor<TestHistoryCommodityData, TestLoadingTask,
        CachingHistoricalEditorConfig, Float, StatsHistoryServiceBlockingStub, Float, TopologyEntityDTO.Builder> {

        private TestBlobPersister(@Nonnull Clock clock,
                                  @Nonnull IdentityProvider identityProvider) {
            super(CONFIG, null, (a, b) -> new TestLoadingTask(), TestHistoryCommodityData::new,
                clock, systemNotificationProducer, identityProvider);
        }

        @Nonnull
        @Override
        protected List<Pair<String, ThrowingFunction<TestHistoryCommodityData, Float, HistoryCalculationException>>>
        getStateExportingFunctions() {
            return Arrays.asList(
                Pair.create(FOO_BINARY, (data) -> 1.0f),
                Pair.create(BAR_BINARY, (data) -> 2.0f));
        }

        @Nonnull
        @Override
        protected TopologyEntityDTO.Builder createBlob(
            @Nonnull ThrowingFunction<TestHistoryCommodityData, Float, HistoryCalculationException> dumpingFunction)
            throws HistoryCalculationException, InterruptedException {
            return TopologyEntityDTO.newBuilder();
        }

        @Override
        protected int getRecordCount(@Nonnull TopologyEntityDTO.Builder blobBuilder) {
            return 0;
        }

        @Override
        protected void restorePersistedData(@Nonnull Map<String, InputStream> diagsMapping,
                                            @Nonnull HistoryAggregationContext context)
            throws HistoryCalculationException, InterruptedException, IOException {

        }

        @Override
        public boolean isApplicable(@Nullable List<ScenarioChange> changes, @Nonnull TopologyInfo topologyInfo,
                                    @Nullable PlanScope scope) {
            return false;
        }

        @Override
        public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                                             @Nonnull CommoditySoldDTO.Builder commSold,
                                             @Nullable TopologyInfo topoInfo) {
            return false;
        }

        @Override
        public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                                             @Nonnull CommodityBoughtDTO.Builder commBought,
                                             int providerType) {
            return false;
        }

        @Override
        public boolean isMandatory() {
            return false;
        }
    }

    /**
     * A blob persister that always throws exceptions when its internal streams are closed.
     */
    private static class ExceptionThrowingBlobPersister extends TestBlobPersister {

        private ExceptionThrowingBlobPersister(@Nonnull Clock clock,
                                               @Nonnull IdentityProvider identityProvider) {
            super(clock, identityProvider);
        }

        @Override
        @Nonnull
        protected Map<String, InputStream> getDiagsMapping(@Nonnull final byte[] compressedDiags) throws DiagnosticsException {
            final Map<String, InputStream> map = new HashMap<>();
            final InputStream fooStream = mock(InputStream.class);
            final InputStream barStream = mock(InputStream.class);

            try {
                doThrow(new IOException(FOO_BINARY)).when(fooStream).close();
                doThrow(new IOException(BAR_BINARY)).when(barStream).close();

                map.put(FOO_BINARY, fooStream);
                map.put(BAR_BINARY, barStream);
            } catch (IOException e) {
                logger.error("getDiagsMapping error", e);
            }
            return map;
        }
    }

    /**
     * Commodity data for testing.
     */
    private static class TestHistoryCommodityData
        implements IHistoryCommodityData<CachingHistoricalEditorConfig, Float, Float> {

        @Override
        public void aggregate(@Nonnull EntityCommodityFieldReference field,
                              @Nonnull CachingHistoricalEditorConfig cachingHistoricalEditorConfig,
                              @Nonnull HistoryAggregationContext context) {

        }

        @Override
        public void init(@Nonnull EntityCommodityFieldReference field,
                         @Nullable Float aFloat,
                         @Nonnull CachingHistoricalEditorConfig cachingHistoricalEditorConfig,
                         @Nonnull HistoryAggregationContext context) {

        }

        @Override
        public boolean needsReinitialization(@Nonnull EntityCommodityReference ref,
                                             @Nonnull HistoryAggregationContext context,
                                             @Nonnull CachingHistoricalEditorConfig cachingHistoricalEditorConfig) {
            return false;
        }
    }

    /**
     * Loading task for testing.
     */
    private static class TestLoadingTask implements IHistoryLoadingTask<CachingHistoricalEditorConfig, Float> {
        @Nonnull
        @Override
        public Map<EntityCommodityFieldReference, Float> load(@Nonnull Collection<EntityCommodityReference> commodities,
                                                              @Nonnull CachingHistoricalEditorConfig cachingHistoricalEditorConfig,
                                                              Set<Long> oidsToUse) throws HistoryCalculationException, InterruptedException {
            return Collections.emptyMap();
        }
    }
}