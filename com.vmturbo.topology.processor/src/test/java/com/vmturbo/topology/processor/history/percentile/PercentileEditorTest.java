package com.vmturbo.topology.processor.history.percentile;

import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;
import com.vmturbo.topology.processor.history.percentile.PercentilePersistenceTask.ReaderObserver;

/**
 * Unit tests for PercentileEditor.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({StatsHistoryServiceStub.class})
public class PercentileEditorTest extends BaseGraphRelatedTest {
    private static final long oid1 = 1;
    private static final int ct1 = 7;
    private static final PercentileHistoricalEditorConfig config =
                    new PercentileHistoricalEditorConfig(1,
                                                         24,
                                                         10,
                                                         100,
                                                         Collections.emptyMap());

    /**
     * Test the initial data loading.
     * That requests to load full and latest window blobs are made and results are accumulated.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testLoadData() throws HistoryCalculationException, InterruptedException {
        final int countAtFullWindow = 10;
        final int countAtLatestWindow = 20;
        Answer<Void> answerGetCounts = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                GetPercentileCountsRequest request = invocation
                                .getArgumentAt(0, GetPercentileCountsRequest.class);
                Assert.assertNotNull(request);
                byte[] payload = request.getStartTimestamp() == 0 ? createOneRecordPayload(countAtFullWindow)
                                : createOneRecordPayload(countAtLatestWindow);
                ReaderObserver observer = invocation.getArgumentAt(1, ReaderObserver.class);
                observer.onNext(PercentileChunk.newBuilder().setPeriod(0).setStartTimestamp(0)
                                .setContent(ByteString.copyFrom(payload))
                                .build());
                observer.onCompleted();
                return null;
            }
        };
        StatsHistoryServiceStub history = PowerMockito.mock(StatsHistoryServiceStub.class);
        Mockito.doAnswer(answerGetCounts).when(history).getPercentileCounts(Mockito.any(),
                                                                            Mockito.any());

        TopologyEntity entity = mockEntity(1, 1L, CommodityType.newBuilder().setType(ct1).build(),
                                           1, 0D, null, null, null, null);
        TopologyGraph<TopologyEntity> graph = mockGraph(ImmutableSet.of(entity));
        ICommodityFieldAccessor accessor = new CommodityFieldAccessor(graph);
        PercentileEditorCacheAccess editor = new PercentileEditorCacheAccess(config, history);

        editor.initContext(new GraphWithSettings(graph, Collections.emptyMap(),
                                                 Collections.emptyMap()),
                           accessor);

        PercentileCommodityData data = editor
            .getEntry(new EntityCommodityFieldReference(oid1,
                                                        CommodityType.newBuilder().setType(ct1).build(),
                                                        CommodityField.USED));
        Assert.assertNotNull(data);
        UtilizationCountStore store = data.getUtilizationCountStore();
        Assert.assertNotNull(store);
        PercentileRecord.Builder full = store.checkpoint(Collections.emptySet());
        Assert.assertNotNull(full);
        PercentileRecord record = full.build();
        Assert.assertEquals(101, record.getUtilizationCount());
        Assert.assertEquals(0, record.getUtilization(0));
        Assert.assertEquals(1, record.getUtilization(countAtFullWindow));
        Assert.assertEquals(1, record.getUtilization(countAtLatestWindow));
    }

    private static byte[] createOneRecordPayload(int count) {
        PercentileRecord.Builder rec = PercentileRecord.newBuilder().setEntityOid(oid1)
                        .setCommodityType(ct1).setCapacity(100f).setPeriod(30);
        for (int i = 0; i <= 100; ++i) {
            rec.addUtilization(i == count ? 1 : 0);
        }
        byte[] payload = PercentileCounts.newBuilder()
                        .addPercentileRecords(rec.build())
                        .build().toByteArray();
        return payload;
    }

    /**
     * Access to the percentile editor cached data.
     */
    private static class PercentileEditorCacheAccess extends PercentileEditor {
        /**
         * Construct the instance.
         *
         * @param config configuration values
         * @param statsHistoryClient history component access stub
         */
        PercentileEditorCacheAccess(PercentileHistoricalEditorConfig config,
                        StatsHistoryServiceStub statsHistoryClient) {
            super(config, statsHistoryClient);
        }

        /**
         * Fetch the cached data for the commodity field.
         *
         * @param field commodity field reference
         * @return percentile data, null if not stored
         */
        public PercentileCommodityData getEntry(EntityCommodityFieldReference field) {
            return getCache().get(field);
        }
    }
}
