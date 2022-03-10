package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

@RunWith(Parameterized.class)
public class StorageLatencyPostStitchingOpTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {new StorageLatencyPostStitchingOperation()}
        });
    }

    private final StorageLatencyPostStitchingOperation operation;

    public StorageLatencyPostStitchingOpTest(@Nonnull final StorageLatencyPostStitchingOperation op) {
        this.operation = op;
    }

    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final float latencyCapacity = 150;

    private final CommoditySoldView emptyLatencyCommodity = makeCommoditySold(CommodityType.STORAGE_LATENCY);
    private final CommoditySoldView fullLatencyCommodity = makeCommoditySold(CommodityType.STORAGE_LATENCY, latencyCapacity);
    private final CommoditySoldView irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);

    private final List<CommoditySoldView> startingList = Arrays.asList(emptyLatencyCommodity, irrelevantCommodity);
    private final List<CommoditySoldView> endingList = Arrays.asList(fullLatencyCommodity, irrelevantCommodity);

    private final Setting latencySetting = makeNumericSetting(latencyCapacity);

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
                eq(EntitySettingSpecs.LatencyCapacity))).thenReturn(Optional.of(latencySetting));

    }

    @Test
    public void testNoEntities() {
        final TopologicalChangelog result =
            operation.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    @Test
    public void testNoCommodities() {
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(Collections.emptyList());

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoSettings() {
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
                eq(EntitySettingSpecs.LatencyCapacity))).thenReturn(Optional.empty());

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(startingList);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().size() == 1);
    }



    @Test
    public void testNoLatencyCommodity() {

        final List<CommoditySoldView> origCommodities = Collections.singletonList(irrelevantCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testLatencyCommodityWithCapacity() {

        final CommoditySoldView preloaded = makeCommoditySold(CommodityType.STORAGE_LATENCY, 99);
        final List<CommoditySoldView> origCommodities =
                Arrays.asList(preloaded, irrelevantCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testHappyPath() {

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(startingList);

        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldView> actualCommodities =
                testTE.getTopologyEntityImpl().getCommoditySoldListList();

        assertTrue(endingList.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(endingList));
    }

    @Test
    public void testMultipleCommodities() {

        final List<CommoditySoldView> origCommodities = Arrays.asList(irrelevantCommodity,
                makeCommoditySold(CommodityType.STORAGE_LATENCY, 23), emptyLatencyCommodity, fullLatencyCommodity);

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldView> actualCommodities =
                testTE.getTopologyEntityImpl().getCommoditySoldListList();
        final List<CommoditySoldView> expectedCommodities = Arrays.asList(irrelevantCommodity,
                makeCommoditySold(CommodityType.STORAGE_LATENCY, 23),
                fullLatencyCommodity, fullLatencyCommodity);

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }
}
