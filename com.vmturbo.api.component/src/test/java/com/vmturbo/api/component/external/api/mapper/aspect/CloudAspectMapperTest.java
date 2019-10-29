package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link CloudAspectMapper}.
 */
public class CloudAspectMapperTest {

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(new SearchServiceMole());

    /**
     * Test for {@link CloudAspectMapper#mapEntityToAspect(TopologyEntityDTO)} method.
     *
     * @throws OperationFailedException in case of API mapper failure.
     */
    @Test
    public void testMapEntityToAspect() throws OperationFailedException {
        // Arrange

        // Simulate 25% RI Coverage for the past state
        final StatSnapshotApiDTO historicalSnapshot = createSnapshot(-1, 25F);
        // Simulate 50% RI Coverage for the current state
        final float currentRiCoverage = 50F;
        final StatSnapshotApiDTO currentSnapshot = createSnapshot(0, currentRiCoverage);
        // Simulate 75% RI Coverage for the projected state
        final StatSnapshotApiDTO projectedSnapshot = createSnapshot(1, 75F);

        final List<StatSnapshotApiDTO> snapshots = ImmutableList.of(
                historicalSnapshot,
                currentSnapshot,
                projectedSnapshot);
        final StatsQueryExecutor statsQueryExecutor = mock(StatsQueryExecutor.class);
        when(statsQueryExecutor.getAggregateStats(any(), any())).thenReturn(snapshots);

        final long oid = 111L;
        final UuidMapper uuidMapper = mock(UuidMapper.class);
        final ApiId apiId = mock(ApiId.class);
        when(uuidMapper.fromOid(oid)).thenReturn(apiId);

        final SearchServiceBlockingStub searchService = SearchServiceGrpc.newBlockingStub(
                grpcServer.getChannel());

        final CloudAspectMapper cloudAspectMapper = new CloudAspectMapper(statsQueryExecutor,
            uuidMapper, searchService);

        final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        // Act
        final EntityAspect aspect = cloudAspectMapper.mapEntityToAspect(entity);

        // Assert
        assertTrue(aspect instanceof CloudAspectApiDTO);
        final CloudAspectApiDTO cloudAspect = (CloudAspectApiDTO)aspect;
        final Float riCoveragePercentage = cloudAspect.getRiCoveragePercentage();
        assertNotNull(riCoveragePercentage);
        // Assert that RI Coverage is 50% which is true for the current state
        assertEquals(currentRiCoverage, riCoveragePercentage, 0.001F);
        final StatApiDTO riCoverage = cloudAspect.getRiCoverage();
        final Optional<StatApiDTO> expectedRiCoverage = currentSnapshot.getStatistics().stream()
                .findFirst();
        assertTrue(expectedRiCoverage.isPresent());
        assertSame(expectedRiCoverage.get(), riCoverage);
    }

    private static StatSnapshotApiDTO createSnapshot(
            final int daysOffset,
            final float riCoverage) {
        final StatSnapshotApiDTO snapshotApiDTO = new StatSnapshotApiDTO();
        snapshotApiDTO.setDate(toDate(LocalDateTime.now().plusDays(daysOffset)));
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setValue(riCoverage);
        final StatValueApiDTO capacityDto = new StatValueApiDTO();
        capacityDto.setAvg(100F);
        statApiDTO.setCapacity(capacityDto);
        snapshotApiDTO.setStatistics(Collections.singletonList(statApiDTO));
        return snapshotApiDTO;
    }

    private static String toDate(LocalDateTime localDateTime) {
        return DateTimeUtil.toString(
                Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant()));
    }
}
