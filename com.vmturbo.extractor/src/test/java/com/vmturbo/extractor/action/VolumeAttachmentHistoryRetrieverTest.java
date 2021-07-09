package com.vmturbo.extractor.action;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryResponse.VolumeAttachmentHistory;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Unit tests for {@link VolumeAttachmentHistoryRetriever}.
 */
public class VolumeAttachmentHistoryRetrieverTest {
    StatsMoles.StatsHistoryServiceMole statsHistoryServiceMole = spy(new StatsMoles.StatsHistoryServiceMole());

    /**
     * Test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcTestHistoryServer = GrpcTestServer.newServer(statsHistoryServiceMole);

    /**
     * Test getting delete volume unattached days.
     */
    @Test
    public void testVolumeAttachmentHistoryRetriever() {
        final long volumeID1 = 11111L;
        final MutableFixedClock clock = new MutableFixedClock(100);

        // 1. rpc response
        final VolumeAttachmentHistory history1 = VolumeAttachmentHistory.newBuilder()
                .setVolumeOid(volumeID1)
                .setLastAttachedDateMs(clock.millis() - TimeUnit.DAYS.toMillis(3))
                .build();
        Stats.GetVolumeAttachmentHistoryResponse response1 = Stats.GetVolumeAttachmentHistoryResponse
                .newBuilder()
                .addHistory(history1)
                .build();
        List<Stats.GetVolumeAttachmentHistoryResponse> responses = Collections.singletonList(response1);
        // 2. when call rpc
        final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub historyRpc = StatsHistoryServiceGrpc.newBlockingStub(grpcTestHistoryServer.getChannel());
        when(statsHistoryServiceMole.getVolumeAttachmentHistory(any())).thenReturn(responses);

        // 1. input
        final ActionDTO.ActionSpec actionSpecsDelete = ActionDTO.ActionSpec.newBuilder()
                .setRecommendation(ActionDTO.Action.newBuilder()
                        .setExplanation(Explanation.getDefaultInstance())
                        .setDeprecatedImportance(0)
                        .setId(volumeID1)
                        .setInfo(ActionInfo.newBuilder()
                                .setDelete(Delete.newBuilder()
                                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                                .setId(volumeID1)
                                                .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE)))))
                .build();
        // 2. test function
        VolumeAttachmentHistoryRetriever volumeAttachmentHistoryRetriever = new VolumeAttachmentHistoryRetriever(historyRpc, clock);
        Map<Long, Integer> unattachedDaysMap = volumeAttachmentHistoryRetriever.getVolumeAttachmentDays(Collections.singletonList(actionSpecsDelete));
        assertThat(unattachedDaysMap.get(volumeID1), is(3));
    }
}
