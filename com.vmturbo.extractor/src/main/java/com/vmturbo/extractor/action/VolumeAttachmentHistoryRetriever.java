package com.vmturbo.extractor.action;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import java.time.Clock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

import io.grpc.StatusRuntimeException;

/**
 * A class responsible for getting unattached days for deleted volumes.
 */
public class VolumeAttachmentHistoryRetriever {
    private final StatsHistoryServiceBlockingStub historyRpcService;
    private final Clock clock;
    private static final Logger logger = LogManager.getLogger();

    VolumeAttachmentHistoryRetriever(@Nonnull final StatsHistoryServiceBlockingStub historyRpcService, @Nonnull final Clock clock) {
        this.historyRpcService = historyRpcService;
        this.clock = clock;
    }

    /**
     * Get number of days unattached by volume oids.
     *
     * @param actionSpecs all action specs
     * @return a map of [volumeOid, unattachedDays]
     */
    public Map<Long, Integer> getVolumeAttachmentDays(@Nonnull final List<ActionSpec> actionSpecs) {
        // 1. get deleted volume ids from actionSpecs
        Set<Long> volumeIds = new HashSet<>();
        actionSpecs.forEach(actionSpec -> {
            ActionInfo actionInfo = actionSpec.getRecommendation().getInfo();
            if (actionInfo.hasDelete()) {
                ActionDTO.ActionEntity target = actionInfo.getDelete().getTarget();
                if (target.getType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    volumeIds.add(target.getId());
                }
            }
        });

        // 2. get history data
        Map<Long, Integer> unattachedDaysMap = new HashMap<>();
        try {
            if (volumeIds.isEmpty()) {
                return unattachedDaysMap;
            }
            final GetVolumeAttachmentHistoryRequest request = GetVolumeAttachmentHistoryRequest
                    .newBuilder()
                    .addAllVolumeOid(volumeIds)
                    .build();
            final Iterator<GetVolumeAttachmentHistoryResponse> responseIterator = historyRpcService.getVolumeAttachmentHistory(request);

            final long currentTime = clock.millis();
            responseIterator.forEachRemaining(response -> {
                response.getHistoryList()
                        .stream()
                        .forEach(history -> {
                            unattachedDaysMap.put(
                                    history.getVolumeOid(),
                                    (int)TimeUnit.MILLISECONDS.toDays(currentTime - history.getLastAttachedDateMs()));
                        });
            });
        } catch (StatusRuntimeException e) {
            logger.error("Failed to fetch volume attachment history", e);
        }
        return unattachedDaysMap;
    }
}