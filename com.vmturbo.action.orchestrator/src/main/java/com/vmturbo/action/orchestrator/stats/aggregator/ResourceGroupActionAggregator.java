package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Aggregates action stats for resource group in the cloud.
 */
public class ResourceGroupActionAggregator extends CloudActionAggregator {
    private final GroupMemberRetriever groupMemberRetriever;
    private Map<Long, Long> entityToResourceGroup;

    protected ResourceGroupActionAggregator(@Nonnull LocalDateTime snapshotTime,
                                            @Nonnull GroupMemberRetriever groupMemberRetriever) {
        super(snapshotTime);
        this.groupMemberRetriever = groupMemberRetriever;
    }

    @Override
    public void start() {
        try (DataMetricTimer ignored = Metrics.INIT_TIME_SECONDS.startTimer()) {
            entityToResourceGroup = retrieveResourceGroupMembershipMap();
            if (entityToResourceGroup.size() > 0) {
                logger.info("Retrieved resource group membership of size {}",
                    entityToResourceGroup.size());
            }
        }
    }

    private Map<Long, Long> retrieveResourceGroupMembershipMap() {
        GroupDTO.GetGroupsRequest groupFilter = GroupDTO.GetGroupsRequest
            .newBuilder()
            .setGroupFilter(
                GroupDTO.GroupFilter
                    .newBuilder()
                    .setGroupType(CommonDTO.GroupDTO.GroupType.RESOURCE)
                    .build())
            .build();
        try {
            Map<Long, Long> entityToRgMapping = new HashMap<>();
            List<GroupAndMembers> groupAndMembers =
                groupMemberRetriever.getGroupsWithMembers(groupFilter);
            for (GroupAndMembers grp : groupAndMembers) {
                for (Long member : grp.members()) {
                    entityToRgMapping.put(member, grp.group().getId());
                }
            }
            return entityToRgMapping;
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve resource group members due to group component error: {}",
                e.getMessage());
            return Collections.emptyMap();
        }
    }

    @Override
    protected Optional<Long> getAggregationEntity(long entityOid) {
        return Optional.ofNullable(entityToResourceGroup.get(entityOid));
    }

    @Override
    protected void incrementEntityWithMissedAggregationEntity() {}

    @Nonnull
    @Override
    protected ManagementUnitType getManagementUnitType() {
        return ManagementUnitType.RESOURCE_GROUP;
    }

    /**
     * Metrics for {@link ResourceGroupActionAggregator}.
     */
    private static class Metrics {

        private static final DataMetricSummary INIT_TIME_SECONDS = DataMetricSummary.builder()
            .withName("ao_action_rg_agg_init_seconds")
            .withHelp("Information about how long it took to initialize the resource group aggregator.")
            .build()
            .register();
    }

    /**
     * Factory class for {@link ResourceGroupActionAggregator}s.
     */
    public static class ResourceGroupActionAggregatorFactory implements ActionAggregatorFactory<ResourceGroupActionAggregator> {
        private final GroupMemberRetriever memberRetriever;

        /**
         * Constructor for the aggregator factory.
         *
         * @param memberRetriever Stub to access the group.
         */
        public ResourceGroupActionAggregatorFactory(@Nonnull final GroupMemberRetriever memberRetriever) {
            this.memberRetriever = Objects.requireNonNull(memberRetriever);
        }

        @Override
        public ResourceGroupActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new ResourceGroupActionAggregator(snapshotTime, memberRetriever);
        }
    }
}
