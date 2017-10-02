package com.vmturbo.systest.policy;

import java.util.Map;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A utility class that provides methods for static import for matching on analysis results.
 */
public class AnalysisResultsMatchers {

    /**
     * Prevent construction
     */
    private AnalysisResultsMatchers() {

    }

    /**
     * Builder for host from which a VM should move.
     */
    public static class FromHost {
        private final long fromId;

        FromHost(final long fromId) {
            this.fromId = fromId;
        }

        public long getFromId() {
            return fromId;
        }
    }

    /**
     * Builder for host to which a VM should move.
     */
    public static class ToHost {
        private final long toId;

        ToHost(final long toId) {
            this.toId = toId;
        }

        public long getToId() {
            return toId;
        }
    }

    /**
     * Builder for number of entities.
     */
    public static class ConsumerCount {
        private final long numberOfEntities;

        ConsumerCount(final long numberOfEntities) {
            this.numberOfEntities = numberOfEntities;
        }

        public long getNumberOfEntities() {
            return numberOfEntities;
        }
    }

    public static FromHost fromHost(final long fromId) {
        return new FromHost(fromId);
    }

    public static ToHost toHost(final long toId) {
        return new ToHost(toId);
    }

    public static ConsumerCount withConsumerCount(final long numberOfEntities) {
        return new ConsumerCount(numberOfEntities);
    }

    /**
     * A matcher helper to allow asserting that a certain ActionPlan contains an action
     * that moves a particular VM from a certain host to a certain host.
     *
     * @param vmId The ID of the VM we expect to be moved.
     * @param fromHost The host from which the VM should be moved.
     * @param toHost The host to which the VM should be moved.
     * @return A {@link Matcher} for use in a Hamcrest assertThat assertion.
     */
    public static Matcher<ActionPlan> movesVm(final long vmId, @Nonnull final FromHost fromHost,
                                              @Nonnull final ToHost toHost) {
        return new BaseMatcher<ActionPlan>() {
            @Override
            public boolean matches(Object o) {
                final ActionPlan actionPlan = (ActionPlan)o;
                return actionPlan.getActionList().stream()
                    .map(Action::getInfo)
                    .filter(info -> info.getActionTypeCase().equals(ActionTypeCase.MOVE))
                    .map(ActionInfo::getMove)
                    .filter(move -> move.getTargetId() == vmId)
                    .filter(move -> move.getSourceId() == fromHost.getFromId())
                    .filter(move -> move.getDestinationId() == toHost.getToId())
                    .findAny()
                    .isPresent();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("ActionPlan should move " + vmId + " from host " +
                    fromHost.getFromId() + " to host " + toHost.getToId());
            }
        };
    }

    /**
     * A matcher helper to allow asserting that a certain ActionPlan contains an action
     * that moves a particular VM. Does not require that the VM move from or to any
     * particular host.
     *
     * @param vmId The ID of the VM we expect to be moved.
     * @return A {@link Matcher} for use in a Hamcrest assertThat assertion.
     */
    public static Matcher<ActionPlan> movesVm(final long vmId) {
        return new BaseMatcher<ActionPlan>() {
            @Override
            public boolean matches(Object o) {
                final ActionPlan actionPlan = (ActionPlan)o;
                return actionPlan.getActionList().stream()
                    .map(Action::getInfo)
                    .filter(info -> info.getActionTypeCase().equals(ActionTypeCase.MOVE))
                    .map(ActionInfo::getMove)
                    .filter(move -> move.getTargetId() == vmId)
                    .findAny()
                    .isPresent();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("ActionPlan should move " + vmId + ".");
            }
        };
    }

    /**
     * A matcher that allows asserting that a particular entity in the topology is acting as a provider
     * for exactly a certain number of entities.
     */
    public static Matcher<Map<Long, TopologyEntityDTO>> hasProvider(final long providerOid,
                                                                    @Nonnull final ConsumerCount consumerCount) {
        return new BaseMatcher<Map<Long, TopologyEntityDTO>>() {
            private long actualConsumerCount;

            @Override
            @SuppressWarnings("unchecked")
            public boolean matches(Object o) {
                final Map<Long, TopologyEntityDTO> topology = (Map<Long, TopologyEntityDTO>)o;

                actualConsumerCount = topology.values().stream()
                    .filter(entity -> isConsumerOf(entity, providerOid))
                    .count();

                return actualConsumerCount == consumerCount.getNumberOfEntities();
            }

            private boolean isConsumerOf(@Nonnull final TopologyEntityDTO entity,
                                         final long providerOid) {
                return entity.getCommodityBoughtMapMap().keySet().contains(providerOid);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Topology should have " +
                    consumerCount.getNumberOfEntities() + " consumers for provider " +
                    providerOid + " but only has " + actualConsumerCount + " consumers.");
            }
        };
    }
}
