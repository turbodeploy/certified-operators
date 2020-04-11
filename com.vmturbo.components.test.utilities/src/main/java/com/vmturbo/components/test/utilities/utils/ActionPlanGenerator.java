package com.vmturbo.components.test.utilities.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utilities for constructing action plans for use in tests.
 *
 * Note that this utility does not guarantee the generation of actions that will "make sense"
 * in the real world. For example it may choose to try to move a datacenter onto an application,
 * activate the same virtual machine 12 times, move an entity onto itself, resize a commodity
 * that is not actually present on an entity, or something similarly nonsensical.
 *
 * No effort is made to produce meaningful explanations.
 *
 * Uses a random seed for randomness. The number generator is given the seed at the time the plan
 * is constructed, so using the same seed with the same topology and the same number of actions
 * should generate the same plan (with the exception of Action IDs which are generated with new Ids
 * using the {@link IdentityGenerator} each time).
 */
public class ActionPlanGenerator {
    private static final int DEFAULT_TOPOLOGY_SIZE = 128;

    private final int randomSeed;

    /**
     * Create a new {@link ActionPlanGenerator} for generating {@link ActionPlan}s.
     */
    public ActionPlanGenerator() {
        this.randomSeed = 1234567;
    }

    /**
     * Create a new {@link ActionPlanGenerator} for generating {@link ActionPlan}s.
     *
     * @param randomSeed The seed to use for randomness in plan generation.
     */
    public ActionPlanGenerator(int randomSeed) {
        this.randomSeed = randomSeed;
    }

    /**
     * Generate an action plan with the number of actions provided.
     * The plan will be generated against a small topology of a fixed size.
     *
     * Half of all actions generated (rounded up) will be moves. All other actions will be selected at random
     * from the other available action types.
     *
     * @param numActions The number of actions in the generated plan.
     * @param topologyId The ID of the topology.
     * @param topologyContextId The topology context ID to be associated with the action plan.
     * @return An action plan with the requested number of actions.
     */
    public ActionPlan generate(int numActions, long topologyId, long topologyContextId) {
        final Random random = new Random(randomSeed);

        final List<GeneratedEntity> generatedEntities = LongStream.range(0, DEFAULT_TOPOLOGY_SIZE)
            .mapToObj(oid -> GeneratedEntity.randomEntityWithOid(oid, random))
                .collect(Collectors.toList());

        return generate(numActions, topologyId, topologyContextId, generatedEntities, random);
    }

    /**
     * Generate an action plan with the number of actions provided.
     * The plan will be generated against the provided topology.
     *
     * Half of all actions generated (rounded up) will be moves. All other actions will be selected at random
     * from the other available action types.
     *
     * @param numActions The number of actions in the generated plan.
     * @param topologyId The ID of the topology.
     * @param topologyContextId The topology context ID to be associated with the action plan.
     * @param topologyEntities The  entities in the topology to generate the action plan against.
     * @param random The random number generator to use.
     * @return An action plan with the requested number of actions.
     */
    public ActionPlan generate(int numActions, long topologyId, long topologyContextId,
                               @Nonnull final List<GeneratedEntity> topologyEntities,
                               @Nonnull final Random random) {
        if (numActions < 0) {
            throw new IllegalArgumentException("Illegal number of actions " + numActions);
        }

        // Half of all actions are moves.
        final MoveCount moveCount = moveCount((int)Math.ceil(numActions * 0.5));
        // Arrays.asList generates an immutable list. We need a mutable list so wrap in new ArrayList.
        final List<ActionTypeCount> counts = new ArrayList<>(Arrays.asList(
            reconfigureCount(0), provisionCount(0), resizeCount(0), activateCount(0), deactivateCount(0)));

        // Randomly choose an action type for each other type of action.
        for (int remaining = numActions - moveCount.getActionCount(); remaining > 0; remaining--) {
            counts.get(random.nextInt(counts.size())).incrementCount();
        }

        counts.add(moveCount);
        return generate(topologyId, topologyContextId, topologyEntities, random,
            counts.toArray(new ActionTypeCount[counts.size()]));
    }

    /**
     * Generate an action plan with the specified counts for each of the provided action types.
     * The plan will be generated against the provided topology.
     *
     * Example usage:
     * {@code actionPlanGenerator.generate(topology, moveCount(4), resizeCount(12), activateCount(6)}
     * will generate a plan with 4 moves, 12 resizes, and 6 activates.
     *
     * Providing the same {@link ActionTypeCount} multiple times will add the sum of the counts to the plan.
     * ie {@code actionPlanGenerator.generate(topology, moveCount(4), moveCount(3))}
     * will generate a plan with 7 moves.
     *
     * @param topologyId The ID of the topology.
     * @param topologyContextId The topology context ID to be associated with the action plan.
     * @param topologyEntities The topology to use when generating the action plan.
     * @param random The random number generator to use.
     * @param actionCounts The counts of each type of action to include in the plan.
     * @return the generated action plan
     */
    public ActionPlan generate(final long topologyId, final long topologyContextId,
                               @Nonnull final List<GeneratedEntity> topologyEntities,
                               @Nonnull final Random random,
                               ActionTypeCount...actionCounts) {
        final ActionPlan.Builder planBuilder = ActionPlan.newBuilder()
            .setId(IdentityGenerator.next())
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setTopologyId(topologyId)
                        .setCreationTime(System.currentTimeMillis())
                        .setTopologyType(topologyContextId == ComponentUtils.REALTIME_TOPOLOGY_CONTEXT ?
                            TopologyType.REALTIME : TopologyType.PLAN))))
            .setAnalysisStartTimestamp(System.currentTimeMillis())
            .setAnalysisCompleteTimestamp(System.currentTimeMillis());
        final Chooser chooser = new Chooser(topologyEntities, random);

        for (ActionTypeCount actionTypeCount : actionCounts) {
            IntStream.range(0, actionTypeCount.getActionCount())
                .forEach(i -> planBuilder.addAction(actionTypeCount.makeAction(chooser)));
        }

        return planBuilder.build();
    }

    public static MoveCount moveCount(int count) {
        return new MoveCount(count);
    }

    public static ReconfigureCount reconfigureCount(int count) {
        return new ReconfigureCount(count);
    }

    public static ProvisionCount provisionCount(int count) {
        return new ProvisionCount(count);
    }

    public static ResizeCount resizeCount(int count) {
        return new ResizeCount(count);
    }

    public static ActivateCount activateCount(int numMoves) {
        return new ActivateCount(numMoves);
    }

    public static DeactivateCount deactivateCount(int numMoves) {
        return new DeactivateCount(numMoves);
    }

    /**
     * Wraps an entity OID and that entity's EntityTYpe together.
     *
     * We use this to ensure every time we reference the entity type of an entity, we have
     * a consistent assignment.
     */
    @VisibleForTesting
    static class GeneratedEntity {
        private final long oid;
        private final EntityType entityType;

        /**
         * The entity types that will be generated for entities used in the action plan.
         */
        public static final List<EntityType> ELIGIBLE_ENTITY_TYPES = ImmutableList.of(
            EntityType.STORAGE,
            EntityType.DISK_ARRAY,
            EntityType.STORAGE_CONTROLLER,
            EntityType.LOGICAL_POOL,
            EntityType.DATACENTER,
            EntityType.PHYSICAL_MACHINE,
            EntityType.VIRTUAL_MACHINE,
            EntityType.VIRTUAL_DATACENTER,
            EntityType.BUSINESS_APPLICATION,
            EntityType.APPLICATION_SERVER,
            EntityType.APPLICATION,
            EntityType.VIRTUAL_APPLICATION,
            EntityType.CHASSIS,
            EntityType.CONTAINER,
            EntityType.CONTAINER_POD
        );

        public GeneratedEntity(final long oid, @Nonnull final EntityType entityType) {
            this.oid = oid;
            this.entityType = entityType;
        }

        public long getOid() {
            return oid;
        }

        public EntityType getEntityType() {
            return entityType;
        }

        public ActionEntity asActionEntity() {
            return ActionEntity.newBuilder()
                .setId(oid)
                .setType(entityType.getNumber())
                .build();
        }

        public static GeneratedEntity randomEntityWithOid(final long oid, @Nonnull final Random random) {
            return new GeneratedEntity(oid, ELIGIBLE_ENTITY_TYPES.get(
                random.nextInt(ELIGIBLE_ENTITY_TYPES.size())
            ));
        }
    }

    /**
     * A small helper utility that knows how to choose values at random.
     */
    private static class Chooser {
        private final List<GeneratedEntity> topologyActionEntities;
        private final Random random;
        private long nextNewOid;

        private Chooser(@Nonnull final List<GeneratedEntity> topologyActionEntities, @Nonnull final Random random) {
            this.topologyActionEntities = topologyActionEntities;
            this.random = random;

            // Select the topologyOid new OID as 1 greater than the maximum in the topology.
            nextNewOid = topologyActionEntities.stream()
                .map(GeneratedEntity::getOid)
                .max(Long::compare)
                .get() + 1;
        }

        /**
         * Get an OID at random from the {@link Chooser}'s topology.
         * @return A random OID.
         */
        private long topologyOid() {
            return topologyActionEntities.get(random.nextInt(topologyActionEntities.size())).getOid();
        }

        /**
         * Get a GeneratedEntity (OID + EntityType) at random from the {@link Chooser}'s topology.
         * @return A random OID.
         */
        private GeneratedEntity generatedEntity() {
            return topologyActionEntities.get(random.nextInt(topologyActionEntities.size()));
        }

        /**
         * Get an ActionEntity (OID + EntityType) at random from the {@link Chooser}'s topology.
         * @return A random OID.
         */
        private ActionEntity actionEntity() {
            return topologyActionEntities.get(random.nextInt(topologyActionEntities.size()))
                .asActionEntity();
        }

        /**
         * Get a new OID.
         *
         * @return An OID not in the topology and has not be retrieved yet.
         */
        public long nextNewOid() {
            long current = nextNewOid;
            nextNewOid++;

            return current;
        }

        /**
         * Get a random double.
         *
         * @return A random double.
         */
        public double nextDouble() {
            return random.nextDouble();
        }


        /**
         * Get a random float.
         *
         * @return A random float.
         */
        public float nextFloat() {
            return random.nextFloat();
        }

        /**
         * Get a random commodity type.
         *
         * @return A random commodity type.
         */
        public CommodityType commodityType() {
            final int typeNumber = CommodityDTO.CommodityType.values()
                [random.nextInt(CommodityDTO.CommodityType.values().length)].getNumber();

            return CommodityType.newBuilder()
                .setType(typeNumber)
                .build();
        }
    }

    /**
     * How many actions of a particular type should be added to an action plan.
     */
    public abstract static class ActionTypeCount {
        private int actionCount;

        public ActionTypeCount(int actionCount) {
            if (actionCount < 0) {
                throw new IllegalArgumentException("Illegal actionCount: " + actionCount);
            }
            this.actionCount = actionCount;
        }

        public int getActionCount() {
            return actionCount;
        }

        public void incrementCount() {
            actionCount++;
        }

        /**
         * A helper that sets the shared properties for an action.
         *
         * @param explanationBuilder The explanation for the action to create.
         * @param actionBuilder The builder for the type-specific properties of the action.
         * @param chooser The chooser to use to choose values for created actions.
         * @return An action with appropriate properties.
         */
        protected Action makeAction(@Nonnull final Explanation.Builder explanationBuilder,
                                    @Nonnull final ActionInfo.Builder actionBuilder,
                                    @Nonnull final Chooser chooser) {
            return Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(chooser.nextDouble())
                .setExplanation(explanationBuilder)
                .setInfo(actionBuilder)
                .setExecutable(true)
                .build();
        }

        /**
         * Make an action of a given type, setting values of the action using the provided chooser.
         *
         * @param chooser The chooser to use to choose values for created actions.
         * @return An action of the appropriate type.
         */
        protected abstract Action makeAction(@Nonnull final Chooser chooser);
    }

    /**
     * How many Move actions to add to an action plan.
     */
    public static class MoveCount extends ActionTypeCount {
        private MoveCount(int actionCount) {
            super(actionCount);
        }

        @Override
        protected Action makeAction(@Nonnull Chooser chooser) {
            return makeAction(
                Explanation.newBuilder()
                    .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(
                            ChangeProviderExplanation.getDefaultInstance()
                        )
                    ),
                ActionInfo.newBuilder()
                    .setMove(Move.newBuilder()
                        .setTarget(chooser.actionEntity())
                        .addChanges(ChangeProvider.newBuilder()
                            .setSource(chooser.actionEntity())
                            .setDestination(chooser.actionEntity())
                            .build())
                        .build()),
                chooser);
        }
    }

    /**
     * How many Reconfigure actions to add to an action plan.
     */
    public static class ReconfigureCount extends ActionTypeCount {
        private ReconfigureCount(int actionCount) {
            super(actionCount);
        }

        @Override
        protected Action makeAction(@Nonnull Chooser chooser) {
            return makeAction(
                Explanation.newBuilder()
                    .setReconfigure(ReconfigureExplanation.getDefaultInstance()),
                ActionInfo.newBuilder()
                    .setReconfigure(Reconfigure.newBuilder()
                        .setSource(chooser.actionEntity())
                        .setTarget(chooser.actionEntity())),
                chooser
            );
        }
    }

    /**
     * How many Provision actions to add to an action plan.
     */
    public static class ProvisionCount extends ActionTypeCount {
        private ProvisionCount(int actionCount) {
            super(actionCount);
        }

        @Override
        protected Action makeAction(@Nonnull Chooser chooser) {
            return makeAction(
                Explanation.newBuilder()
                    .setProvision(ProvisionExplanation.getDefaultInstance()),
                ActionInfo.newBuilder()
                    .setProvision(Provision.newBuilder()
                        .setEntityToClone(chooser.actionEntity())
                        .setProvisionedSeller(chooser.nextNewOid())),
                chooser
            );
        }
    }

    /**
     * How many Resize actions to add to an action plan.
     */
    public static class ResizeCount extends ActionTypeCount {
        private ResizeCount(int actionCount) {
            super(actionCount);
        }

        @Override
        protected Action makeAction(@Nonnull Chooser chooser) {
            return makeAction(
                Explanation.newBuilder()
                    .setResize(ResizeExplanation.newBuilder()
                        .setDeprecatedStartUtilization(chooser.nextFloat())
                        .setDeprecatedEndUtilization(chooser.nextFloat())),
                ActionInfo.newBuilder()
                    .setResize(Resize.newBuilder()
                        .setTarget(chooser.actionEntity())
                        .setOldCapacity(chooser.nextFloat())
                        .setNewCapacity(chooser.nextFloat())),
                chooser
            );
        }
    }

    /**
     * How many Activate actions to add to an action plan.
     */
    public static class ActivateCount extends ActionTypeCount {
        private ActivateCount(int actionCount) {
            super(actionCount);
        }

        @Override
        protected Action makeAction(@Nonnull Chooser chooser) {
            return makeAction(
                Explanation.newBuilder()
                    .setActivate(ActivateExplanation.newBuilder()
                        .setMostExpensiveCommodity(chooser.commodityType().getType())),
                ActionInfo.newBuilder()
                    .setActivate(Activate.newBuilder()
                        .setTarget(chooser.actionEntity())
                        .addTriggeringCommodities(chooser.commodityType())),
                chooser
            );
        }
    }

    /**
     * How many Deactivate actions to add to an action plan.
     */
    public static class DeactivateCount extends ActionTypeCount {
        private DeactivateCount(int actionCount) {
            super(actionCount);
        }


        @Override
        protected Action makeAction(@Nonnull Chooser chooser) {
            return makeAction(
                Explanation.newBuilder()
                    .setDeactivate(DeactivateExplanation.getDefaultInstance()),
                ActionInfo.newBuilder()
                    .setDeactivate(Deactivate.newBuilder()
                        .setTarget(chooser.actionEntity())
                        .addTriggeringCommodities(chooser.commodityType())
                        .build()),
                chooser);
        }
    }
}
