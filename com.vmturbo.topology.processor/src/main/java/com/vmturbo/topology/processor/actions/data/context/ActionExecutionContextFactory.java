package com.vmturbo.topology.processor.actions.data.context;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.PolicyRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Constructs instances of ActionExecutionContext, an interface for collecting data needed for
 * action execution.
 *
 * The factory must be instantiated before use.
 */
public class ActionExecutionContextFactory {

    /**
     * Tracks data requirements for handling action execution special cases (i.e. complex actions)
     */
    private final ActionDataManager actionDataManager;

    /**
     * Used to fetch additional data about the entities involved in the action
     * Specifically, this is where raw discovery data is stored
     */
    private final EntityStore entityStore;

    /**
     * Retrieves and converts an entity in order to provide the full entity data for action
     * execution
     */
    private final EntityRetriever entityRetriever;

    /**
     * Used for determining the target type of a given target
     */
    private final TargetStore targetStore;

    /**
     * Used for determining the action policy of a given target.
     */
    private final ProbeStore probeStore;

    /**
     * Retrieves policies required for action execution.
     */
    protected final PolicyRetriever policyRetriever;

    public ActionExecutionContextFactory(@Nonnull final ActionDataManager actionDataManager,
                                         @Nonnull final EntityStore entityStore,
                                         @Nonnull final EntityRetriever entityRetriever,
                                         @Nonnull final TargetStore targetStore,
                                         @Nonnull final ProbeStore probeStore,
                                         @Nonnull final PolicyRetriever policyRetriever) {
        this.actionDataManager = Objects.requireNonNull(actionDataManager);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.entityRetriever = Objects.requireNonNull(entityRetriever);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.policyRetriever = Objects.requireNonNull(policyRetriever);
    }

    /**
     * Create an {@link ActionExecutionContext} to collect additional data needed in order to
     * execute the provided action.
     *
     * @param request an request containing an action to be executed
     * @return an {@link ActionExecutionContext} to collect additional data needed in order to
     *         execute the provided action.
     */
    @Nonnull
    public ActionExecutionContext getActionExecutionContext(
            @Nonnull final ExecuteActionRequest request) {
        return ActionExecutionContextFactory.getActionExecutionContext(
                Objects.requireNonNull(request),
                actionDataManager,
                entityStore,
                entityRetriever,
                targetStore,
                probeStore,
                policyRetriever);
    }

    /**
     * Create an {@link ActionExecutionContext} to collect additional data needed in order to
     * execute the provided action.
     *
     * @param request an request containing an action to be executed.
     * @param dataManager tracks data requirements for handling action execution special cases
     * @param entityStore used to fetch additional data about the entities involved in the action
     * @param entityRetriever retrieves and converts an entity in order to provide the full entity
     *                        data for action execution.
     * @param targetStore used to check crossTarget move in MoveContext.
     * @param probeStore used for determining the action policy of a given target.
     * @param policyRetriever the object for retrieving policies.
     * @return an {@link ActionExecutionContext} to collect additional data needed in order to
     *         execute the provided action.
     */
    @Nonnull
    private static ActionExecutionContext getActionExecutionContext(
            @Nonnull final ExecuteActionRequest request,
            @Nonnull final ActionDataManager dataManager,
            @Nonnull final EntityStore entityStore,
            @Nonnull final EntityRetriever entityRetriever,
            @Nonnull final TargetStore targetStore,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final PolicyRetriever policyRetriever
            ) {
        if (!request.hasActionSpec()) {
            throw new IllegalArgumentException("Cannot execute action with no action info. "
                    + "Action request: " + request.toString());
        }
        ActionInfo actionInfo = request.getActionSpec().getRecommendation().getInfo();
        switch (actionInfo.getActionTypeCase()) {
            case MOVE:
                return new MoveContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case SCALE:
                return new ScaleContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case RESIZE:
                return new ResizeContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case ACTIVATE:
                return new ActivateContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case DEACTIVATE:
                return  new DeactivateContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case PROVISION:
                return new ProvisionContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case DELETE:
                return new DeleteContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case ATOMICRESIZE:
                return new AtomicResizeContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            case RECONFIGURE:
                return new ReconfigureContext(request, dataManager, entityStore, entityRetriever,
                    targetStore, probeStore, policyRetriever);
            default:
                throw new IllegalArgumentException("Unsupported action type: " +
                        actionInfo.getActionTypeCase());
        }
    }
}
