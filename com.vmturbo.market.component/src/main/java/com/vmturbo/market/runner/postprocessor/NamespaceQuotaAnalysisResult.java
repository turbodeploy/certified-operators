package com.vmturbo.market.runner.postprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;

/**
 * Wrapper class for namespace quota analysis result, including list of generated namespace quota
 * resizing actions, map of namespace actions by entity OID and commodity type and map of related
 * namespace resizing actions by container resizing action ID.
 */
public class NamespaceQuotaAnalysisResult {
    /**
     * List of namespace quota resize actions.
     */
    private final List<Action> namespaceQuotaResizeActions;

    /**
     * Map of namespace OID to map of namespace action by commodity type, which will be used to
     * interpret related namespace actions for corresponding container resize actions.
     */
    private final Map<Long, Map<Integer, Action>> nsActionsByEntityOIDAndCommodityType;

    /**
     * NamespaceQuotaAnalysisResult constructor.
     */
    public NamespaceQuotaAnalysisResult() {
        namespaceQuotaResizeActions = new ArrayList<>();
        nsActionsByEntityOIDAndCommodityType = new HashMap<>();
    }

    /**
     * Add namespace resize action to {@link NamespaceQuotaAnalysisResult} and populate
     * nsActionsByEntityOIDAndCommodityType which will be used to interpret related namespace actions
     * for corresponding containe resize actions.
     *
     * @param nsResizeAction Namespace resize action to be added to the result.
     */
    public void addNamespaceResizeAction(@Nonnull final Action nsResizeAction) {
        namespaceQuotaResizeActions.add(nsResizeAction);
        nsActionsByEntityOIDAndCommodityType.computeIfAbsent(nsResizeAction.getInfo().getResize().getTarget().getId(),
                        k -> new HashMap<>())
                .put(nsResizeAction.getInfo().getResize().getCommodityType().getType(), nsResizeAction);
    }

    /**
     * Get list of namespace quota resize actions.
     *
     * @return List of namespace quota resize actions.
     */
    public List<Action> getNamespaceQuotaResizeActions() {
        return namespaceQuotaResizeActions;
    }

    /**
     * Get map of namespace OID to map of namespace action by commodity type, which will be used to
     * interpret related namespace actions for corresponding container resize actions.
     *
     * @return Map of namespace OID to map of namespace action by commodity type.
     */
    public Map<Long, Map<Integer, Action>> getNSActionsByEntityOIDAndCommodityType() {
        return nsActionsByEntityOIDAndCommodityType;
    }
}
