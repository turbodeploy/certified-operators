package com.vmturbo.api.component.external.api.util.action;

import java.util.LinkedHashMap;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ActionTypeMapper;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Utility class to capture the filters specified in an {@link ActionStatsQuery},
 * allow setting the values of those filters, and recite them back when constructing
 * the {@link StatApiDTO} result.
 * <p>
 * The intended workflow:
 *    - Construct a {@link GroupByFilters} using a {@link GroupByFiltersFactory}.
 *    - Call the various methods to set values for the filters -
 *      e.g. {@link GroupByFilters#setCategory(ActionCategory)}.
 *    - Call {@link GroupByFilters#getFilters()} to get the list of {@link StatFilterApiDTO}s to
 *      return to the API caller.
 * <p>
 * The main reason we have this utility is to return filters that had no value set by XL
 * (e.g. if asked to group by a non-existing/invalid criteria).
 */
public class GroupByFilters {

    private final ActionSpecMapper actionSpecMapper;

    private final LinkedHashMap<String, StatFilterApiDTO> filtersByName;

    /**
     * Intentionally private/hidden. Use {@link GroupByFiltersFactory}.
     */
    private GroupByFilters(@Nonnull final ActionSpecMapper actionSpecMapper,
                           @Nonnull final ActionStatsQuery query) {
        this.actionSpecMapper = actionSpecMapper;
        // Preserve order.
        filtersByName = new LinkedHashMap<>();
        CollectionUtils.emptyIfNull(query.actionInput().getGroupBy()).stream()
            .map(groupByName -> {
                final StatFilterApiDTO filter = new StatFilterApiDTO();
                filter.setType(groupByName);
                // Set a value, so that we don't return nulls back to the caller (UI).
                filter.setValue("");
                return filter;
            })
            .forEach(filter -> filtersByName.put(filter.getType(), filter));

        // If the query asks for a cost type, we need to specify the cost category in the list
        // of returned filters. This is because the UI/classic uses a common "costPrice" stat to
        // represent savings and investments, and uses the "property" filter to distinguish
        // between them.
        query.getCostType().ifPresent(costType -> {
            final StatFilterApiDTO costPropFilter = new StatFilterApiDTO();
            costPropFilter.setType(StringConstants.PROPERTY);
            if (costType == ActionCostType.INVESTMENT) {
                costPropFilter.setValue(StringConstants.INVESTMENT);
            } else {
                costPropFilter.setValue(StringConstants.SAVINGS);
            }
            filtersByName.put(StringConstants.PROPERTY, costPropFilter);
        });
    }

    public void setCategory(@Nonnull final ActionCategory category) {
        setValue(StringConstants.RISK_SUB_CATEGORY,
            ActionSpecMapper.mapXlActionCategoryToApi(category));
    }

    /**
     * Mutator for risk string.
     * @param risk The risk (e.g. "Mem congestion")
     */
    public void setRelatedRisk(@Nonnull final String risk) {
        setValue(StringConstants.RISK, risk);
    }

    public void setState(@Nonnull final ActionState state) {
        setValue(StringConstants.ACTION_STATES,
            ActionSpecMapper.mapXlActionStateToApi(state).name());
    }

    public void setType(@Nonnull final ActionType type) {
        setValue(StringConstants.ACTION_TYPES, ActionTypeMapper.toApiApproximate(type).name());
    }

    public void setActionCostType(@Nonnull final ActionDTO.ActionCostType costType) {
        setValue(StringConstants.ACTION_COST_TYPE, costType.name());
    }

    /**
     * Set filter by Action Severity value
     * @param severity - the severity of the Action {@link Severity}
     */
    public void setActionRiskSeverity(@Nonnull final Severity severity) {
        setValue(StringConstants.SEVERITY, severity.name());
    }

    public void setReasonCommodity(final int reasonCommodityBaseType) {
        setValue(StringConstants.REASON_COMMODITY,
            ClassicEnumMapper.COMMODITY_TYPE_MAPPINGS.inverse()
                .get(CommodityType.forNumber(reasonCommodityBaseType)));
    }

    public void setTargetEntityType(final int entityType) {
        setValue(StringConstants.TARGET_TYPE,
            ApiEntityType.fromType(entityType).apiStr());
    }

    /**
     * Add a filter with a particular business account ID.
     *
     * @param businessAccountId The ID of the business account.
     */
    public void setBusinessAccountId(final long businessAccountId) {
        setValue(StringConstants.BUSINESS_UNIT, String.valueOf(businessAccountId));
    }

    /**
     * Add a filter with particular resource group ID.
     *
     * @param resourceGroupId The ID of the resource group.
     */
    public void setResourceGroupId(final long resourceGroupId) {
        setValue(StringConstants.RESOURCE_GROUP, String.valueOf(resourceGroupId));
    }

    public void setTargetEntityId(final long entityId) {
        setValue(StringConstants.TARGET_UUID_CC, String.valueOf(entityId));
    }

    public void setTemplate(final String template) {
        setValue(StringConstants.TEMPLATE, template);
    }

    /**
     * Add a filter with particular csp type.
     *
     * @param cspType the type of cloud service provider
     */
    public void setCSP(final String cspType) {
        setValue(StringConstants.CSP, cspType);
    }

    @Nonnull
    public List<StatFilterApiDTO> getFilters() {
        return Lists.newArrayList(filtersByName.values());
    }

    private void setValue(@Nonnull final String type, @Nonnull final String value) {
        final StatFilterApiDTO filter = this.filtersByName.get(type);
        if (filter != null) {
            filter.setValue(value);
        }
    }

    /**
     * Factory class for {@link GroupByFilters}
     */
    public static class GroupByFiltersFactory {

        private final ActionSpecMapper actionSpecMapper;

        public GroupByFiltersFactory(@Nonnull final ActionSpecMapper actionSpecMapper) {
            this.actionSpecMapper = actionSpecMapper;
        }

        @Nonnull
        GroupByFilters filtersForQuery(@Nonnull final ActionStatsQuery query) {
            return new GroupByFilters(actionSpecMapper, query);
        }
    }
}
