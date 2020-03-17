package com.vmturbo.api.component.external.api.util.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.GroupByFilters.GroupByFiltersFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class GroupByFiltersTest {

    private ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);

    private GroupByFiltersFactory groupByFiltersFactory = new GroupByFiltersFactory(actionSpecMapper);

    @Test
    public void unsetFilter() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Collections.singletonList("foo"));

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(inputDTO);
        when(query.getCostType()).thenReturn(Optional.empty());

        final List<StatFilterApiDTO> filters =
            groupByFiltersFactory.filtersForQuery(query).getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is("foo"));
        assertThat(filter.getValue(), is(""));
    }

    @Test
    public void testInvestmentCostFilter() {
        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(new ActionApiInputDTO());
        when(query.getCostType()).thenReturn(Optional.of(ActionCostType.INVESTMENT));

        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        final List<StatFilterApiDTO> filters = groupByFilters.getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is(StringConstants.PROPERTY));
        assertThat(filter.getValue(), is(StringConstants.INVESTMENT));
    }

    @Test
    public void testSavingsCostFilter() {
        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(new ActionApiInputDTO());
        when(query.getCostType()).thenReturn(Optional.of(ActionCostType.SAVING));

        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        final List<StatFilterApiDTO> filters = groupByFilters.getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is(StringConstants.PROPERTY));
        assertThat(filter.getValue(), is(StringConstants.SAVINGS));
    }

    @Test
    public void categoryFilter() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Collections.singletonList(StringConstants.RISK_SUB_CATEGORY));

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(inputDTO);
        when(query.getCostType()).thenReturn(Optional.empty());

        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        groupByFilters.setCategory(ActionCategory.EFFICIENCY_IMPROVEMENT);

        final List<StatFilterApiDTO> filters = groupByFilters.getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is(StringConstants.RISK_SUB_CATEGORY));
        assertThat(filter.getValue(), is("Efficiency Improvement"));
    }

    @Test
    public void stateFilter() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Collections.singletonList(StringConstants.ACTION_STATES));

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(inputDTO);
        when(query.getCostType()).thenReturn(Optional.empty());

        when(actionSpecMapper.mapXlActionStateToApi(ActionState.READY))
            .thenReturn(com.vmturbo.api.enums.ActionState.RECOMMENDED);

        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        groupByFilters.setState(ActionState.READY);

        final List<StatFilterApiDTO> filters = groupByFilters.getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is(StringConstants.ACTION_STATES));
        assertThat(filter.getValue(), is("RECOMMENDED"));
    }

    @Test
    public void typeFilter() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Collections.singletonList(StringConstants.ACTION_TYPES));

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(inputDTO);
        when(query.getCostType()).thenReturn(Optional.empty());

        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        groupByFilters.setType(ActionType.MOVE);

        final List<StatFilterApiDTO> filters = groupByFilters.getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is(StringConstants.ACTION_TYPES));
        assertThat(filter.getValue(), is("MOVE"));
    }

    @Test
    public void reasonCommodityFilter() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Collections.singletonList(StringConstants.REASON_COMMODITY));

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(inputDTO);
        when(query.getCostType()).thenReturn(Optional.empty());

        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        groupByFilters.setReasonCommodity(CommodityType.VMEM_VALUE);

        final List<StatFilterApiDTO> filters = groupByFilters.getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is(StringConstants.REASON_COMMODITY));
        assertThat(filter.getValue(), is("VMem"));
    }

    @Test
    public void targetEntityTypeFilter() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Collections.singletonList(StringConstants.TARGET_TYPE));

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.actionInput()).thenReturn(inputDTO);
        when(query.getCostType()).thenReturn(Optional.empty());

        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        groupByFilters.setTargetEntityType(EntityType.VIRTUAL_MACHINE_VALUE);

        final List<StatFilterApiDTO> filters = groupByFilters.getFilters();
        assertThat(filters.size(), is(1));
        final StatFilterApiDTO filter = filters.get(0);
        assertThat(filter.getType(), is(StringConstants.TARGET_TYPE));
        assertThat(filter.getValue(), is("VirtualMachine"));
    }
}