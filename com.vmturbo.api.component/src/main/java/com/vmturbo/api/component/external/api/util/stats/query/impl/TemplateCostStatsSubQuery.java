package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Template cost sub-query. Provides prices for templates from scenario change
 * that contains topology replace list.
 */
public class TemplateCostStatsSubQuery implements StatsSubQuery {

    private final TemplatesUtils templatesUtils;

    /**
     * Creates {@link TemplateCostStatsSubQuery} instance.
     *
     * @param templatesUtils the service used to retrieve template information
     */
    public TemplateCostStatsSubQuery(@Nonnull TemplatesUtils templatesUtils) {
        this.templatesUtils = Objects.requireNonNull(templatesUtils);
    }

    @Override
    public boolean applicableInContext(StatsQueryContext context) {
        return context.getInputScope().isPlan() && !context.getInputScope().isCloud();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(StatsQueryContext context) {
        return SubQuerySupportedStats.some(context
                        .findStats(Collections.singleton(StringConstants.COST_PRICE)));
    }

    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(Set<StatApiInputDTO> stats,
                                                      StatsQueryContext context)
                    throws OperationFailedException, InterruptedException, ConversionException {
        if (!context.getPlanInstance().isPresent()) {
            return Collections.emptyList();
        }
        final PlanInstance planInstance = context.getPlanInstance().get();
        final Set<Long> templateIds = planInstance.getScenario().getScenarioInfo()
                        .getChangesList().stream().filter(ScenarioChange::hasTopologyReplace)
                        .map(ScenarioChange::getTopologyReplace)
                        .map(TopologyReplace::getAddTemplateId)
                        .collect(Collectors.toSet());
        if (templateIds.isEmpty()) {
            return Collections.emptyList();
        }
        final GetTemplatesRequest request = GetTemplatesRequest.newBuilder()
                        .setFilter(TemplatesFilter.newBuilder()
                                        .addAllTemplateIds(templateIds))
                        .build();
        final List<TemplateApiDTO> retTemplates = templatesUtils.getTemplates(request)
                        .collect(Collectors.toList());
        if (retTemplates.isEmpty()) {
            return Collections.emptyList();
        }
        final List<StatSnapshotApiDTO> statSnapshots = new ArrayList<>();
        for (TemplateApiDTO template : retTemplates) {
            final Float price = template.getPrice();
            final StatApiDTO statApiDTO = new StatApiDTO();
            statApiDTO.setName(StringConstants.COST_PRICE);
            statApiDTO.setValue(price);
            statApiDTO.setRelatedEntity(template);

            final StatValueApiDTO values = new StatValueApiDTO();
            values.setTotal(price);
            values.setMin(price);
            values.setMax(price);
            values.setAvg(price);
            statApiDTO.setValues(values);

            StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
            final long statsTime = planInstance.getPlanProgress().getSourceTopologySummary()
                    .getTopologyInfo().getCreationTime();
            statSnapshotApiDTO.setDate(DateTimeUtil.toString(statsTime));
            statSnapshotApiDTO.setEpoch(Epoch.PLAN_PROJECTED);
            statSnapshotApiDTO.setStatistics(Collections.singletonList(statApiDTO));
            statSnapshots.add(statSnapshotApiDTO);
        }
        return statSnapshots;
    }

}
