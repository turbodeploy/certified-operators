package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * {@link TemplateCostStatsSubQuery} tests.
 */
public class TemplateCostStatsSubQueryTest {

    private TemplateCostStatsSubQuery query;

    @Mock
    private TemplatesUtils templatesUtils;

    @Mock
    private StatApiInputDTO apiInputDto;

    /**
     * Initial set up for the tests.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        query = new TemplateCostStatsSubQuery(templatesUtils);
    }

    /**
     * Tests applicableInContext method.
     * Case: is plan
     */
    @Test
    public void testApplicableInContext() {
        final ApiId scope = Mockito.mock(ApiId.class);
        Mockito.when(scope.isPlan()).thenReturn(true);

        final StatsQueryContext context = Mockito.mock(StatsQueryContext.class);
        Mockito.when(context.getInputScope()).thenReturn(scope);

        Assert.assertTrue(query.applicableInContext(context));
    }

    /**
     * Tests applicableInContext method.
     * Case: is not plan
     */
    @Test
    public void testNotApplicableInContext() {
        final ApiId scope = Mockito.mock(ApiId.class);
        Mockito.when(scope.isPlan()).thenReturn(false);

        final StatsQueryContext context = Mockito.mock(StatsQueryContext.class);
        Mockito.when(context.getInputScope()).thenReturn(scope);

        Assert.assertFalse(query.applicableInContext(context));
    }

    /**
     * Tests getAggregateStats method that returns template price.
     *
     * @throws OperationFailedException If anything goes wrong
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Test
    public void testGetAggregateStats()
                    throws OperationFailedException, InterruptedException, ConversionException {
        final StatsQueryContext context = createContext(222222L);
        final TemplateApiDTO template = new TemplateApiDTO();
        template.setUuid("222222");
        template.setDisplayName("test template");
        template.setPrice(234.0f);
        Mockito.when(templatesUtils.getTemplates(GetTemplatesRequest.newBuilder()
                        .setFilter(TemplatesFilter.newBuilder()
                                        .addAllTemplateIds(Collections.singletonList(222222L)))
                        .build())).thenReturn(Stream.of(template));

        final List<StatSnapshotApiDTO> results = query.getAggregateStats(Collections
                        .singleton(apiInputDto), context);
        Assert.assertEquals(1, results.size());
        StatSnapshotApiDTO statsSnapshot = results.get(0);
        Assert.assertEquals(Epoch.PLAN_PROJECTED, statsSnapshot.getEpoch());
        final List<StatApiDTO> statistics = statsSnapshot.getStatistics();
        Assert.assertEquals(1, statistics.size());
        StatApiDTO stats = statistics.get(0);
        Assert.assertEquals(StringConstants.COST_PRICE, stats.getName());
        Assert.assertEquals(234, stats.getValue(), 0.001);
    }

    private static StatsQueryContext createContext(Long templateId) {
        final StatsQueryContext context = Mockito.mock(StatsQueryContext.class);
        final ScenarioInfo.Builder scenarioInfoBuilder = ScenarioInfo.newBuilder();
        if (templateId != null) {
            scenarioInfoBuilder.addChanges(ScenarioChange
                            .newBuilder()
                            .setTopologyReplace(TopologyReplace.newBuilder()
                                            .setAddTemplateId(templateId)));
        }
        final Scenario scenario = Scenario.newBuilder().setScenarioInfo(scenarioInfoBuilder).build();
        final PlanInstance plan = PlanInstance.newBuilder().setPlanId(1234L)
                        .setStatus(PlanStatus.SUCCEEDED).setScenario(scenario)
                        .setStartTime(1611060000000L).build();
        Mockito.when(context.getPlanInstance()).thenReturn(Optional.of(plan));
        return context;
    }

    /**
     * Tests getAggregateStats method when context doesn't have templates.
     *
     * @throws OperationFailedException If anything goes wrong
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Test
    public void testGetAggregateStatsWithoutTemplates()
                    throws OperationFailedException, InterruptedException, ConversionException {
        final StatsQueryContext context = createContext(null);
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(Collections
                        .singleton(apiInputDto), context);
        Assert.assertTrue(results.isEmpty());
    }

}
