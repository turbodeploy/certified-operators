package com.vmturbo.api.component.external.api.util.stats;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.commons.collections4.ListUtils;
import org.immutables.value.Value;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Factory to create {@link StatsQueryContext} instances.
 */
public class StatsQueryContextFactory {

    private final Duration liveStatsRetrievalWindow;

    private final UserSessionContext userSessionContext;

    private final Clock clock;

    private final ThinTargetCache thinTargetCache;

    public StatsQueryContextFactory(@Nonnull final Duration liveStatsRetrievalWindow,
                                    @Nonnull final UserSessionContext userSessionContext,
                                    @Nonnull final Clock clock,
                                    @Nonnull final ThinTargetCache thinTargetCache) {
        this.liveStatsRetrievalWindow = liveStatsRetrievalWindow;
        this.userSessionContext = userSessionContext;
        this.clock = clock;
        this.thinTargetCache = thinTargetCache;
    }

    @Nonnull
    StatsQueryContext newContext(@Nonnull final ApiId scope,
                                 @Nonnull final StatsQueryScope expandedScope,
                                 @Nonnull final StatPeriodApiInputDTO inputDTO) {
        final long clockTimeNow = clock.millis();
        final Optional<TimeWindow> timeWindow = StatsUtils.sanitizeStartDateOrEndDate(
                inputDTO, clockTimeNow, liveStatsRetrievalWindow.toMillis());

        final List<ThinTargetInfo> targets = thinTargetCache.getAllTargets();

        return new StatsQueryContext(scope,
            userSessionContext,
            new HashSet<>(ListUtils.emptyIfNull(inputDTO.getStatistics())),
            timeWindow,
            targets,
            expandedScope,
            clockTimeNow);
    }

    /**
     * The context for a stats query. Contains information that is useful for the various
     * {@link com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery}
     * implementations, mainly to avoid large numbers of parameters.
     */
    public static class StatsQueryContext {
        private final ApiId scope;

        private final StatsQueryScope queryScope;

        private final Set<StatApiInputDTO> requestedStats;

        private final long curTime;

        private final Optional<TimeWindow> timeWindow;

        private final UserSessionContext userSessionContext;

        private final List<ThinTargetInfo> targets;

        private Optional<PlanInstance> planInstance = null;

        /**
         * Use {@link StatsQueryContextFactory}.
         */
        private StatsQueryContext(@Nonnull final ApiId scope,
                                 @Nonnull final UserSessionContext userSessionContext,
                                 @Nonnull final Set<StatApiInputDTO> requestedStats,
                                 @Nonnull final Optional<TimeWindow> timeWindow,
                                 @Nonnull final List<ThinTargetInfo> targets,
                                 @Nonnull final StatsQueryScope expandedScope,
                                 final long curTime) {
            this.scope = Objects.requireNonNull(scope);
            this.requestedStats = Objects.requireNonNull(requestedStats);
            this.curTime = curTime;
            this.timeWindow = Objects.requireNonNull(timeWindow);
            this.userSessionContext = Objects.requireNonNull(userSessionContext);
            this.targets = Objects.requireNonNull(targets);
            this.queryScope = Objects.requireNonNull(expandedScope);
        }

        @Value.Immutable
        public interface TimeWindow {
            long startTime();
            long endTime();
            boolean includeProjected();
            boolean includeCurrent();
            boolean includeHistorical();
        }

        /**
         * The input scope is the scope that was given by the API user. It's always a single
         * {@link ApiId} indicating some object in the system.
         */
        @Nonnull
        public ApiId getInputScope() {
            return scope;
        }

        /**
         * The query scope is the expansion of the input scope, and specifies which entities
         * to get stats from to fulfill the query.
         */
        @Nonnull
        public StatsQueryScope getQueryScope() {
            return queryScope;
        }

        @Nonnull
        public Set<StatApiInputDTO> getRequestedStats() {
            return Collections.unmodifiableSet(requestedStats);
        }

        public boolean includeCurrent() {
            return timeWindow.map(TimeWindow::includeCurrent).orElse(true);
        }

        public boolean requestProjected() {
            return timeWindow.map(TimeWindow::includeProjected).orElse(false);
        }

        @Nonnull
        public List<ThinTargetInfo> getTargets() {
            return Collections.unmodifiableList(targets);
        }

        public long getCurTime() {
            return curTime;
        }

        @Nonnull
        public StatPeriodApiInputDTO newPeriodInputDto(@Nonnull final Set<StatApiInputDTO> stats) {
            final StatPeriodApiInputDTO periodApiInputDTO = new StatPeriodApiInputDTO();
            timeWindow.ifPresent(window -> {
                // only set start/end date if requiring stats other than current (like historical)
                // no need to worry about projected stats here since they are handled in
                // ProjectedCommodityStatsSubQuery
                if (!window.includeCurrent() || window.includeHistorical()) {
                    periodApiInputDTO.setStartDate(DateTimeUtil.toString(window.startTime()));
                    periodApiInputDTO.setEndDate(DateTimeUtil.toString(window.endTime()));
                }
            });
            periodApiInputDTO.setStatistics(Lists.newArrayList(stats));
            return periodApiInputDTO;
        }

        @Nonnull
        public UserSessionContext getSessionContext() {
            return userSessionContext;
        }

        @Nonnull
        public Optional<PlanInstance> getPlanInstance() {
            if (this.planInstance == null) {
                this.planInstance = scope.getCachedPlanInfo()
                        .map(UuidMapper.CachedPlanInfo::getPlanInstance);
            }
            return this.planInstance;
        }

        @Nonnull
        public Optional<TimeWindow> getTimeWindow() {
            return timeWindow;
        }

        public boolean isGlobalScope() {
            return scope.isRealtimeMarket() || scope.isGlobalTempGroup();
        }

        @Nonnull
        public Set<StatApiInputDTO> findStats(final Set<String> statNames) {
            return this.requestedStats.stream()
                .filter(stat -> stat.getName() != null)
                .filter(stat -> statNames.contains(stat.getName()))
                .collect(Collectors.toSet());
        }
    }
}
