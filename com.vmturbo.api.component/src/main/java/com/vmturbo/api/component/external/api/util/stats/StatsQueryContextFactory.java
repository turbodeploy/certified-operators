package com.vmturbo.api.component.external.api.util.stats;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;

/**
 * Factory to create {@link StatsQueryContext} instances.
 */
public class StatsQueryContextFactory {
    private static final Logger logger = LogManager.getLogger();

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
        // OM-37484: give the startTime a +/- 60 second window for delineating between "current"
        // and "projected" stats requests. Without this window, the stats retrieval is too
        // sensitive to clock skew issues between the browser and the server, leading to incorrect
        // results in the UI.
        final long currentStatsTimeWindowStart = clockTimeNow - liveStatsRetrievalWindow.toMillis();
        final long currentStatsTimeWindowEnd = clockTimeNow + liveStatsRetrievalWindow.toMillis();

        final Long startTime;
        if (inputDTO.getStartDate() == null) {
            startTime = null;
        } else {
            final long inputStartTime = DateTimeUtil.parseTime(inputDTO.getStartDate());
            if (inputStartTime >= currentStatsTimeWindowStart && inputStartTime <= currentStatsTimeWindowEnd) {
                startTime = null;
            } else {
                startTime = inputStartTime;
            }
        }

        final Long endTime = (inputDTO.getEndDate() == null || inputDTO.getEndDate().equalsIgnoreCase("1M")) ?
            null : DateTimeUtil.parseTime(inputDTO.getEndDate());


        final Optional<TimeWindow> timeWindow;
        if (startTime != null && endTime != null) {
            timeWindow = Optional.of(ImmutableTimeWindow.builder()
                .startTime(startTime)
                .endTime(endTime)
                .build());
        } else {
            timeWindow = Optional.empty();
        }

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

        private final Set<StatApiInputDTO> requestedStats;

        private final long curTime;

        private final Optional<TimeWindow> timeWindow;

        private final UserSessionContext userSessionContext;

        private final List<ThinTargetInfo> targets;

        private Optional<PlanInstance> planInstance = null;

        private final Set<Long> scopeEntities;

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
            this.scope = scope;
            this.requestedStats = requestedStats;
            this.curTime = curTime;
            this.timeWindow = timeWindow;
            this.userSessionContext = userSessionContext;
            this.targets = targets;
            this.scopeEntities = expandedScope.getEntities();
        }

        @Value.Immutable
        public interface TimeWindow {
            long startTime();
            long endTime();

            default boolean contains(final long time) {
                return startTime() <= time && endTime() >= time;
            }
        }

        @Nonnull
        public ApiId getScope() {
            return scope;
        }

        @Nonnull
        public Set<StatApiInputDTO> getRequestedStats() {
            return Collections.unmodifiableSet(requestedStats);
        }

        public boolean includeCurrent() {
            return timeWindow.map(window -> window.contains(curTime)).orElse(true);
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
                periodApiInputDTO.setStartDate(DateTimeUtil.toString(window.startTime()));
                periodApiInputDTO.setEndDate(DateTimeUtil.toString(window.endTime()));
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
                this.planInstance = scope.getPlanInstance();
            }
            return this.planInstance;
        }

        @Nonnull
        public Set<Long> getScopeEntities() {
            return scopeEntities;
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
