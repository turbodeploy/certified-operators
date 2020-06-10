package com.vmturbo.market.component.api.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketSubscription.Topic;

/**
 * Spring configuration to import to connecto to Market instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
@Import(BaseKafkaConsumerConfig.class)
public class MarketClientConfig {

    @Value("${marketHost}")
    private String marketHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;


    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConfig;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    @Bean
    public Channel marketChannel() {
        return ComponentGrpcServer.newChannelBuilder(marketHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    protected IMessageReceiver<ActionPlan> actionPlanReceiver(final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
                new TopicSettings(MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC, startFrom),
                ActionPlan::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC,
                ActionPlan::parseFrom));
    }

    @Bean
    protected IMessageReceiver<AnalysisSummary> analysisSummaryReceiver(final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
                new TopicSettings(MarketComponentNotificationReceiver.ANALYSIS_SUMMARY_TOPIC, startFrom),
                AnalysisSummary::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.ANALYSIS_SUMMARY_TOPIC,
                AnalysisSummary::parseFrom));
    }

    @Bean
    protected IMessageReceiver<ProjectedTopology> projectedTopologyReceiver(final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
                new TopicSettings(MarketComponentNotificationReceiver.PROJECTED_TOPOLOGIES_TOPIC, startFrom),
                ProjectedTopology::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_TOPOLOGIES_TOPIC,
                ProjectedTopology::parseFrom));
    }

    @Bean
    protected IMessageReceiver<ProjectedEntityCosts> projectedEntityCostsReceiver(final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
                new TopicSettings(MarketComponentNotificationReceiver.PROJECTED_ENTITY_COSTS_TOPIC, startFrom),
                ProjectedEntityCosts::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_ENTITY_COSTS_TOPIC,
                ProjectedEntityCosts::parseFrom));
    }

    @Bean
    protected IMessageReceiver<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageReceiver(final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
                new TopicSettings(MarketComponentNotificationReceiver.PROJECTED_ENTITY_RI_COVERAGE_TOPIC, startFrom),
                ProjectedEntityReservedInstanceCoverage::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_ENTITY_RI_COVERAGE_TOPIC,
                ProjectedEntityReservedInstanceCoverage::parseFrom));
    }

    @Bean
    protected IMessageReceiver<Topology> planAnalysisTopologyReceiver(final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
                new TopicSettings(MarketComponentNotificationReceiver.PLAN_ANALYSIS_TOPOLOGIES_TOPIC, startFrom),
                Topology::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.PLAN_ANALYSIS_TOPOLOGIES_TOPIC,
                Topology::parseFrom));
    }

    @Bean
    protected IMessageReceiver<AnalysisStatusNotification> analysisStatusReceiver(final Optional<StartFrom> startFromOverride) {
        return startFromOverride
        .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
            new TopicSettings(MarketComponentNotificationReceiver.ANALYSIS_STATUS_NOTIFICATION_TOPIC, startFrom),
            AnalysisStatusNotification::parseFrom))
        .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
            MarketComponentNotificationReceiver.ANALYSIS_STATUS_NOTIFICATION_TOPIC,
            AnalysisStatusNotification::parseFrom));
    }

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService marketClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("market-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Nonnull
    public MarketComponent marketComponent(@Nonnull MarketSubscription... subscriptions) {
        final Map<Topic, Optional<StartFrom>> topicsAndOverrides = new HashMap<>();
        for (MarketSubscription sub : subscriptions) {
            topicsAndOverrides.put(sub.getTopic(), sub.getStartFrom());
        }
        final IMessageReceiver<ActionPlan> actionPlansReceiver =
            topicsAndOverrides.containsKey(Topic.ActionPlans) ?
                actionPlanReceiver(topicsAndOverrides.get(Topic.ActionPlans)) : null;
        final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver =
            topicsAndOverrides.containsKey(Topic.ProjectedTopologies) ?
                projectedTopologyReceiver(topicsAndOverrides.get(Topic.ProjectedTopologies)) : null;
        final IMessageReceiver<ProjectedEntityCosts> projectedEntityCostReceiver =
            topicsAndOverrides.containsKey(Topic.ProjectedEntityCosts) ?
                projectedEntityCostsReceiver(topicsAndOverrides.get(Topic.ProjectedEntityCosts)) : null;
        final IMessageReceiver<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageReceiver =
            topicsAndOverrides.containsKey(Topic.ProjectedEntityRiCoverage) ?
                projectedEntityRiCoverageReceiver(topicsAndOverrides.get(Topic.ProjectedEntityRiCoverage)) : null;
        final IMessageReceiver<Topology> planAnalysisTopologyReceiver =
            topicsAndOverrides.containsKey(Topic.PlanAnalysisTopologies) ?
                    planAnalysisTopologyReceiver(topicsAndOverrides.get(Topic.PlanAnalysisTopologies)) : null;
        final IMessageReceiver<AnalysisSummary> analysisSummaryReceiver =
            topicsAndOverrides.containsKey(Topic.AnalysisSummary) ?
                analysisSummaryReceiver(topicsAndOverrides.get(Topic.AnalysisSummary)) : null;
        final IMessageReceiver<AnalysisStatusNotification> analysisStatusReceiver =
             topicsAndOverrides.containsKey(Topic.AnalysisStatusNotification) ?
                analysisStatusReceiver(topicsAndOverrides.get(Topic.AnalysisStatusNotification)) : null;
        return new MarketComponentNotificationReceiver(projectedTopologyReceiver,
                projectedEntityCostReceiver, projectedEntityRiCoverageReceiver, actionPlansReceiver,
                planAnalysisTopologyReceiver, analysisSummaryReceiver, analysisStatusReceiver, marketClientThreadPool(),
                kafkaReceiverTimeoutSeconds);
    }
}
