package com.vmturbo.market.component.api.impl;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

/**
 * Spring configuration to import to connecto to Market instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
@Import(BaseKafkaConsumerConfig.class)
public class MarketClientConfig {

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConfig;

    @Bean
    protected IMessageReceiver<ActionPlan> actionPlanReceiver() {
        return baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC,
                ActionPlan::parseFrom);
    }

    @Bean
    protected IMessageReceiver<ProjectedTopology> projectedTopologyReceiver() {
        return baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_TOPOLOGIES_TOPIC,
                ProjectedTopology::parseFrom);
    }

    @Bean
    protected IMessageReceiver<PriceIndexMessage> priceIndexReceiver() {
        return baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.PRICE_INDICES_TOPIC,
                PriceIndexMessage::parseFrom);
    }

    @Bean
    protected IMessageReceiver<Topology> planAnalysisTopologyReceiver() {
        return baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentNotificationReceiver.PLAN_ANALYSIS_TOPOLOGIES_TOPIC,
                Topology::parseFrom);
    }

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService marketClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("market-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    public MarketComponent marketComponent(@Nonnull Set<Subscription> subscriptions) {
        final IMessageReceiver<ActionPlan> actionPlansReceiver =
                subscriptions.contains(Subscription.ActionPlans) ? actionPlanReceiver() : null;
        final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver =
                subscriptions.contains(Subscription.ProjectedTopologies) ?
                        projectedTopologyReceiver() : null;
        final IMessageReceiver<PriceIndexMessage> priceIndexReceiver =
                subscriptions.contains(Subscription.PriceIndexes) ? priceIndexReceiver() : null;
        final IMessageReceiver<Topology> planAnalysisTopologyReceiver =
                subscriptions.contains(Subscription.PlanAnalysisTopologies) ?
                        planAnalysisTopologyReceiver() : null;
        return new MarketComponentNotificationReceiver(projectedTopologyReceiver,
                actionPlansReceiver, priceIndexReceiver, planAnalysisTopologyReceiver,
                marketClientThreadPool());
    }

    public enum Subscription {
        ActionPlans, ProjectedTopologies, PriceIndexes, PlanAnalysisTopologies;
    }
}
