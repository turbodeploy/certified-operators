package com.vmturbo.plan.orchestrator.reservation;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.plan.orchestrator.DbAccessConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Spring Configuration for Reservation services.
 */
@Configuration
@Import({DbAccessConfig.class, TemplatesConfig.class, BaseKafkaProducerConfig.class,
        MarketClientConfig.class})
public class ReservationConfig {

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private DbAccessConfig dbConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Value("${prepareReservationCache:true}")
    private boolean prepareReservationCache;

    @Value("${delayedDeletionTimeInMillis:172800000}")
    private long delayedDeletionTimeInMillis;

    @Value("${numReservationDeletionCleanupThreads:1}")
    private int numReservationDeletionCleanupThreads;

    @Value("${enableOP:false}")
    private boolean enableOP;

    @Bean
    public ReservationDao reservationDao() {
        try {
            return new ReservationDaoImpl(dbConfig.dsl());

        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ReservationDao", e);
        }
    }

    @Bean
    public ReservationRpcService reservationRpcService() {
        return new ReservationRpcService(templatesConfig.templatesDao(),
                reservationDao(), reservationManager(),
                delayedDeletionTimeInMillis);
    }

    /**
     * Create a {@link ReservationNotificationSender}.
     *
     * @return the reservation notification sender.
     */
    @Bean
    public ReservationNotificationSender reservationNotificationSender() {
        return new ReservationNotificationSender(kafkaProducerConfig.kafkaMessageSender()
                .messageSender(PlanOrchestratorClientImpl.RESERVATION_NOTIFICATION_TOPIC));
    }

    /**
     * Create a {@link InitialPlacementServiceBlockingStub}.
     *
     * @return the InitialPlacementServiceBlockingStub.
     */
    @Bean
    public InitialPlacementServiceBlockingStub initialPlacementService() {
        return InitialPlacementServiceGrpc.newBlockingStub(marketClientConfig.marketChannel());
    }

    /**
     * create a ReservationManager.
     *
     * @return a new ReservationManager
     */
    @Bean
    public ReservationManager reservationManager() {
        return new ReservationManager(reservationDao(), reservationNotificationSender(),
                initialPlacementService(), templatesConfig.templatesDao(), planConfig.planDao(),
                planConfig.planService(), prepareReservationCache,
                planConfig.groupServiceBlockingStub(), startReservationDeletionCleanupThreadPool(), enableOP);
    }

    /**
     * create a ExecutorService.
     *
     * @return a new ExecutorService
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService startReservationDeletionCleanupThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("reservation-deletion-cleanup-worker-%d")
                .build();
        return Executors.newFixedThreadPool(Math.max(1, numReservationDeletionCleanupThreads), threadFactory);
    }
}