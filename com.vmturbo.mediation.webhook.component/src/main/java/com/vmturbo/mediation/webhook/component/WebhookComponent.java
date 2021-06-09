package com.vmturbo.mediation.webhook.component;

import static com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic.Notifications;
import static com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.forTopic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.ExecutionStatus;
import com.vmturbo.mediation.client.MediationComponentConfig;
import com.vmturbo.mediation.client.MediationComponentMain;
import com.vmturbo.mediation.common.WorkerLifecycleListener;
import com.vmturbo.mediation.diagnostic.MediationDiagnosticsConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * The component for the Webhook probe.
 */
@Component
@Configuration("theComponent")
@Import({MediationComponentConfig.class,
        MediationDiagnosticsConfig.class, TopologyProcessorClientConfig.class})
public class WebhookComponent extends MediationComponentMain {
    private static final Logger LOGGER = LogManager.getLogger();

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    /**
     * Topology Processor API interface.
     *
     * @return an interface instance.
     */
    @Bean
    public TopologyProcessor topologyProcessor() {
        return tpClientConfig.topologyProcessor(forTopic(Notifications));
    }

    private void checkTargetRegistration() {
        try {
            new TargetRegistration(topologyProcessor()).checkTargetRegistration();
        } catch (CommunicationException | TopologyProcessorException e) {
            LOGGER.error("Webhook Target registration error:", e);
        }
    }

    /**
     * The handled of component`s lifecycle stages.
     *
     * @return an instance of listener.
     */
    @Bean
    public WorkerLifecycleListener lifecycleListener() {
        return new WebhookWorkerLifecycleListener();
    }

    /**
     * Listener class for the component's worker.
     */
    private class WebhookWorkerLifecycleListener implements WorkerLifecycleListener {
        @Override
        public void onStopping() {
            setStatus(ExecutionStatus.STOPPING);
        }

        @Override
        public void onStopped() {
            setStatus(ExecutionStatus.TERMINATED);
        }

        @Override
        public void onRunning() {
            setStatus(ExecutionStatus.RUNNING);
            checkTargetRegistration();
        }

        @Override
        public void onStarting() {
            setStatus(ExecutionStatus.STARTING);
        }
    }

    /**
     * The component entry point.
     *
     * @param args Program arguments
     */
    public static void main(String[] args) {
        startContext(WebhookComponent.class);
    }
}
