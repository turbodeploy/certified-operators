package com.vmturbo.action.orchestrator.api.impl;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentRestClient;

/**
 * The client that calls the Action Orchestrator's REST methods
 * and interprets the responses.
 *
 * Does nothing because gRPC is here now.
 */
class ActionOrchestratorRestClient extends ComponentRestClient {

    ActionOrchestratorRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }
}
