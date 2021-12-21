package com.vmturbo.action.orchestrator;

import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Workaround for {@link ActionOrchestratorDbEndpointConfig} (remove conditional annotation), since
 * it's conditionally initialized based on {@link FeatureFlags#POSTGRES_PRIMARY_DB}. When we
 * test all combinations of it using {@link FeatureFlagTestRule}, first it's false, so
 * {@link ActionOrchestratorDbEndpointConfig} is not created; then second it's true,
 * {@link ActionOrchestratorDbEndpointConfig} is created, but the endpoint inside is also eagerly
 * initialized due to the same FF, which results in several issues like: it doesn't go through
 * DbEndpointTestRule, making call to auth to get root password, etc.
 */
@Configuration
public class TestActionOrchestratorDbEndpointConfig extends ActionOrchestratorDbEndpointConfig {}
