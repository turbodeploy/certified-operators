package com.vmturbo.trax.rpc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.trax.Trax.ClearAllConfigurationsRequest;
import com.vmturbo.common.protobuf.trax.Trax.ClearAllConfigurationsResponse;
import com.vmturbo.common.protobuf.trax.Trax.ClearConfigurationsRequest;
import com.vmturbo.common.protobuf.trax.Trax.ClearConfigurationsResponse;
import com.vmturbo.common.protobuf.trax.Trax.TraxConfigurationItem;
import com.vmturbo.common.protobuf.trax.Trax.TraxConfigurationListingRequest;
import com.vmturbo.common.protobuf.trax.Trax.TraxConfigurationRequest;
import com.vmturbo.common.protobuf.trax.TraxConfigurationServiceGrpc.TraxConfigurationServiceImplBase;
import com.vmturbo.trax.TraxConfiguration;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;

/**
 * Expose {@link TraxConfiguration} settings via an RPC service.
 */
public class TraxConfigurationRpcService extends TraxConfigurationServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfiguration(TraxConfigurationRequest request,
                                 StreamObserver<TraxConfigurationItem> responseObserver) {
        try {
            logger.info("Configuring Trax topics: {}", request);
            final TopicSettings settings = TraxConfiguration.configureTopics(request);
            responseObserver.onNext(toConfigurationItem(settings));
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Exception while attempting to apply Trax configuration " + request, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to configure topic. " + e.getMessage())
                .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void listConfigurations(TraxConfigurationListingRequest request,
                                   StreamObserver<TraxConfigurationItem> responseObserver) {
        final Map<TopicSettings, List<String>> settingsToTopics = new HashMap<>();

        TraxConfiguration.getTrackingTopics().forEach((topicName, settings) -> {
            final List<String> topicsForSettings = settingsToTopics.computeIfAbsent(
                settings, key -> new ArrayList<>());
            topicsForSettings.add(topicName);
        });

        settingsToTopics.forEach((setting, topics) ->
            responseObserver.onNext(toConfigurationItem(topics, setting)));
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearConfigurations(ClearConfigurationsRequest request,
                                    StreamObserver<ClearConfigurationsResponse> responseObserver) {
        final ClearConfigurationsResponse.Builder builder = ClearConfigurationsResponse.newBuilder();
        request.getTopicNamesList().forEach(topicName -> {
            if (TraxConfiguration.clearConfiguration(topicName) != null) {
                builder.addClearedTopicNames(topicName);
            }
        });

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearAllConfigurations(ClearAllConfigurationsRequest request,
                                       StreamObserver<ClearAllConfigurationsResponse> responseObserver) {
        responseObserver.onNext(ClearAllConfigurationsResponse.newBuilder()
            .setConfigurationClearCount(TraxConfiguration.clearAllConfiguration())
            .build());
        responseObserver.onCompleted();
    }

    private static TraxConfigurationItem toConfigurationItem(@Nonnull final TopicSettings settings) {
        return toConfigurationItem(settings.getTopicNames(), settings);
    }

    private static TraxConfigurationItem toConfigurationItem(@Nonnull final Collection<String> topicNames,
                                                             @Nonnull final TopicSettings settings) {
        return TraxConfigurationItem.newBuilder()
            .setTopicConfiguration(settings.getTopicConfiguration().toBuilder()
                .build())
            .setLimitRemainder(settings.getLimit().toProto())
            .addAllTopicNames(topicNames)
            .build();
    }
}