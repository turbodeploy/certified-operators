package com.vmturbo.components.common.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

import com.vmturbo.common.protobuf.logging.LogConfigurationServiceGrpc.LogConfigurationServiceImplBase;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsResponse;
import com.vmturbo.components.common.utils.LoggingUtils;

/**
 * Service that allows changing of log levels.
 */
public class LogConfigurationService extends LogConfigurationServiceImplBase {
    private static final Logger logger = LogManager.getLogger();

    /**
     * This is the package whose logging level is changed by the /admin/logginglevels API
     * method.
     */
    public static final String TURBO_PACKAGE_NAME = "com.vmturbo";

    @Override
    public void setLogLevels(final SetLogLevelsRequest request, final StreamObserver<SetLogLevelsResponse> responseObserver) {
        // update the log levels for the specified packages.
        logger.info("Setting log levels: {}", request.getLogLevelsMap());
        if (request.getLogLevelsCount() > 0) {
            Map<String, Level> logLevels = new HashMap<>(request.getLogLevelsCount());
            for (Entry<String, LogLevel> entry : request.getLogLevelsMap().entrySet()) {
                Level newLevel = LoggingUtils.protoLogLevelToLog4jLevel(entry.getValue());
                logLevels.put(entry.getKey(), newLevel);
            }
            Configurator.setLevel(logLevels);
        }
        responseObserver.onNext(SetLogLevelsResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getLogLevels(final GetLogLevelsRequest request,
            final StreamObserver<GetLogLevelsResponse> responseObserver) {
        LoggerContext logContext = (LoggerContext)LogManager.getContext(false);
        Map<String, LoggerConfig> map = logContext.getConfiguration().getLoggers();
        GetLogLevelsResponse.Builder responseBuilder = GetLogLevelsResponse.newBuilder();
        // build a response map
        map.entrySet().forEach(entry -> {
            responseBuilder.putLogLevels(entry.getKey(),
                LoggingUtils.log4jLevelToProtoLogLevel(entry.getValue().getLevel()));
        });
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
