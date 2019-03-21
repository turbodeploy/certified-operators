package com.vmturbo.components.common.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.logging.LogConfigurationServiceGrpc.LogConfigurationServiceImplBase;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsResponse;

/**
 * Service that allows changing of log levels
 */
public class LogConfigurationService extends LogConfigurationServiceImplBase {
    private static final Logger logger = LogManager.getLogger();

    private Level protoLogLevelToLog4jLevel(LogLevel logLevel) {
        switch(logLevel.getNumber()) {
            case LogLevel.DEBUG_VALUE:
                return Level.DEBUG;
            case LogLevel.ERROR_VALUE:
                return Level.ERROR;
            case LogLevel.FATAL_VALUE:
                return Level.FATAL;
            case LogLevel.TRACE_VALUE:
                return Level.TRACE;
            case LogLevel.WARN_VALUE:
                return Level.WARN;
            case LogLevel.INFO_VALUE:
            default:
                return Level.INFO;
        }
    }

    private LogLevel log4jLevelToProtoLogLevel(Level log4jLevel) {
        if (log4jLevel == Level.TRACE) {
            return LogLevel.TRACE;
        } else if (log4jLevel == Level.DEBUG) {
            return LogLevel.DEBUG;
        } else if (log4jLevel == Level.INFO) {
            return LogLevel.INFO;
        } else if (log4jLevel == Level.WARN) {
            return LogLevel.WARN;
        } else if (log4jLevel == Level.ERROR) {
            return LogLevel.ERROR;
        } else if (log4jLevel == Level.FATAL) {
            return LogLevel.FATAL;
        }
        return LogLevel.UNKNOWN;
    }

    @Override
    public void setLogLevels(final SetLogLevelsRequest request, final StreamObserver<SetLogLevelsResponse> responseObserver) {
        // update the log levels for the specified pacakages.
        logger.info("Setting log levels: {}", request.getLogLevelsMap());
        if (request.getLogLevelsCount() > 0) {
            Map<String, Level> logLevels = new HashMap<>(request.getLogLevelsCount());
            for (Entry<String, LogLevel> entry : request.getLogLevelsMap().entrySet()) {
                Level newLevel = protoLogLevelToLog4jLevel(entry.getValue());
                logLevels.put(entry.getKey(), newLevel);
            }
            Configurator.setLevel(logLevels);
        }
        responseObserver.onNext(SetLogLevelsResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getLogLevels(final GetLogLevelsRequest request, final StreamObserver<GetLogLevelsResponse> responseObserver) {
        LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
        Map<String, LoggerConfig> map = logContext.getConfiguration().getLoggers();
        GetLogLevelsResponse.Builder responseBuilder = GetLogLevelsResponse.newBuilder();
        // build a response map
        map.entrySet().forEach(entry -> {
            responseBuilder.putLogLevels(entry.getKey(), log4jLevelToProtoLogLevel(entry.getValue().getLevel()));
        });
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
