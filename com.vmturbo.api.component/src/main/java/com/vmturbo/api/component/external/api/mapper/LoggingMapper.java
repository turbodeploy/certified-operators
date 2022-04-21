package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.enums.LoggingLevel;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;

/**
 * Utility class to convert API logging level into log4j lgging level and wise versa.
 */
public class LoggingMapper {

    private LoggingMapper() {}

    private static final Logger logger = LogManager.getLogger();

    /**
     * Convert the log level from {@link LoggingLevel} provided by api to internal proto {@link LogLevel}.
     * Currently we only supports 6 levels: FATAL, ERROR, WARN, INFO, DEBUG, TRACE.
     *
     * @param apiLogLevel the api representation of log level
     * @return proto log level in the form of {@link LogLevel}
     */
    public static LogLevel apiLogLevelToProtoLogLevel(@Nonnull LoggingLevel apiLogLevel) {
        switch (apiLogLevel) {
            case FATAL:
                return LogLevel.FATAL;
            case ERROR:
                return LogLevel.ERROR;
            case WARN:
                return LogLevel.WARN;
            case INFO:
                return LogLevel.INFO;
            case DEBUG:
                return LogLevel.DEBUG;
            case TRACE:
                return LogLevel.TRACE;
            case UNKNOWN:
                return LogLevel.UNKNOWN;
            default:
                logger.warn("Unrecognized api logging level: " + apiLogLevel);
                return LogLevel.UNKNOWN;
        }
    }

    /**
     * Convert the log level from internal proto {@link LogLevel} to api {@link LoggingLevel}.
     * Currently we only supports 6 levels: FATAL, ERROR, WARN, INFO, DEBUG, TRACE.
     *
     * @param protoLogLevel the proto representation of log level
     * @return api log level in the form of {@link LoggingLevel}
     */
    public static LoggingLevel protoLogLevelToApiLogLevel(@Nonnull LogLevel protoLogLevel) {
        switch (protoLogLevel) {
            case FATAL:
                return LoggingLevel.FATAL;
            case ERROR:
                return LoggingLevel.ERROR;
            case WARN:
                return LoggingLevel.WARN;
            case INFO:
                return LoggingLevel.INFO;
            case DEBUG:
                return LoggingLevel.DEBUG;
            case TRACE:
                return LoggingLevel.TRACE;
            case UNKNOWN:
                return LoggingLevel.UNKNOWN;
            default:
                logger.warn("Unrecognized proto logging level: " + protoLogLevel);
                return LoggingLevel.UNKNOWN;
        }
    }
}
