package com.vmturbo.api.component.external.api.mapper;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.LoggingLevel;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;

/**
 * Utility class to convert API logging level into log4j lgging level and wise versa.
 */
public class LoggingMapper {

    private LoggingMapper() {}

    /**
     * Convert the log level from {@link LoggingLevel} provided by api to internal proto {@link LogLevel}.
     * Currently we only supports 4 levels: WARN, INFO, DEBUG, TRACE.
     *
     * @param apiLogLevel the api representation of log level
     * @return proto log level in the form of {@link LogLevel}
     */
    public static LogLevel apiLogLevelToProtoLogLevel(@Nonnull LoggingLevel apiLogLevel) {
        switch (apiLogLevel) {
            case WARN:
                return LogLevel.WARN;
            case INFO:
                return LogLevel.INFO;
            case DEBUG:
                return LogLevel.DEBUG;
            case TRACE:
                return LogLevel.TRACE;
            default:
                throw new UnsupportedOperationException("Unsupported api logging level: " + apiLogLevel);
        }
    }

    /**
     * Convert the log level from internal proto {@link LogLevel} to api {@link LoggingLevel}.
     * Currently we only supports 4 levels: WARN, INFO, DEBUG, TRACE.
     *
     * @param protoLogLevel the proto representation of log level
     * @return api log level in the form of {@link LoggingLevel}
     */
    public static LoggingLevel protoLogLevelToApiLogLevel(@Nonnull LogLevel protoLogLevel) {
        switch (protoLogLevel) {
            case WARN:
                return LoggingLevel.WARN;
            case INFO:
                return LoggingLevel.INFO;
            case DEBUG:
                return LoggingLevel.DEBUG;
            case TRACE:
                return LoggingLevel.TRACE;
            default:
                throw new UnsupportedOperationException("Unsupported proto logging level: " + protoLogLevel);
        }
    }
}
