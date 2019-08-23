package com.vmturbo.components.common.utils;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Level;

import com.vmturbo.api.enums.LoggingLevel;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;

public class LoggingUtils {

    /**
     * Convert the log level from internal proto {@link LogLevel} to {@link Level} provided by log4j.
     *
     * @param logLevel the proto representation of log level
     * @return log level in the form of {@link Level}
     */
    public static Level protoLogLevelToLog4jLevel(@Nonnull LogLevel logLevel) {
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

    /**
     * Convert the log level from {@link Level} provided by log4j to internal proto {@link LogLevel}.
     *
     * @param log4jLevel the log4j representation of log level
     * @return log level in the form of {@link LogLevel}
     */
    public static LogLevel log4jLevelToProtoLogLevel(@Nonnull Level log4jLevel) {
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
