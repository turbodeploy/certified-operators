package com.vmturbo.components.common.utils;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Level;

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

}
