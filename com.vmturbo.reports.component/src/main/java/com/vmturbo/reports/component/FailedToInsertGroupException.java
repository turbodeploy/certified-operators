package com.vmturbo.reports.component;

import javax.annotation.Nonnull;

import org.jooq.exception.DataAccessException;

public class FailedToInsertGroupException extends RuntimeException {
    public FailedToInsertGroupException(@Nonnull final Throwable cause) {
        super("Failed to insert group:", cause);
    }
}


