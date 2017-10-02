package com.vmturbo.history.component.api;

import javax.annotation.Nonnull;

public interface HistoryComponent extends AutoCloseable {

    void addStatsListener(@Nonnull final StatsListener listener);

}
