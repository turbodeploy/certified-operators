package com.vmturbo.history.component.api;

import javax.annotation.Nonnull;

public interface HistoryComponent {

    void addStatsListener(@Nonnull final StatsListener listener);

}
