package com.vmturbo.api.component.external.api.util.stats;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;

public class StatsTestUtil {

    @Nonnull
    public static StatApiDTO stat(final String name) {
        final StatApiDTO apiDTO = new StatApiDTO();
        apiDTO.setName(name);
        return apiDTO;
    }

    @Nonnull
    public static StatApiInputDTO statInput(final String name) {
        StatApiInputDTO apiInputDTO = new StatApiInputDTO();
        apiInputDTO.setName(name);
        return apiInputDTO;
    }
}
