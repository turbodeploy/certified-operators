package com.vmturbo.cost.component.savings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;

/**
 * Gets action chains from AO.
 */
public class GrpcActionChainStore implements ActionChainStore {
    @Override
    @Nonnull
    public Map<Long, NavigableSet<ActionSpec>> getActionChains(@Nonnull List<Long> entityIds) {
        // TODO: hookup
        return Collections.emptyMap();
    }
}
