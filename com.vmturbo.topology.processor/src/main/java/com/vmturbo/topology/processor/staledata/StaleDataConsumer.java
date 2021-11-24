package com.vmturbo.topology.processor.staledata;

import java.util.Map;
import java.util.function.Consumer;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;

/**
 * Interface for consumers of stale data reports.
 */
public interface StaleDataConsumer extends Consumer<Map<Long, TargetHealth>> {}
