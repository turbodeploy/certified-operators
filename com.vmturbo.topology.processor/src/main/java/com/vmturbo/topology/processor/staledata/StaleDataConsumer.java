package com.vmturbo.topology.processor.staledata;

import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;

/**
 * Interface for consumers of stale data reports.
 */
public interface StaleDataConsumer extends Consumer<ImmutableMap<Long, TargetHealth>> {}
