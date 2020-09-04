package com.vmturbo.action.orchestrator.store;

import java.util.Optional;

import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;

/**
 * Interface to build Atomic action DTOs from {@link AggregatedAction}.
 *
 * <p>The atomic action DTO for an aggregated action for this set of entities is depicted below.
 * Example for WorkloadController W1 that
 * --> owns -> ContainerSpec CS1 --> aggregates --> Containers C1, C2, C3
 * and also
 * --> owns -> ContainerSpec CS2 --> aggregates --> Containers C4, C5, C6
 * and also
 * ---> aggregates ---> Containers C10, C11 (without container spec)
 * If there are duplicated resize actions {A1::VCPU, A2::VMEM} on C1, {A3::VCPU,A4::VMEM} on C2
 * and duplicated A7::VCPU, A8::VCPU on C4 and C5 respectively
 * and also resize action A10::VMEM, A11::VMEM on C10, C11,
 * then the ActionDTO for atomic resizes is as follows :
 *
 * ActionDTO::Info {
 *      AtomicResize: {
 *              Execution Target Entity : WorkloadController W1
 *              ResizeInfo: [
 *                              {
 *                                  TargetEntity ContainerSpec CS1
 *                                  Commodity: VCPU
 *                                  [resize details...]
 *                              },
 *                              {
 *                                  TargetEntity ContainerSpec CS2
 *                                  Commodity: VMEM
 *                                  [resize details...]
 *                              },
 *                              {
 *                                  TargetEntity ContainerSpec CS2
 *                                  Commodity: VCPU
 *                                  [resize details...]
 *                              },
 *                              {
 *                                  TargetEntity Container C10
 *                                  Commodity: VMEM
 *                                  [resize details...]
 *                              },
 *                              {
 *                                  TargetEntity Container C11
 *                                  Commodity: VMEM
 *                                  [resize details...]
 *                              }
 *                          ]
 *               }
 *      }
 * </p>
 */
public interface AtomicActionBuilder {

    /**
     * Build Atomic action DTOs from a given {@link AggregatedAction}.
     *
     * @return  a {@link AtomicActionResult} containing a new primary {@link Action} that will
     *          atomically execute all the aggregated actions.
     *          The atomic action result also consists of a map of non-executable {@link Action}s
     *          for the de-duplicated actions that were merged in the primary atomic action.
     *
     */
    Optional<AtomicActionResult> build();
}
