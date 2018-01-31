package com.vmturbo.mediation.client;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import com.vmturbo.mediation.common.ProbeProperties;
import com.vmturbo.mediation.common.RemoteComposer;
import com.vmturbo.mediation.common.WorkerLifecycleListener;

/**
 * Wrapper for all Mediation Probe Components - instances of IProbe.
 **/
public class MediationWorker extends RemoteComposer {

    public MediationWorker(MediationComponentConfig componentConfig,
            ProbeProperties<?> probeProperties, WorkerLifecycleListener theComponent,
            ExecutorService threadPool, long negotiationTimeoutSec, long keepAliveIntervalSec, long chunkSendDelayMs) {
        super(Collections.singleton(probeProperties), componentConfig, theComponent, threadPool,
                negotiationTimeoutSec, keepAliveIntervalSec, chunkSendDelayMs);
    }
}
