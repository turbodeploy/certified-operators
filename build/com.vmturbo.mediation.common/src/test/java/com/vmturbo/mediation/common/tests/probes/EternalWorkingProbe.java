package com.vmturbo.mediation.common.tests.probes;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;

/**
 * Probe that works eternally. It is guaranteed, that probe will not return any value in any amount
 * of time.
 */
public class EternalWorkingProbe extends FakeExternalProbe {

    private void waitEternally() throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
        }
    }

    @Override
    protected DiscoveryResponse discoverTarget() throws InterruptedException {
        waitEternally();
        return null;
    }

    @Override
    protected ValidationResponse validateTarget() throws InterruptedException {
        waitEternally();
        return null;
    }
}
