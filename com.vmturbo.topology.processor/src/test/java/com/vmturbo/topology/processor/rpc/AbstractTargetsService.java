package com.vmturbo.topology.processor.rpc;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test class represents target and probe stores.
 */
public class AbstractTargetsService {

    private static final AtomicLong COUNTER = new AtomicLong(100);
    private static final String ID = "target-address";

    private Map<Long, ProbeInfo> probes;
    private Map<Long, Target> targets;

    private TargetStore targetStore;
    private ProbeStore probeStore;
    private GrpcTestServer grpcServer;

    /**
     * Set up test configuration.
     *
     * @throws IOException shouldn't happen
     */
    @Before
    public void init() throws IOException {
        targetStore = Mockito.mock(TargetStore.class);
        probeStore = Mockito.mock(ProbeStore.class);
        probes = new HashMap<>();
        targets = new HashMap<>();

        Mockito.when(targetStore.getTarget(Mockito.anyLong())).thenAnswer(answerById(targets));
        Mockito.when(targetStore.getAll())
                .thenAnswer(invocation -> new ArrayList<>(targets.values()));
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenAnswer(answerById(probes));
        Mockito.when(probeStore.getProbes()).thenReturn(probes);
    }

    @Nonnull
    private <T> Answer<Optional<T>> answerById(@Nonnull final Map<Long, T> map) {
        return invocation -> {
            final long id = (long)invocation.getArguments()[0];
            return Optional.ofNullable(map.get(id));
        };
    }

    /**
     * Creates target.
     *
     * @param probeId the probe id
     * @return the target id
     * @throws InvalidTargetException if failed to create the target
     */
    protected long createTarget(long probeId) throws InvalidTargetException {
        final long targetId = COUNTER.getAndIncrement();
        final TargetSpec targetSpec = TargetSpec.newBuilder()
                .setProbeId(probeId)
                .addAccountValue(
                        AccountValue.newBuilder().setKey(ID).setStringValue("tgt-" + targetId))
                .build();
        final Target target = new Target(targetId, probeStore, targetSpec, false, true,
                Clock.systemUTC());
        targets.put(targetId, target);
        return targetId;
    }

    /**
     * Return target store.
     *
     * @return target store
     */
    protected TargetStore getTargetStore() {
        return targetStore;
    }

    /**
     * Return probe store.
     *
     * @return probe store
     */
    protected ProbeStore getProbeStore() {
        return probeStore;
    }
}
