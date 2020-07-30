package com.vmturbo.topology.processor.api.util;


import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class ThinTargetCacheTest {
    private final static long TARGET_ID = 10L;
    private final static long PROBE_ID = 11L;
    private final static String TARGET_DISPLAY_NAME = "display name";
    private final static String PROBE_TYPE = "probe type";
    private final static String PROBE_CATEGORY = "probe category";
    private final static String PROBE_UI_CATEGORY = "probe ui category";

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    private ThinTargetCache thinTargetCache = new ThinTargetCache(topologyProcessor);

    @Test
    public void testDiscoveryCacheLoadAndCacheTarget() throws Exception {
        mockTopologyProcessorOutputs();

        Optional<ThinTargetInfo> dto = thinTargetCache.getTargetInfo(TARGET_ID);
        checkTargetApiDTO(dto.get());

        verify(topologyProcessor).getTarget(TARGET_ID);

        // THe second time should return the same thing, but not make a subsequent call.
        Optional<ThinTargetInfo> dto2 = thinTargetCache.getTargetInfo(TARGET_ID);
        checkTargetApiDTO(dto2.get());
        verify(topologyProcessor, times(1)).getTarget(TARGET_ID);
    }

    @Test
    public void testDiscoveryCacheClearOnTargetUpdate() throws Exception {
        mockTopologyProcessorOutputs();

        Optional<ThinTargetInfo> dto = thinTargetCache.getTargetInfo(TARGET_ID);
        checkTargetApiDTO(dto.get());

        verify(topologyProcessor).getTarget(TARGET_ID);

        TargetInfo targetInfo = mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(TARGET_ID);
        when(targetInfo.getProbeId()).thenReturn(PROBE_ID);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);

        thinTargetCache.onTargetChanged(targetInfo);

        Optional<ThinTargetInfo> dto2 = thinTargetCache.getTargetInfo(TARGET_ID);
        checkTargetApiDTO(dto2.get());
        // Should not have made a subsequent call - we use the target info passed via the
        // notification.
        verify(topologyProcessor, times(1)).getTarget(TARGET_ID);
    }

    @Test
    public void testOnTargetAdded() throws Exception {
        mockTopologyProcessorOutputs();

        TargetInfo targetInfo = mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(TARGET_ID);
        when(targetInfo.getProbeId()).thenReturn(PROBE_ID);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);

        thinTargetCache.onTargetAdded(targetInfo);

        verify(topologyProcessor).getProbe(PROBE_ID);

        final ThinTargetInfo dto = thinTargetCache.getTargetInfo(TARGET_ID).get();

        // Shouldn't have made a call, since we got the target info as part of the notification.
        verify(topologyProcessor, times(0)).getTarget(TARGET_ID);

        checkTargetApiDTO(dto);
    }

    @Test
    public void testDiscoveryCacheClearOnTargetRemove() throws Exception {
        mockTopologyProcessorOutputs();

        Optional<ThinTargetInfo> dto = thinTargetCache.getTargetInfo(TARGET_ID);
        checkTargetApiDTO(dto.get());

        verify(topologyProcessor).getTarget(TARGET_ID);

        thinTargetCache.onTargetRemoved(TARGET_ID);

        Optional<ThinTargetInfo> dto2 = thinTargetCache.getTargetInfo(TARGET_ID);
        checkTargetApiDTO(dto2.get());
        // Should have made a subsequent call.
        verify(topologyProcessor, times(2)).getTarget(TARGET_ID);
    }

    @Test
    public void testGetAllTargetsInitialFailure() throws Exception {
        final TargetInfo targetInfo = mock(TargetInfo.class);
        final ProbeInfo probeInfo = mock(ProbeInfo.class);
        when(targetInfo.getId()).thenReturn(TARGET_ID);
        when(targetInfo.getProbeId()).thenReturn(PROBE_ID);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);
        when(probeInfo.getId()).thenReturn(PROBE_ID);
        when(probeInfo.getType()).thenReturn(PROBE_TYPE);
        when(probeInfo.getCategory()).thenReturn(PROBE_CATEGORY);
        when(probeInfo.getUICategory()).thenReturn(PROBE_UI_CATEGORY);
        when(topologyProcessor.getProbe(Mockito.eq(PROBE_ID))).thenReturn(probeInfo);
        when(topologyProcessor.getAllProbes()).thenReturn(Collections.singleton(probeInfo));
        when(topologyProcessor.getAllTargets())
            .thenThrow(new CommunicationException("foo"))
            .thenReturn(Collections.singleton(targetInfo));
        when(topologyProcessor.getTarget(Mockito.eq(TARGET_ID))).thenReturn(targetInfo);

        List<ThinTargetInfo> info = thinTargetCache.getAllTargets();
        MatcherAssert.assertThat(info.size(), is(0));
        verify(topologyProcessor).getAllTargets();

        List<ThinTargetInfo> info2 = thinTargetCache.getAllTargets();
        MatcherAssert.assertThat(info2.size(), is(1));
        verify(topologyProcessor, times(2)).getAllTargets();
    }

    @Test
    public void testGetAllTargets() throws Exception {
        mockTopologyProcessorOutputs();

        List<ThinTargetInfo> info = thinTargetCache.getAllTargets();

        MatcherAssert.assertThat(info.size(), is(1));
        checkTargetApiDTO(info.get(0));
        verify(topologyProcessor, times(1)).getAllProbes();
        verify(topologyProcessor, times(1)).getAllTargets();

        // Subsequent call shouldn't make another RPC.
        List<ThinTargetInfo> info2 = thinTargetCache.getAllTargets();
        MatcherAssert.assertThat(info2, is(info));
        // Still just one call.
        verify(topologyProcessor, times(1)).getAllProbes();
        verify(topologyProcessor, times(1)).getAllTargets();

        // Subsequent call for a specific target shouldn't make an RPC.
        ThinTargetInfo target = thinTargetCache.getTargetInfo(TARGET_ID).get();
        verify(topologyProcessor, never()).getTarget(anyLong());
        verify(topologyProcessor, never()).getProbe(anyLong());

        checkTargetApiDTO(target);
    }

    private void mockTopologyProcessorOutputs() throws Exception {
        final TargetInfo targetInfo = mock(TargetInfo.class);
        final ProbeInfo probeInfo = mock(ProbeInfo.class);
        when(targetInfo.getId()).thenReturn(TARGET_ID);
        when(targetInfo.getProbeId()).thenReturn(PROBE_ID);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);
        when(probeInfo.getId()).thenReturn(PROBE_ID);
        when(probeInfo.getType()).thenReturn(PROBE_TYPE);
        when(probeInfo.getCategory()).thenReturn(PROBE_CATEGORY);
        when(probeInfo.getUICategory()).thenReturn(PROBE_UI_CATEGORY);
        when(topologyProcessor.getProbe(Mockito.eq(PROBE_ID))).thenReturn(probeInfo);
        when(topologyProcessor.getAllProbes()).thenReturn(Collections.singleton(probeInfo));
        doReturn(Collections.singleton(targetInfo)).when(topologyProcessor).getAllTargets();
        when(topologyProcessor.getTarget(Mockito.eq(TARGET_ID))).thenReturn(targetInfo);
    }

    private void checkTargetApiDTO(ThinTargetInfo targetApiDTO) {
        Assert.assertNotNull(targetApiDTO);
        Assert.assertEquals(TARGET_ID, targetApiDTO.oid());
        Assert.assertEquals(TARGET_DISPLAY_NAME, targetApiDTO.displayName());
        Assert.assertEquals(PROBE_TYPE, targetApiDTO.probeInfo().type());
        Assert.assertEquals(PROBE_CATEGORY, targetApiDTO.probeInfo().category());
    }

}
