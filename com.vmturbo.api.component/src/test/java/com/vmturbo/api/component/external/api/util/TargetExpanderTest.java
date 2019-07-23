package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

@RunWith(MockitoJUnitRunner.class)
public class TargetExpanderTest {

    private static final String ORDINARY_TARGET_ID_STRING = "123456";
    private static final long ORDINARY_TARGET_ID = Long.valueOf(ORDINARY_TARGET_ID_STRING);
    private static final String MARKET_ID_STRING = "Market";

    @Mock
    TopologyProcessor topologyProcessorClient;

    @Mock
    RepositoryApi repositoryApi;

    @Mock
    TargetInfo targetInfo;

    @InjectMocks
    private TargetExpander targetExpander;

    @Test
    public void getOrdinaryTargetOid_expectTargetInfo() throws CommunicationException,
        TopologyProcessorException {
        // arrange
        when(topologyProcessorClient.getTarget(eq(ORDINARY_TARGET_ID))).thenReturn(targetInfo);
        // act
        Optional<TargetInfo> result = targetExpander.getTarget(ORDINARY_TARGET_ID_STRING);
        // assert
        assertTrue(result.isPresent());
        assertThat(result.get(), equalTo(targetInfo));
        verify(topologyProcessorClient).getTarget(ORDINARY_TARGET_ID);
        verifyNoMoreInteractions(topologyProcessorClient);
        verifyZeroInteractions(repositoryApi);
    }

    @Test
    public void getMissingTargetOid_expectOptionalEmpty() throws CommunicationException,
        TopologyProcessorException {
        // arrange
        when(topologyProcessorClient.getTarget(eq(ORDINARY_TARGET_ID))).thenThrow(
            new TopologyProcessorException("not found"));
        // act
        Optional<TargetInfo> result = targetExpander.getTarget(ORDINARY_TARGET_ID_STRING);
        // assert
        assertFalse(result.isPresent());
        verify(topologyProcessorClient).getTarget(ORDINARY_TARGET_ID);
        verifyNoMoreInteractions(topologyProcessorClient);
        verifyZeroInteractions(repositoryApi);
    }

    @Test
    public void getMarketOid_expectOptionalEmpty() throws CommunicationException {
        // arrange
        // act
        Optional<TargetInfo> result = targetExpander.getTarget(MARKET_ID_STRING);
        // assert
        assertFalse(result.isPresent());
        verifyZeroInteractions(topologyProcessorClient);
        verifyZeroInteractions(repositoryApi);
    }

    @Test
    public void getTargetEntityIds_expectSet() {
        // arrange
        final SearchParameters expectedSearchParameters = SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.discoveredBy(targetInfo.getId()))
            .build();
        final SearchRequest mockSearchRequest = Mockito.mock(SearchRequest.class);
        when(repositoryApi.newSearchRequest(eq(expectedSearchParameters)))
            .thenReturn(mockSearchRequest);
        final Set<Long> expectedOIDSet = Sets.newSet(1L, 2L, 3L);
        when(mockSearchRequest.getOids()).thenReturn(expectedOIDSet);
        // act
        final Set<Long> result = targetExpander.getTargetEntityIds(targetInfo);
        // assert
        assertThat(result.size(), equalTo(expectedOIDSet.size()));
        assertThat(result, equalTo(expectedOIDSet));
    }
}