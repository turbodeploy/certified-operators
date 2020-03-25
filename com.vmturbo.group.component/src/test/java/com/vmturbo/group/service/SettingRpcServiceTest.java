package com.vmturbo.group.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SingleSettingSpecRequest;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;

@RunWith(MockitoJUnitRunner.class)
public class SettingRpcServiceTest {

    private final Logger logger = LogManager.getLogger();

    @Mock
    private SettingSpecStore settingSpecStore;

    @Mock
    private SettingStore settingStore;

    private SettingRpcService settingRpcService;

    private String specName1 = "setting1";
    private String specName2 = "setting2";

    @Before
    public void setup() {
        settingRpcService =
            new SettingRpcService(settingSpecStore, settingStore);

        injectSettingSpecIntoStore(settingSpecStore);
    }

    private void injectSettingSpecIntoStore(final SettingSpecStore settingSpecStore) {

        // create spec1
        SettingSpec injectedSettingSpec1 = SettingSpec.newBuilder()
            .setName(specName1)
                .build();

        // create spec2
        SettingSpec injectedSettingSpec2 = SettingSpec.newBuilder()
                .setName(specName2)
                .build();

        // inject them
        given(settingSpecStore.getSettingSpec(specName1)).willReturn(Optional.of(injectedSettingSpec1));
        given(settingSpecStore.getSettingSpec(specName2)).willReturn(Optional.of(injectedSettingSpec2));

        ImmutableSet<SettingSpec> specSet = ImmutableSet.of(
                injectedSettingSpec1,
                injectedSettingSpec2
                );

        given(settingSpecStore.getAllSettingSpecs()).willReturn(specSet);

    }

    @Test
    public void testGetSettingSpec() {

        SingleSettingSpecRequest settingSpecRequest = SingleSettingSpecRequest.newBuilder()
                .setSettingSpecName(specName1).build();

        StreamObserver<SettingSpec> mockObserver =
                (StreamObserver<SettingSpec>) Mockito.mock(StreamObserver.class);

        settingRpcService.getSettingSpec(settingSpecRequest, mockObserver);

        verify(mockObserver).onNext(any(SettingSpec.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());

    }

    @Test
    public void testGetAllSettingSpec() {

        SearchSettingSpecsRequest settingSpecRequest =
                SearchSettingSpecsRequest.newBuilder().build();

        StreamObserver<SettingSpec> mockObserver =
                (StreamObserver<SettingSpec>) Mockito.mock(StreamObserver.class);

        settingRpcService.searchSettingSpecs(settingSpecRequest, mockObserver);

        verify(mockObserver, times(2)).onNext(any(SettingSpec.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testGetSomeSettingSpec() {
        SearchSettingSpecsRequest settingSpecRequest =
                SearchSettingSpecsRequest.newBuilder()
                        .addSettingSpecName(specName1)
                        .build();

        StreamObserver<SettingSpec> mockObserver =
                (StreamObserver<SettingSpec>) Mockito.mock(StreamObserver.class);

        settingRpcService.searchSettingSpecs(settingSpecRequest, mockObserver);

        ArgumentCaptor<SettingSpec> specCaptor = ArgumentCaptor.forClass(SettingSpec.class);
        verify(mockObserver).onNext(specCaptor.capture());
        verify(mockObserver).onCompleted();

        assertEquals(specName1, specCaptor.getValue().getName());
    }

}
