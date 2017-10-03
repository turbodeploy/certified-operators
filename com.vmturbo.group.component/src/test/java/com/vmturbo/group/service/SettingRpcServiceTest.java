package com.vmturbo.group.service;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.setting.SettingProto.AllSettingSpecRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SingleSettingSpecRequest;
import com.vmturbo.group.persistent.SettingStore;


@RunWith(MockitoJUnitRunner.class)
public class SettingRpcServiceTest {

    private final Logger logger = LogManager.getLogger();

    @Mock
    private SettingStore settingStore;

    private SettingRpcService settingRpcService;

    private String specName1 = "setting1";
    private String specName2 = "setting2";

    @Before
    public void setup() {
        settingRpcService = new SettingRpcService(settingStore);

        injectSettingSpecIntoStore(settingStore);
    }

    private void injectSettingSpecIntoStore(final SettingStore settingStore) {

        // create spec1
        SettingSpec injectedSettingSpec1 = SettingSpec.newBuilder()
            .setName(specName1)
                .build();

        // create spec2
        SettingSpec injectedSettingSpec2 = SettingSpec.newBuilder()
                .setName(specName2)
                .build();

        // inject them
        given(settingStore.getSettingSpec(specName1)).willReturn(Optional.of(injectedSettingSpec1));
        given(settingStore.getSettingSpec(specName2)).willReturn(Optional.of(injectedSettingSpec2));

        ImmutableSet<SettingSpec> specSet = ImmutableSet.of(
                injectedSettingSpec1,
                injectedSettingSpec2
                );

        given(settingStore.getAllSettingSpec()).willReturn(specSet);

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

        AllSettingSpecRequest settingSpecRequest = AllSettingSpecRequest.newBuilder().build();

        StreamObserver<SettingSpec> mockObserver =
                (StreamObserver<SettingSpec>) Mockito.mock(StreamObserver.class);

        settingRpcService.getAllSettingSpec(settingSpecRequest, mockObserver);

        verify(mockObserver, times(2)).onNext(any(SettingSpec.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());

    }

}
