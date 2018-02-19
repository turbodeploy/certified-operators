package com.vmturbo.reports.component;

import java.util.Set;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;

/**
 * Setting service for tests purpose.
 */
public class TestSetttingsService extends SettingServiceGrpc.SettingServiceImplBase {

    /**
     * Test implementation of getting settings.
     *
     * @param request with provided settings names
     * @param responseObserver to send response
     */
    @Override
    public void getMultipleGlobalSettings(SettingProto.GetMultipleGlobalSettingsRequest request,
                    StreamObserver<SettingProto.Setting> responseObserver) {
        responseObserver.onNext(SettingProto.Setting.newBuilder().setStringSettingValue(
                        SettingProto.StringSettingValue.newBuilder().setValue("TestValue").build()
        ).build());
    }
}
