package com.vmturbo.api.component.external.api.service;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.WidgetsetMapper;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IWidgetSetsService;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.CreateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.DeleteWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.GetWidgetsetListRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.GetWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.UpdateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc.WidgetsetsServiceBlockingStub;

/**
 * Service implementation of Widget Sets, which calls the grpc service WidgetsetsRpcService in
 * in the AuthComponent.
 **/
public class WidgetSetsService implements IWidgetSetsService {

    // the GRPC service stub to call for Widgetset services
    private final WidgetsetsServiceBlockingStub widgetsetsService;

    private final Logger logger = LogManager.getLogger();

    public WidgetSetsService(WidgetsetsServiceBlockingStub widgetsetsService) {
        this.widgetsetsService = widgetsetsService;
    }

    @Override
    public List<WidgetsetApiDTO> getWidgetsetList(
            @Nullable Set<String> categories,
            @Nullable String scopeType) {
        final GetWidgetsetListRequest.Builder widgetsetListRequest = GetWidgetsetListRequest
                .newBuilder();
        if (categories != null && categories.size() > 0) {
            widgetsetListRequest.addAllCategories(categories);
        }
        if (scopeType != null) {
            widgetsetListRequest.setScopeType(scopeType);
        }
        final List<WidgetsetApiDTO> answer = Lists.newLinkedList();
        widgetsetsService.getWidgetsetList(widgetsetListRequest.build())
                .forEachRemaining(widgetsetRecord ->
                        answer.add(WidgetsetMapper.toUiWidgetset(widgetsetRecord)));
        return answer;
    }

    @Override
    public WidgetsetApiDTO getWidgetset(String uuid) throws Exception {
        final long widgetsetOid;
        try {
            widgetsetOid = Long.valueOf(uuid);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid widgetset uuid: " + uuid);
        }
        Widgets.Widgetset result = widgetsetsService.getWidgetset(GetWidgetsetRequest.newBuilder()
                .setOid(widgetsetOid)
                .build());
        if (result.hasOid()) {
            return WidgetsetMapper.toUiWidgetset(result);
        } else {
            throw new UnknownObjectException("cannot find widgetset " + uuid);
        }
    }

    @Override
    public WidgetsetApiDTO createWidgetset(@Nonnull WidgetsetApiDTO input) {
        Widgets.WidgetsetInfo widgetsetInfo = WidgetsetMapper.fromUiWidgetsetApiDTO(input);
        try {
            Widgets.Widgetset result = widgetsetsService.createWidgetset(CreateWidgetsetRequest.newBuilder()
                    .setWidgetsetInfo(widgetsetInfo)
                    .build());
            return WidgetsetMapper.toUiWidgetset(result);
        } catch(Exception e) {
            logger.error("create widget exception ", e);
            throw e;
        }
    }

    @Override
    public WidgetsetApiDTO updateWidgetset(String uuid, WidgetsetApiDTO input) throws
            UnknownObjectException, OperationFailedException {
        Widgets.Widgetset updatedWidgetset = WidgetsetMapper.fromUiWidgetset(input);
        try {
            Widgets.Widgetset result = widgetsetsService.updateWidgetset(UpdateWidgetsetRequest.newBuilder()
                    .setOid(updatedWidgetset.getOid())
                    .setWidgetsetInfo(updatedWidgetset.getInfo())
                    .build());
            return WidgetsetMapper.toUiWidgetset(result);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().equals(Status.NOT_FOUND)) {
                throw new UnknownObjectException("Cannot find widgetset: " + uuid);
            } else {
                throw new OperationFailedException("Internal error updating widgetset " + uuid
                        + ", error: " + e.getMessage());
            }
        }
    }

    @Override
    public void deleteWidgetset(String uuid) throws UnknownObjectException,
            OperationFailedException {
        final long widgetsetOid;
        try {
            widgetsetOid = Long.valueOf(uuid);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid widgetset uuid: " + uuid);
        }
        try {
            widgetsetsService.deleteWidgetset(DeleteWidgetsetRequest.newBuilder()
                    .setOid(widgetsetOid)
                    .build());
        } catch(StatusRuntimeException e) {
            if (e.getStatus().equals(Status.NOT_FOUND)) {
                throw new UnknownObjectException("Cannot find widgetset to delete: " + uuid);
            } else {
                throw new OperationFailedException("Internal error updating widgetset " + uuid
                        + ", error: " + e.getMessage());
            }
        }
    }
}
