package com.vmturbo.api.component.external.api.service;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.WidgetsetMapper;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IWidgetSetsService;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.CreateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.DeleteWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.GetWidgetsetListRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.GetWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.TransferWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.UpdateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc.WidgetsetsServiceBlockingStub;

/**
 * Service implementation of Widget Sets, which calls the grpc service WidgetsetsRpcService in
 * in the AuthComponent.
 **/
public class WidgetSetsService implements IWidgetSetsService {

    private final WidgetsetMapper widgetsetMapper;

    // the GRPC service stub to call for Widgetset services
    private final WidgetsetsServiceBlockingStub widgetsetsService;

    private final Logger logger = LogManager.getLogger();

    public WidgetSetsService(@Nonnull final WidgetsetsServiceBlockingStub widgetsetsService,
                             @Nonnull final WidgetsetMapper widgetsetMapper) {
        this.widgetsetsService = widgetsetsService;
        this.widgetsetMapper = widgetsetMapper;
    }

    @Override
    public List<WidgetsetApiDTO> getWidgetsetList(
            @Nullable Set<String> categories,
            @Nullable String scopeType) throws ConversionException, InterruptedException {
        final GetWidgetsetListRequest.Builder widgetsetListRequest = GetWidgetsetListRequest
                .newBuilder();
        if (categories != null && categories.size() > 0) {
            widgetsetListRequest.addAllCategories(categories);
        }
        if (scopeType != null) {
            widgetsetListRequest.setScopeType(scopeType);
        }
        final List<WidgetsetApiDTO> answer = Lists.newLinkedList();
        final Iterator<Widgetset> widdetSetIterator =
        widgetsetsService.getWidgetsetList(widgetsetListRequest.build());
        while (widdetSetIterator.hasNext()) {
            answer.add(widgetsetMapper.toUiWidgetset(widdetSetIterator.next()));
        }
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
            return widgetsetMapper.toUiWidgetset(result);
        } else {
            throw new UnknownObjectException("cannot find widgetset " + uuid);
        }
    }

    @Override
    public WidgetsetApiDTO createWidgetset(@Nonnull WidgetsetApiDTO input)
            throws ConversionException, InterruptedException {
        Widgets.WidgetsetInfo widgetsetInfo = widgetsetMapper.fromUiWidgetsetApiDTO(input);
        final Widgets.Widgetset result = widgetsetsService.createWidgetset(
                CreateWidgetsetRequest.newBuilder().setWidgetsetInfo(widgetsetInfo).build());
        AuditLog.newEntry(AuditAction.CREATE_WIDGETSET,
                String.format("Created new widget set %s", widgetsetInfo.getDisplayName()), true)
                .targetName("WIDGET SET")
                .audit();
        return widgetsetMapper.toUiWidgetset(result);
    }

    @Override
    public WidgetsetApiDTO updateWidgetset(String uuid, WidgetsetApiDTO input)
            throws UnknownObjectException, OperationFailedException, ConversionException,
            InterruptedException {
        Widgets.Widgetset updatedWidgetset = widgetsetMapper.fromUiWidgetset(input);
        try {
            Widgets.Widgetset result = widgetsetsService.updateWidgetset(UpdateWidgetsetRequest.newBuilder()
                    .setOid(updatedWidgetset.getOid())
                    .setWidgetsetInfo(updatedWidgetset.getInfo())
                    .build());
            AuditLog.newEntry(AuditAction.UPDATE_WIDGETSET,
                    String.format("Updated widget set %s", updatedWidgetset.getInfo().getDisplayName()), true)
                    .targetName("WIDGET SET")
                    .audit();
            return widgetsetMapper.toUiWidgetset(result);
        } catch (StatusRuntimeException e) {
            AuditLog.newEntry(AuditAction.UPDATE_WIDGETSET,
                    String.format("Failed to update widget set %s", updatedWidgetset.getInfo().getDisplayName()), false)
                    .targetName("WIDGET SET")
                    .audit();
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
            Widgets.Widgetset result = widgetsetsService.deleteWidgetset(DeleteWidgetsetRequest.newBuilder()
                    .setOid(widgetsetOid)
                    .build());
            AuditLog.newEntry(AuditAction.DELETE_WIDGETSET,
                    String.format("Deleted widget set with id: %s", result.getInfo().getDisplayName()), true)
                    .targetName("WIDGET SET")
                    .audit();
        } catch(StatusRuntimeException e) {
            AuditLog.newEntry(AuditAction.DELETE_WIDGETSET,
                    String.format("Failed to delete widget set %d", widgetsetOid), false)
                    .targetName("WIDGET SET")
                    .audit();
            if (e.getStatus().equals(Status.NOT_FOUND)) {
                throw new UnknownObjectException("Cannot find widgetset to delete: " + uuid);
            } else {
                throw new OperationFailedException("Internal error updating widgetset " + uuid
                        + ", error: " + e.getMessage());
            }
        }
    }

    /**
     * Transfer all of the widgetsets from the user to be deleted to current login user, including
     * the non-shared widgetsets.
     *
     * @param removedUserid the user id which was deleted.
     * @param userName the user name which was deleted.
     */
    public void transferWidgetsets(@Nonnull final String removedUserid, @Nonnull final String userName) {
        final Iterator<Widgetset> updatedWidgetsetsIter = widgetsetsService.transferWidgetset(
            TransferWidgetsetRequest.newBuilder()
                .setRemovedUserid(removedUserid)
                .build());
        while (updatedWidgetsetsIter.hasNext()) {
            final Widgetset widgetset = updatedWidgetsetsIter.next();
            AuditLog.newEntry(AuditAction.TRANSFER_WIDGETSET,
                    String.format("Transferred widget set %s from user %s to %s.",
                            widgetset.getInfo().getDisplayName(), userName, widgetset.getOwnerUserid()), true)
                    .targetName("WIDGET SET")
                    .audit();
            logger.info("Transfer widgetset {} from user {} to {}.",
                widgetset.getInfo().getDisplayName(), userName, widgetset.getOwnerUserid());
        }
    }
}
