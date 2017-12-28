package com.vmturbo.api.component.external.api.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IWidgetSetsService;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.reports.db.VmtDbException;

/**
 * Service implementation of Widget Sets
 *
 * As a temporary measure, use an in-memory cache to store and retrieve widgetsets.
 * In this scaffolding, the widgetsets are stored independent of user.
 *
 * Note that we store the input WidgetSetApiDTO itself, instead of making a copy. It is assumed
 * that this value will come from external API requests, and so will be a new object each time.
 *
 * The full implementation for widgetset persistence is captured in: OM-23744
 **/
public class WidgetSetsService implements IWidgetSetsService {

    // The following is an in-memory cache mapping from the widgetset uuid to the widgetset dto
    private final Map<String, WidgetsetApiDTO> inMemoryWidgetSetCache = new ConcurrentHashMap<>();

    @Override
    public List<WidgetsetApiDTO> getWidgetsetList(
            @Nullable Set<String> categories,
            @Nullable String scopeType) throws Exception {
        return inMemoryWidgetSetCache.values().stream()
                .filter(widgetSet -> (categories == null || categories.isEmpty() ||
                    categories.contains(widgetSet.getCategory()))
                        && (scopeType == null || scopeType.equals(widgetSet.getScopeType())))
                .collect(Collectors.toList());
    }

    @Override
    public WidgetsetApiDTO getWidgetset(String uuid) throws Exception {
        WidgetsetApiDTO widgetset = inMemoryWidgetSetCache.get(uuid);
        if (widgetset == null) {
            throw new UnknownObjectException("Record not found");
        }
        return widgetset;
    }

    @Override
    public WidgetsetApiDTO createWidgetset(WidgetsetApiDTO input) throws Exception {
        input.setUuid(Long.toString(IdentityGenerator.next()));
        inMemoryWidgetSetCache.put(input.getUuid(), input);
        return input;
    }

    @Override
    public WidgetsetApiDTO updateWidgetset(String uuid, WidgetsetApiDTO input) throws VmtDbException {
        inMemoryWidgetSetCache.put(uuid, input);
        return input;
    }

    @Override
    public void deleteWidgetset(String uuid) throws Exception {
        inMemoryWidgetSetCache.remove(uuid);
    }

    @VisibleForTesting
    Map<String, WidgetsetApiDTO> getInMemoryWidgetSetCache() {
        return inMemoryWidgetSetCache;
    }
}
