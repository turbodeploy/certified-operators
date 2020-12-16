package com.vmturbo.integrations.intersight.targetsync;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.api.AssetApi;
import com.cisco.intersight.client.model.AssetTarget;
import com.cisco.intersight.client.model.AssetTargetList;

import com.vmturbo.integrations.intersight.IntersightApiQuery;

/**
 * class for {@link AssetTarget} query.
 */
public class AssetTargetQuery
        extends IntersightApiQuery<AssetTarget, AssetTargetList> {

    /**
     * Constructs a {@link AssetTargetQuery} with a customer "select".
     *
     * @param select custom "select" for this query
     */
    public AssetTargetQuery(@Nonnull final String select) {
        this.select = select;
    }

    @Override
    protected @Nonnull
    AssetTargetList queryInstances(@Nonnull final ApiClient client) throws ApiException {
        final AssetApi assetApi = new AssetApi(client);
        return assetApi.getAssetTargetList(filter, orderBy, top, skip,
                select, expand, apply, count, inlineCount, at, tags);
    }

    @Override
    protected @Nonnull String getInstanceType() {
        return AssetTarget.class.getSimpleName();
    }

    @Override
    protected @Nullable List<AssetTarget> convertResponse(
            @Nonnull final AssetTargetList assetTargetList) {
        return assetTargetList.getResults();
    }
}