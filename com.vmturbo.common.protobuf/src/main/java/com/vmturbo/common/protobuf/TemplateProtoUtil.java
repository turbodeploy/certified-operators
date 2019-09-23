package com.vmturbo.common.protobuf;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;

/**
 * Utilities for easier interaction with the protos defined in TemplateDTO.
 */
public class TemplateProtoUtil {

    private TemplateProtoUtil() {}

    /**
     * Flatten an iterator over chunks of {@link SingleTemplateResponse}s into a stream of
     * {@link SingleTemplateResponse}s.
     *
     * @param responseIt The iterator, returned from an RPC call to the templates service.
     * @return A stream of {@link SingleTemplateResponse}s contained in the response..
     */
    @Nonnull
    public static Stream<SingleTemplateResponse> flattenGetResponse(
            @Nonnull final Iterator<GetTemplatesResponse> responseIt) {
        final Iterable<GetTemplatesResponse> iterable = () -> responseIt;
        return StreamSupport.stream(iterable.spliterator(), false)
            .flatMap(resp -> resp.getTemplatesList().stream());
    }
}
