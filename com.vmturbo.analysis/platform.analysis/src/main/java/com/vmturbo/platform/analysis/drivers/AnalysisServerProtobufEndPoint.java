package com.vmturbo.platform.analysis.drivers;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

import com.google.protobuf.CodedInputStream;

import com.vmturbo.communication.AbstractProtobufEndpoint;
import com.vmturbo.communication.ITransport;

import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisCommand;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;

/**
 * Server side protobuf endpoint that receives
 * message in bytes and converts them in java object
 * to be used on the server.
 */

@ThreadSafe
public class AnalysisServerProtobufEndPoint
                extends AbstractProtobufEndpoint<AnalysisResults, AnalysisCommand> {

    /**
     * Initialize the endpoint with a transport.
     * @param transport associated transport to this endpoint
     */
    public AnalysisServerProtobufEndPoint(ITransport<ByteBuffer, InputStream> transport) {
        super(transport);
    }

    @Override
    protected AnalysisCommand parseFromData(CodedInputStream bytes) throws IOException {
        return AnalysisCommand.parseFrom(bytes);
    }
}

