package com.vmturbo.repository.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import com.google.protobuf.util.JsonFormat;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

public class RepositoryTestUtil {
    /**
     * Load a json file into a DTO.
     *
     * @param fileName the name of the file to load
     * @return The entity DTO represented by the file
     * @throws IOException when the file is not found
     */
    public static TopologyEntityDTO messageFromJsonFile(String fileName) throws IOException {
        URL fileUrl = RepositoryTestUtil.class.getClassLoader().getResources(fileName).nextElement();
        TopologyDTO.TopologyEntityDTO.Builder builder = TopologyDTO.TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(fileUrl.openStream()), builder);
        TopologyEntityDTO message = builder.build();
        return message;
    }
}
