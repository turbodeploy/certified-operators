package com.vmturbo.repository.dto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.NamespaceInfo;

/**
 * Class to test NamespaceInfoRepoDTO conversion to and from TypeSpecificInfo.
 */
public class NamespaceInfoRepoDTOTest {

    /**
     * Test filling a RepoDTO from a TypeSpecificInfo with data fields populated.
     */
    @Test
    public void testFillFromTypeSpecificInfo() {
        final ServiceEntityRepoDTO repoDTO = new ServiceEntityRepoDTO();
        final NamespaceInfo.Builder builder = NamespaceInfo.newBuilder();
        final TypeSpecificInfo.Builder tsiBuilder = TypeSpecificInfo.newBuilder();
        final NamespaceInfoRepoDTO nsInfoDto = new NamespaceInfoRepoDTO();

        builder.setAverageNodeCpuFrequency(2.0);
        nsInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setNamespace(builder).build(), repoDTO);
        assertEquals(2.0, nsInfoDto.getAverageNodeCpuFrequency(), 0);

        builder.setAverageNodeCpuFrequency(3.0);
        nsInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setNamespace(builder).build(), repoDTO);
        assertEquals(3.0, nsInfoDto.getAverageNodeCpuFrequency(), 0);
    }

    /**
     * Test filling a RepoDTO from an empty TypeSpecificInfo.
     */
    @Test
    public void testFillFromEmptyTypeSpecificInfo() {
        // arrange
        TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
            .build();
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final NamespaceInfoRepoDTO testNamespaceRepoDTO =
            new NamespaceInfoRepoDTO();

        // act
        testNamespaceRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);

        // assert
        assertEquals(1.0, testNamespaceRepoDTO.getAverageNodeCpuFrequency(), 0);
    }

    /**
     * Test extracting a TypeSpecificInfo from a RepoDTO.
     */
    @Test
    public void testCreateFromRepoDTO() {
        final ServiceEntityRepoDTO repoDTO = new ServiceEntityRepoDTO();
        final NamespaceInfo.Builder builder = NamespaceInfo.newBuilder();
        final TypeSpecificInfo.Builder tsiBuilder = TypeSpecificInfo.newBuilder();
        final NamespaceInfoRepoDTO nsInfoDto = new NamespaceInfoRepoDTO();

        builder.setAverageNodeCpuFrequency(2.0);
        nsInfoDto.fillFromTypeSpecificInfo(tsiBuilder.setNamespace(builder).build(), repoDTO);
        assertEquals(2.0, nsInfoDto.getAverageNodeCpuFrequency(), 0);

        assertEquals(2.0,
            nsInfoDto.createTypeSpecificInfo().getNamespace().getAverageNodeCpuFrequency(), 0);
    }

    /**
     * Test extracting a TypeSpecificInfo from an empty RepoDTO.
     */
    @Test
    public void testCreateFromEmptyRepoDTO() {
        // arrange
        final NamespaceInfoRepoDTO repoDto =
            new NamespaceInfoRepoDTO();
        NamespaceInfo expected = NamespaceInfo.newBuilder()
            .setAverageNodeCpuFrequency(1.0)
            .build();
        // act
        TypeSpecificInfo result = repoDto.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasNamespace());
        final NamespaceInfo namespaceInfo = result.getNamespace();
        assertEquals(expected, namespaceInfo);
    }
}
