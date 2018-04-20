package com.vmturbo.topology.processor.deployment.profile;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.ScopeAccessType;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.ProfileDTO.DeploymentProfileDTO;
import com.vmturbo.topology.processor.entity.EntityStore;

public class DeploymentProfileMapperTest {

    @Test
    public void testConvertDeploymentProfile() {
        final int targetId = 123;
        EntityStore entityStore = Mockito.mock(EntityStore.class);

        DeploymentProfileDTO testDeploymentProfileDTO = DeploymentProfileDTO.newBuilder()
            .setId("id")
            .setProfileName("test-deployment-profile")
            .addContextData(ContextData.newBuilder()
                .setContextKey("test-key")
                .setContextValue("test-value"))
            .addAllRelatedEntityProfileId(Lists.newArrayList("1", "2"))
            .addAllRelatedScopeId(Lists.newArrayList("3", "4"))
            .addAllAccessibleScopeId(Lists.newArrayList("5"))
            .build();
        Map<String, Long> entityMap = ImmutableMap.of("1", 1L,
            "2", 2L, "3", 3L, "4", 4L, "5", 5L);
        Mockito.when(entityStore.getTargetEntityIdMap(targetId)).thenReturn(Optional.of(entityMap));
        DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileMapper.convertToDeploymentProfile(
            targetId, entityStore, testDeploymentProfileDTO
        );
        assertEquals(deploymentProfileInfo.getName(), "test-deployment-profile");
        assertEquals(deploymentProfileInfo.getContextData(0).getKey(), "test-key");
        assertEquals(deploymentProfileInfo.getContextData(0).getValue(), "test-value");
        assertEquals(deploymentProfileInfo.getScopesCount(), 2);
        assertEquals(deploymentProfileInfo.getScopesList().stream()
            .filter(scope -> scope.getScopeAccessType().equals(ScopeAccessType.And))
            .map(scope -> scope.getIdsList())
            .flatMap(List::stream)
            .count(), 2);
        assertEquals(deploymentProfileInfo.getScopesList().stream()
            .filter(scope -> scope.getScopeAccessType().equals(ScopeAccessType.Or))
            .map(scope -> scope.getIdsList())
            .flatMap(List::stream)
            .count(), 1);
    }
}
