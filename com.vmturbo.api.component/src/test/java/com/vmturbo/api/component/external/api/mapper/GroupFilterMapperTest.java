package com.vmturbo.api.component.external.api.mapper;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * This class tests the functionality of {@link GroupFilterMapper} class.
 */
public class GroupFilterMapperTest {

    private final GroupFilterMapper groupFilterMapper = new GroupFilterMapper();

    /**
     * Tests translation of a dynamic nested group with two filter properties
     * (one for name and one for tags) from the API structure to the internal
     * nested group structure.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testDynamicNestedGroupTranslation() throws Exception {
        final FilterApiDTO nameFilter = new FilterApiDTO();
        nameFilter.setFilterType(GroupFilterMapper.CLUSTERS_FILTER_TYPE);
        nameFilter.setExpType(EntityFilterMapper.EQUAL);
        nameFilter.setExpVal("the name");

        final FilterApiDTO tagsFilter = new FilterApiDTO();
        tagsFilter.setFilterType(GroupFilterMapper.CLUSTERS_BY_TAGS_FILTER_TYPE);
        tagsFilter.setExpType(EntityFilterMapper.EQUAL);
        tagsFilter.setExpVal("key=value1|key=value2");

        GroupFilter groupFilter = groupFilterMapper.apiFilterToGroupFilter(
                        GroupType.COMPUTE_HOST_CLUSTER, ImmutableList.of(nameFilter, tagsFilter));
        final PropertyFilter namePropertyFilter = groupFilter.getPropertyFiltersList().get(0);
        final PropertyFilter tagsPropertyFilter = groupFilter.getPropertyFiltersList().get(1);

        Assert.assertEquals(PropertyFilter.newBuilder()
                        .setPropertyName(StringConstants.DISPLAY_NAME_ATTR)
                        .setStringFilter(StringFilter.newBuilder()
                                        .setStringPropertyRegex("^the name$").setPositiveMatch(true)
                                        .setCaseSensitive(false).build())
                        .build(), namePropertyFilter);
        Assert.assertEquals(
                        PropertyFilter.newBuilder().setPropertyName(StringConstants.TAGS_ATTR)
                                        .setMapFilter(MapFilter.newBuilder().setKey("key")
                                                        .addValues("value1").addValues("value2")
                                                        .setPositiveMatch(true).build())
                                        .build(),
                        tagsPropertyFilter);
    }

}
