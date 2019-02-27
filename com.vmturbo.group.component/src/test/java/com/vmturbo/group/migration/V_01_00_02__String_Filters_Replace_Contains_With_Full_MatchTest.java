package com.vmturbo.group.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.group.group.GroupStore;
import com.vmturbo.group.migration.V_01_00_02__String_Filters_Replace_Contains_With_Full_Match.DefaultSearchParamsMigrator;
import com.vmturbo.group.migration.V_01_00_02__String_Filters_Replace_Contains_With_Full_Match.MigratedSearchParams;
import com.vmturbo.group.migration.V_01_00_02__String_Filters_Replace_Contains_With_Full_Match.SearchParamsMigrator;

public class V_01_00_02__String_Filters_Replace_Contains_With_Full_MatchTest {

    private GroupStore groupStore = mock(GroupStore.class);

    private SearchParamsMigrator mockEditorFactory = mock(SearchParamsMigrator.class);

    private SearchParamsMigrator realEditorFactory = new DefaultSearchParamsMigrator();

    private V_01_00_02__String_Filters_Replace_Contains_With_Full_Match migration =
        new V_01_00_02__String_Filters_Replace_Contains_With_Full_Match(groupStore, mockEditorFactory);

    @Test
    public void testFilterClusters() throws GroupNotFoundException, ImmutableGroupUpdateException, DuplicateNameException {
        when(groupStore.getAll()).thenReturn(Collections.singletonList(Group.newBuilder()
            .setId(7)
            .setType(Type.CLUSTER)
            .setOrigin(Origin.USER)
            .setCluster(ClusterInfo.getDefaultInstance())
            .build()));
        migration.startMigration();

        verify(groupStore).getAll();
        verify(groupStore, times(0)).updateUserGroup(anyLong(), any());
        verifyZeroInteractions(mockEditorFactory);
    }

    @Test
    public void testFilterDiscoveredGroups() throws GroupNotFoundException, ImmutableGroupUpdateException, DuplicateNameException {
        when(groupStore.getAll()).thenReturn(Collections.singletonList(Group.newBuilder()
            .setId(7)
            .setType(Type.GROUP)
            .setOrigin(Origin.DISCOVERED)
            .setGroup(GroupInfo.newBuilder()
                .setSearchParametersCollection(SearchParametersCollection.getDefaultInstance()))
            .build()));
        migration.startMigration();

        verify(groupStore).getAll();
        verify(groupStore, times(0)).updateUserGroup(anyLong(), any());
        verifyZeroInteractions(mockEditorFactory);
    }

    @Test
    public void testFilterStaticGroups() throws GroupNotFoundException, ImmutableGroupUpdateException, DuplicateNameException {
        when(groupStore.getAll()).thenReturn(Collections.singletonList(Group.newBuilder()
            .setId(7)
            .setType(Type.GROUP)
            .setOrigin(Origin.USER)
            .setGroup(GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.getDefaultInstance()))
            .build()));
        migration.startMigration();

        verify(groupStore).getAll();
        verify(groupStore, times(0)).updateUserGroup(anyLong(), any());
        verifyZeroInteractions(mockEditorFactory);
    }

    @Test
    public void testIgnoreNotChangedGroups() throws GroupNotFoundException, ImmutableGroupUpdateException, DuplicateNameException {
        final SearchParametersCollection parametersCollection = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.getDefaultInstance())
            .build();
        when(groupStore.getAll()).thenReturn(Collections.singletonList(Group.newBuilder()
            .setId(7)
            .setType(Type.GROUP)
            .setOrigin(Origin.USER)
            .setGroup(GroupInfo.newBuilder()
                .setSearchParametersCollection(parametersCollection))
            .build()));

        final MigratedSearchParams editor = mock(MigratedSearchParams.class);
        when(mockEditorFactory.migrateSearchParams(parametersCollection)).thenReturn(editor);
        when(editor.isAnyPropertyChanged()).thenReturn(false);

        migration.startMigration();

        verify(groupStore).getAll();
        verify(mockEditorFactory).migrateSearchParams(parametersCollection);
        verify(editor).isAnyPropertyChanged();
        verify(groupStore, times(0)).updateUserGroup(anyLong(), any());
    }

    @Test
    public void testUpdateChangedGroups() throws GroupNotFoundException, ImmutableGroupUpdateException, DuplicateNameException {
        final SearchParametersCollection initialParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.getDefaultInstance())
            .build();
        final SearchParametersCollection modifiedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.getDefaultInstance())
            .addSearchParameters(SearchParameters.getDefaultInstance())
            .build();
        when(groupStore.getAll()).thenReturn(Collections.singletonList(Group.newBuilder()
            .setId(7)
            .setType(Type.GROUP)
            .setOrigin(Origin.USER)
            .setGroup(GroupInfo.newBuilder()
                .setSearchParametersCollection(initialParams))
            .build()));

        final MigratedSearchParams editor = mock(MigratedSearchParams.class);
        when(mockEditorFactory.migrateSearchParams(initialParams)).thenReturn(editor);
        when(editor.isAnyPropertyChanged()).thenReturn(true);
        when(editor.getMigratedParams()).thenReturn(modifiedParams);

        migration.startMigration();

        verify(groupStore).getAll();
        verify(mockEditorFactory).migrateSearchParams(initialParams);
        verify(editor).isAnyPropertyChanged();
        verify(groupStore).updateUserGroup(7, GroupInfo.newBuilder()
            .setSearchParametersCollection(modifiedParams)
            .build());
    }

    private static final String PROP_NAME = "proper12";

    private PropertyFilter strFilter(final String value) {
        return PropertyFilter.newBuilder()
            .setPropertyName(PROP_NAME)
            .setStringFilter(StringFilter.newBuilder()
                .setStringPropertyRegex(value))
            .build();
    }

    @Test
    public void testFilterEditorTraversalFilter() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(PropertyFilter.newBuilder()
                    .setNumericFilter(NumericFilter.getDefaultInstance()))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setTraversalFilter(TraversalFilter.newBuilder()
                        .setStoppingCondition(StoppingCondition.newBuilder()
                            .setStoppingPropertyFilter(strFilter("foo"))))))
            .build();

        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(PropertyFilter.newBuilder()
                    .setNumericFilter(NumericFilter.getDefaultInstance()))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setTraversalFilter(TraversalFilter.newBuilder()
                        .setStoppingCondition(StoppingCondition.newBuilder()
                            .setStoppingPropertyFilter(strFilter("^.*foo.*$"))))))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }

    @Test
    public void testFilterEditorClusterMembershipFilter() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(PropertyFilter.newBuilder()
                    .setNumericFilter(NumericFilter.getDefaultInstance()))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                        .setClusterSpecifier(strFilter("foo")))))
            .build();
        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(PropertyFilter.newBuilder()
                    .setNumericFilter(NumericFilter.getDefaultInstance()))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setClusterMembershipFilter(ClusterMembershipFilter.newBuilder()
                        .setClusterSpecifier(strFilter("^.*foo.*$")))))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }

    @Test
    public void testFilterEditorStringPropertyFilter() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("foo"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(strFilter("bar"))))
            .build();
        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("^.*foo.*$"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(strFilter("^.*bar.*$"))))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }

    @Test
    public void testFilterEditorListObjectPropertyFilter() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("foo"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(PropertyFilter.newBuilder()
                        .setListFilter(ListFilter.newBuilder()
                            .setObjectFilter(ObjectFilter.newBuilder()
                                .addFilters(strFilter("bar")))))))
            .build();
        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("^.*foo.*$"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(PropertyFilter.newBuilder()
                        .setListFilter(ListFilter.newBuilder()
                            .setObjectFilter(ObjectFilter.newBuilder()
                                .addFilters(strFilter("^.*bar.*$")))))))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }

    @Test
    public void testFilterEditorListStringPropertyFilter() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("foo"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(PropertyFilter.newBuilder()
                        .setListFilter(ListFilter.newBuilder()
                            .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("bar"))))))
            .build();
        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("^.*foo.*$"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(PropertyFilter.newBuilder()
                        .setListFilter(ListFilter.newBuilder()
                            .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("^.*bar.*$"))))))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }

    @Test
    public void testFilterEditorObjectPropertyFilter() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("foo"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(PropertyFilter.newBuilder()
                        .setObjectFilter(ObjectFilter.newBuilder()
                            .addFilters(strFilter("bar"))
                            .addFilters(strFilter("baz"))))))
            .build();
        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(strFilter("^.*foo.*$"))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(PropertyFilter.newBuilder()
                        .setObjectFilter(ObjectFilter.newBuilder()
                            .addFilters(strFilter("^.*bar.*$"))
                            .addFilters(strFilter("^.*baz.*$"))))))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }

    @Test
    public void testFilterEditorStringPropertyFilterNoDoublePrefix() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                // Already starts with "^"
                .setStartingFilter(strFilter("^foo")))
            .build();
        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                // Should append trailing .*$
                .setStartingFilter(strFilter("^foo.*$")))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }

    @Test
    public void testFilterEditorStringPropertyFilterNoDoubleSuffix() {
        final SearchParametersCollection params = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                // Already ends with "$"
                .setStartingFilter(strFilter("foo$")))
            .build();
        final SearchParametersCollection expectedParams = SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                // Should append starting ^.*
                .setStartingFilter(strFilter("^.*foo$")))
            .build();
        final MigratedSearchParams editor = realEditorFactory.migrateSearchParams(params);
        assertTrue(editor.isAnyPropertyChanged());
        assertThat(editor.getMigratedParams(), is(expectedParams));
    }
}