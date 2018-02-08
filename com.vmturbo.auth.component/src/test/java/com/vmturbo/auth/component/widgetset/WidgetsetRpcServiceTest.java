package com.vmturbo.auth.component.widgetset;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.grpc.StatusRuntimeException;

import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.test.JwtContextUtil;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.CreateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.DeleteWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.GetWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.UpdateWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.common.protobuf.widgets.Widgets.WidgetsetInfo;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc.WidgetsetsServiceBlockingStub;

public class WidgetsetRpcServiceTest {

    private WidgetsetsServiceBlockingStub widgetsetRpcClient;

    private IWidgetsetStore widgetsetStore = Mockito.mock(IWidgetsetStore.class);

    private WidgetsetRpcService widgetsetRpcService = new WidgetsetRpcService(widgetsetStore);


    static final WidgetsetInfo WIDGETSET_1_INFO = WidgetsetInfo.newBuilder()
            .setWidgets("{}")
            .build();
    public static final Widgetset WIDGETSET_1 = Widgetset.newBuilder()
            .setOid(1)
            .setOwnerOid(100)
            .setInfo(WIDGETSET_1_INFO)
            .build();
    public static final WidgetsetInfo WIDGETSET_2_INFO = WidgetsetInfo.newBuilder()
            .setWidgets("{}")
            .build();
    public static final Widgetset WIDGETSET_2 = Widgetset.newBuilder()
            .setOid(2)
            .setOwnerOid(100)
            .setInfo(WIDGETSET_2_INFO)
            .build();

    private static final List<Widgetset> WIDGETSETS = Lists.newArrayList(
            WIDGETSET_1,
            WIDGETSET_2
    );

    private static final long userOid = 123;

    private JwtContextUtil jwtContextUtil;

    @Before
    public void setup() throws Exception {

        jwtContextUtil = new JwtContextUtil();
        jwtContextUtil.setupSecurityContext(widgetsetRpcService, userOid);

        widgetsetRpcClient = WidgetsetsServiceGrpc.newBlockingStub(jwtContextUtil.getChannel())
                .withInterceptors(new JwtClientInterceptor());
    }

    @After
    public void tearDown() {
        jwtContextUtil.shutdown();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetWidgetsetList() {

        // Arrange
        List<String> categories = Lists.newArrayList("a", "b");
        String scopeType = "scope-type";

        when(widgetsetStore.search(userOid, categories, scopeType)).thenReturn(WIDGETSETS.iterator());
        // Act
        Iterator<Widgetset> result = widgetsetRpcClient
                .withCallCredentials(new JwtCallCredential(jwtContextUtil.getToken().
                        getCompactRepresentation()))
                .getWidgetsetList(Widgets.GetWidgetsetListRequest.newBuilder()
                        .addAllCategories(categories)
                        .setScopeType(scopeType)
                        .build());
        // Assert
        List<Widgetset> returned = Lists.newArrayList(result);
        assertThat(returned.size(), equalTo(WIDGETSETS.size()));
        assertThat(returned, containsInAnyOrder(WIDGETSETS.toArray()));
    }

    @Test
    public void testGetWidgetset() {
        // Arrange
        when(widgetsetStore.fetch(eq(userOid), anyLong())).thenReturn(Optional.of(WIDGETSET_1));
        // Act
        Widgetset result = widgetsetRpcClient
                .getWidgetset(GetWidgetsetRequest.newBuilder()
                .setOid(1L)
                .build());
        // Assert
        assertThat(result, equalTo(WIDGETSET_1));
    }

    @Test
    public void testGetWidgetsetNotFound() {
        // Arrange
        when(widgetsetStore.fetch(eq(userOid), anyLong())).thenReturn(Optional.empty());
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("NOT_FOUND: Widgetset: 1 not found.");

        // Act
        widgetsetRpcClient.getWidgetset(GetWidgetsetRequest.newBuilder()
                .setOid(1L)
                .build());
    }

    @Test
    public void testCreateWidgetset() {
        // Arrange
        final WidgetsetInfo emptyDTO = WidgetsetInfo.newBuilder().build();
        when(widgetsetStore.createWidgetSet(userOid, emptyDTO)).thenReturn(WIDGETSET_1);
        // Act
        Widgetset result = widgetsetRpcClient.createWidgetset(CreateWidgetsetRequest.newBuilder()
                .setWidgetsetInfo(emptyDTO)
                .build());
        // Assert
        assertTrue(result.hasOid());
        assertThat(result.getOid(), equalTo(WIDGETSET_1.getOid()));
    }

    @Test
    public void testUpdateWidgetset() {
        // Arrange
        when(widgetsetStore.update(userOid, WIDGETSET_1.getOid(), WIDGETSET_1_INFO))
                .thenReturn(WIDGETSET_2);
        // Act
        Widgetset result = widgetsetRpcClient.updateWidgetset(UpdateWidgetsetRequest.newBuilder()
                .setOid(WIDGETSET_1.getOid())
                .setWidgetsetInfo(WIDGETSET_1_INFO)
                .build());
        // Assert
        assertThat(result.getOid(), equalTo(WIDGETSET_2.getOid()));
    }

    @Test
    public void testDeleteWidgetset() {
        // Arrange
        final long oidToDelete = 1L;
        when(widgetsetStore.delete(userOid, oidToDelete)).thenReturn(Optional.of(WIDGETSET_1));
        // Act
        Widgetset result = widgetsetRpcClient.deleteWidgetset(DeleteWidgetsetRequest.newBuilder()
                .setOid(oidToDelete)
                .build());
        // Assert
        assertThat(result.getOid(), equalTo(oidToDelete));
        verify(widgetsetStore).delete(userOid, oidToDelete);
        verifyNoMoreInteractions(widgetsetStore);
    }


}