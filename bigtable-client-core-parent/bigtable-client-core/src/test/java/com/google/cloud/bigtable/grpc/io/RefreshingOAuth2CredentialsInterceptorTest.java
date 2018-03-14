/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.io;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.auth.oauth2.OAuth2Credentials;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.grpc.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


/**
 * Tests for {@link RefreshingOAuth2CredentialsInterceptor}
 */
@RunWith(JUnit4.class)
public class RefreshingOAuth2CredentialsInterceptorTest {

    private static ExecutorService executorService;
    private static String HEADER = "SomeHeader";

    @BeforeClass
    public static void setup() {
        executorService = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void shtudown() {
        executorService.shutdownNow();
    }

    private RefreshingOAuth2CredentialsInterceptor underTest;

    @Mock
    private OAuth2Credentials mockCredentials;

    @Mock
    private Channel mockChannel;

    @Mock
    private ClientCall mockClientCall;

    @Mock
    private ClientCall.Listener mockListener;

    @Mock
    private OAuthCredentialsCache mockCache;

    @Before
    public void setupMocks() {
        MockitoAnnotations.initMocks(this);
        when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
                .thenReturn(mockClientCall);
        underTest = new RefreshingOAuth2CredentialsInterceptor(mockCache);
    }

    @Test
    /**
     * Basic test to make sure that the interceptor works properly
     */
    public void testValidAuthToken() throws IOException {
        initializeOk();
        sendRequest(CallOptions.DEFAULT);
        receiveMessage();
        checkOKCompletedCorrectly();
    }

    @Test
    /**
     * Ensures that default deadline is honored
     */
    public void testDefaultDeadline() throws IOException {
        initializeOk();
        sendRequest(CallOptions.DEFAULT);
        assertEquals(RefreshingOAuth2CredentialsInterceptor.TIMEOUT_SECONDS * 1000, getDeadlineMs());
    }

    @Test
    /**
     * Ensures that short deadlines in CallOptions are honored
     */
    public void testShortDeadline() throws IOException {
        initializeOk();
        sendRequest(CallOptions.DEFAULT.withDeadlineAfter(2, TimeUnit.SECONDS));
        assertTrue(getDeadlineMs() <= 2000);
    }

    @Test
    /**
     * Ensures that long deadlines in CallOptions are honored
     */
    public void testLongDeadline() throws IOException {
        initializeOk();
        sendRequest(CallOptions.DEFAULT.withDeadlineAfter(60, TimeUnit.SECONDS));
        assertTrue(getDeadlineMs() >= 59000);
    }

    private long getDeadlineMs() {
        ArgumentCaptor<Long> timeoutCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockCache, times(1)).getHeader(timeoutCaptor.capture());
        return timeoutCaptor.getValue();
    }

    @Test
    /**
     * Test to make sure that the interceptor revokes credentials if an RPC receives an UNAUTHENTICATED status.
     */
    public void testInvalidAuthToken() throws IOException {
        Status grpcStatus = Status.UNAUTHENTICATED;

        // Something bad happened, and authentication could not be established
        when(mockCache.getHeader(anyInt()))
                .thenReturn(new OAuthCredentialsCache.HeaderToken(grpcStatus, null));

        ClientCall<ReadRowsRequest, ReadRowsResponse> call = underTest.interceptCall(
                BigtableGrpc.METHOD_READ_ROWS,
                CallOptions.DEFAULT,
                mockChannel);
        call.start(mockListener, new Metadata());

        // Since the auth header is not available, the call should immediately fail with a non-OK status
        verify(mockListener, times(1)).onClose(same(grpcStatus), any(Metadata.class));

        // Users might do some additional work with call, so it shouldn't be null, but should not
        // forward the request to the underlying call either.

        // Verify that request, sendMessage, halfClose and cancel calls on the call object returned from
        // is not null and does not
        call.request(1);
        verify(mockClientCall, times(0)).request(anyInt());

        call.halfClose();
        verify(mockClientCall, times(0)).halfClose();

        call.sendMessage(ReadRowsRequest.getDefaultInstance());
        verify(mockClientCall, times(0)).sendMessage(any(ReadRowsRequest.class));

        call.cancel("", null);
        verify(mockClientCall, times(0)).cancel(anyString(), any(Throwable.class));
    }

    @Test
    /**
     * Test to make sure that the interceptor revokes credentials if an RPC receives an UNAUTHENTICATED status.
     */
    public void testRpcReturnsUnauthenticated() throws IOException {
        initializeOk();

        // 10 requests come in around the same time, and all of them get UNAUTHENTICATED
        for (int i = 0; i < 10; i++) {
            sendRequest(CallOptions.DEFAULT);
            getListener().onClose(Status.UNAUTHENTICATED, new Metadata());
        }

        // only a single revoke should be submitted due to rate limiting
        verify(mockCache, times(1)).revokeUnauthToken(
                any(OAuthCredentialsCache.HeaderToken.class));
    }

    private ClientCall<ReadRowsRequest, ReadRowsResponse> sendRequest(CallOptions callOptions) {
        ClientCall<ReadRowsRequest, ReadRowsResponse> call = underTest.interceptCall(
                BigtableGrpc.METHOD_READ_ROWS,
                callOptions,
                mockChannel);
        call.start(mockListener, new Metadata());
        call.request(1);
        call.sendMessage(ReadRowsRequest.getDefaultInstance());
        call.halfClose();
        return call;
    }

    private ClientCall.Listener receiveMessage() {
        ClientCall.Listener listener = getListener();
        listener.onMessage(ReadRowsResponse.getDefaultInstance());
        listener.onClose(Status.OK, new Metadata());

        return listener;
    }

    private ClientCall.Listener getListener() {
        ArgumentCaptor<ClientCall.Listener> listenerCaptor = ArgumentCaptor.forClass(ClientCall.Listener.class);
        verify(mockClientCall, atLeastOnce()).start(listenerCaptor.capture(), any(Metadata.class));

        // this gets the last matched value
        return listenerCaptor.getValue();
    }

    private void initializeOk() {
        when(mockCache.getHeader(anyInt()))
                .thenReturn(new OAuthCredentialsCache.HeaderToken(Status.OK, HEADER));
    }

    private void checkOKCompletedCorrectly() {
        ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(mockClientCall, atLeastOnce()).start(any(ClientCall.Listener.class), metadataCaptor.capture());
        Assert.assertEquals(HEADER,
                metadataCaptor.getValue().get(RefreshingOAuth2CredentialsInterceptor.AUTHORIZATION_HEADER_KEY));

        verify(mockClientCall, times(1))
                .request(eq(1));
        verify(mockClientCall, times(1))
                .sendMessage(same(ReadRowsRequest.getDefaultInstance()));
        verify(mockListener, times(1)).onMessage(same(ReadRowsResponse.getDefaultInstance()));
        verify(mockListener, times(1)).onClose(same(Status.OK), any(Metadata.class));
    }
}
