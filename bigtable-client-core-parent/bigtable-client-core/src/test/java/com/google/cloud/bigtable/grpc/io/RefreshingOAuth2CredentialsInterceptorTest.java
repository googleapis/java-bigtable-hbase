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

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Clock;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.grpc.io.RefreshingOAuth2CredentialsInterceptor.CacheState;
import com.google.cloud.bigtable.grpc.io.RefreshingOAuth2CredentialsInterceptor.HeaderCacheElement;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.grpc.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * Tests for {@link RefreshingOAuth2CredentialsInterceptor}
 */
@RunWith(JUnit4.class)
public class RefreshingOAuth2CredentialsInterceptorTest {

  private static ExecutorService executorService;

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

  @Before
  public void setupMocks() {
    MockitoAnnotations.initMocks(this);
    setTimeInMillieconds(0L);
    when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
        .thenReturn(mockClientCall);
  }

  private void setTimeInMillieconds(final long timeMs) {
    RefreshingOAuth2CredentialsInterceptor.clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return timeMs;
      }
    };
  }

  @Test
  public void testSyncRefresh() throws IOException {
    Mockito.when(mockCredentials.refreshAccessToken()).thenReturn(
        new AccessToken("", new Date(HeaderCacheElement.TOKEN_STALENESS_MS + 1)));
    underTest = new RefreshingOAuth2CredentialsInterceptor(executorService, mockCredentials);
    Assert.assertEquals(CacheState.Expired, underTest.getCacheState());
    underTest.getHeaderSafe();
    Assert.assertNotEquals(CacheState.Exception, underTest.getCacheState());
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
    Assert.assertFalse(underTest.isRefreshing());
  }

  @Test
  /**
   * Basic test to make sure that the interceptor works properly
   */
  public void testIntercept() throws IOException {
    initialize(HeaderCacheElement.TOKEN_STALENESS_MS + 1);
    initializeCall(CallOptions.DEFAULT);
  }

  @Test
  /**
   * Test to make sure that the interceptor revokes credentials if an RPC receives an UNAUTHENTICATED status.
   */
  public void testInvalid() throws IOException {
    long expiration = HeaderCacheElement.TOKEN_STALENESS_MS + 1;
    initialize(expiration);
    ClientCall.Listener listener = initializeCall(CallOptions.DEFAULT);
    listener.onClose(Status.UNAUTHENTICATED, new Metadata());
    Assert.assertEquals(CacheState.Expired, underTest.getCacheState());
  }

  private ClientCall.Listener initializeCall(CallOptions callOptions) throws IOException {
    ClientCall<ReadRowsRequest, ReadRowsResponse> call = underTest.interceptCall(
        BigtableGrpc.METHOD_READ_ROWS,
        callOptions,
        mockChannel);
    Metadata metadata = new Metadata();
    call.start(mockListener, metadata);
    Assert.assertEquals(underTest.getHeaderSafe().header,
        metadata.get(RefreshingOAuth2CredentialsInterceptor.AUTHORIZATION_HEADER_KEY));

    ArgumentCaptor<ClientCall.Listener> listenerCaptor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockClientCall, times(1))
        .start(listenerCaptor.capture(), same(metadata));

    call.request(1);
    call.sendMessage(ReadRowsRequest.getDefaultInstance());

    verify(mockClientCall, times(1))
        .request(eq(1));
    verify(mockClientCall, times(1))
        .sendMessage(same(ReadRowsRequest.getDefaultInstance()));

    return listenerCaptor.getValue();
  }

  @Test
  public void testRefresh() throws IOException {
    Mockito.when(mockCredentials.refreshAccessToken()).thenReturn(
        new AccessToken("", new Date(HeaderCacheElement.TOKEN_STALENESS_MS + 1)));
    underTest = new RefreshingOAuth2CredentialsInterceptor(MoreExecutors.newDirectExecutorService(),
        mockCredentials);
    underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
    Assert.assertFalse(underTest.isRefreshing());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testExecutorProblem() throws IOException {
    ExecutorService mockExecutor = Mockito.mock(ExecutorService.class);
    when(mockExecutor.submit(any(Callable.class))).thenThrow(new RuntimeException(""));
    underTest = new RefreshingOAuth2CredentialsInterceptor(mockExecutor, mockCredentials);
    Assert.assertEquals(CacheState.Exception, underTest.getHeaderSafe().getCacheState());
  }

  @Test
  public void testStaleAndExpired() throws IOException {
    long expiration = HeaderCacheElement.TOKEN_STALENESS_MS + 1;
    initialize(expiration);
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
    long startTime = 2L;
    setTimeInMillieconds(startTime);
    Assert.assertEquals(CacheState.Stale, underTest.getCacheState());
    long expiredStaleDiff =
        HeaderCacheElement.TOKEN_STALENESS_MS - HeaderCacheElement.TOKEN_EXPIRES_MS;
    setTimeInMillieconds(startTime + expiredStaleDiff);
    Assert.assertEquals(CacheState.Expired, underTest.getCacheState());
  }

  @Test
  public void testNullExpiration() throws Exception {
    setTimeInMillieconds(100);
    Mockito.when(mockCredentials.refreshAccessToken()).thenReturn(
        new AccessToken("", null));
    underTest = new RefreshingOAuth2CredentialsInterceptor(executorService, mockCredentials);
    underTest.asyncRefresh().get(100, TimeUnit.MILLISECONDS);
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());

    // Make sure that the header doesn't expire in the distant future.
    setTimeInMillieconds(100000000000L);
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
  }

  @Test
  public void testRefreshAfterFailure() throws Exception {
    underTest = new RefreshingOAuth2CredentialsInterceptor(executorService, mockCredentials);

    final AccessToken accessToken = new AccessToken("hi", new Date(HeaderCacheElement.TOKEN_STALENESS_MS + 1));

    //noinspection unchecked
    Mockito.when(mockCredentials.refreshAccessToken())
        // First call will throw Exception & bypass retries
        .thenThrow(new IOException())
        // Second call will succeed
        .thenReturn(accessToken);

    // First call
    HeaderCacheElement firstResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Exception, firstResult.getCacheState());

    // Now the second token should be available
    HeaderCacheElement secondResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Good, secondResult.getCacheState());
    Assert.assertThat(secondResult.header, containsString("hi"));
    // Make sure that the token was only requested twice: once for the first failure & second time for background recovery
    Mockito.verify(mockCredentials, times(2)).refreshAccessToken();
  }

  @Test
  public void testRefreshAfterStale() throws Exception {
    underTest = new RefreshingOAuth2CredentialsInterceptor(executorService, mockCredentials);

    long expires = HeaderCacheElement.TOKEN_STALENESS_MS + 1;
    final AccessToken staleToken = new AccessToken("stale", new Date(expires));
    AccessToken goodToken =
        new AccessToken("good", new Date(expires + HeaderCacheElement.TOKEN_STALENESS_MS + 1));

    //noinspection unchecked
    Mockito.when(mockCredentials.refreshAccessToken())
        // First call will setup a stale token
        .thenReturn(staleToken)
        // Second call will give a good token
        .thenReturn(goodToken);

    // First call - setup
    HeaderCacheElement firstResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Good, firstResult.getCacheState());
    Assert.assertThat(firstResult.header, containsString("stale"));

    // Fast forward until token is stale
    setTimeInMillieconds(10);

    // Second call - return stale token, but schedule refresh
    HeaderCacheElement secondResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Stale, secondResult.getCacheState());
    Assert.assertThat(secondResult.header, containsString("stale"));

    // forward to expired
    setTimeInMillieconds(expires);

    // Third call - now returns good token
    HeaderCacheElement thirdResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Good, thirdResult.getCacheState());
    Assert.assertThat(thirdResult.header, containsString("good"));

    // Make sure that the token was only requested twice: once for the stale token & second time for the good token
    Mockito.verify(mockCredentials, times(2)).refreshAccessToken();
  }

  @Test(timeout = 30000)
  /*
   * Test that checks that concurrent requests to RefreshingOAuth2CredentialsInterceptor refresh
   * logic doesn't cause hanging behavior.  Specifically, when an Expired condition occurs it
   * triggers a call to syncRefresh() which potentially waits for refresh that was initiated
   * from another thread either through syncRefresh() or asyncRefresh().  This test case simulates
   * that condition.
   */
  public void testRefreshDoesntHang() throws Exception {
    for (int i = 0; i < 100; i++) {
      MockitoAnnotations.initMocks(this);
      testHanging();
    }
  }

  private void testHanging()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    // Assume that the user starts at this time... it's an arbitrarily big number which will
    // assure that subtracting HeaderCacheElement.TOKEN_STALENESS_MS and TOKEN_EXPIRES_MS will not
    // be negative.
    long start = HeaderCacheElement.TOKEN_STALENESS_MS * 10;
    setTimeInMillieconds(start);

    // RefreshingOAuth2CredentialsInterceptor will show that the access token is stale.
    final long expiration = start + HeaderCacheElement.TOKEN_EXPIRES_MS + 1;

    // Create a mechanism that will allow us to control when the accessToken is returned.
    // mockCredentials.refreshAccessToken() will get called asynchronously and will wait until the
    // lock is notified before returning.  That will allow us to set up multiple concurrent calls
    final FutureAnswer<AccessToken> answer = new FutureAnswer<>();
    Mockito.when(mockCredentials.refreshAccessToken()).thenAnswer(answer);

    underTest =
        new RefreshingOAuth2CredentialsInterceptor(executorService, mockCredentials);

    // At this point, the access token wasn't retrieved yet. The
    // RefreshingOAuth2CredentialsInterceptor considers null to be Expired.
    Assert.assertEquals(CacheState.Expired, underTest.getCacheState());

    syncCall(answer, new Date(expiration));

    // Check to make sure that the AccessToken was retrieved.
    Assert.assertEquals(CacheState.Stale, underTest.getCacheState());

    // Check to make sure we're no longer refreshing.
    Assert.assertFalse(underTest.isRefreshing());

    answer.reset();

    // Kick off 100 asynchronous refreshes. Kicking off more than one shouldn't be
    // necessary, but also should not be harmful, since there are likely to be multiple concurrent
    // requests that call asyncRefresh() when the token turns stale.
    Future<HeaderCacheElement> previous = underTest.asyncRefresh();
    for (int i = 0; i < 100; i++) {
      Future<HeaderCacheElement> current = underTest.asyncRefresh();
      Assert.assertEquals(previous, current);
      previous = current;
    }

    syncCall(answer, new Date(expiration + HeaderCacheElement.TOKEN_EXPIRES_MS + 1));
    Assert.assertFalse(underTest.isRefreshing());
  }

  private static class FutureAnswer<T> implements Answer<T> {

    private SettableFuture<T> future = SettableFuture.create();

    @Override
    public T answer(InvocationOnMock invocation) throws Throwable {
      return future.get();
    }

    void set(T value) {
      future.set(value);
    }

    void reset() {
      future = SettableFuture.create();
    }
  }

  private void syncCall(FutureAnswer<AccessToken> answer, Date expirationTime)
      throws InterruptedException, ExecutionException, TimeoutException {
    underTest.asyncRefresh();

    // There should be a single thread kicked off by the underTest.asyncRefresh() calls about
    // actually doing a refresh at this point; the other ones will have see that a refresh is in
    // progress and finish the invocation of the Thread without performing a refresh().. Make sure
    // that at least 1 refresh process is in progress.
    Assert.assertTrue(underTest.isRefreshing());

    answer.set(new AccessToken("hi", expirationTime));
    underTest.asyncRefresh().get(100, TimeUnit.MILLISECONDS);
  }

  private void initialize(long expiration) throws IOException {
    Mockito.when(mockCredentials.refreshAccessToken()).thenReturn(
        new AccessToken("", new Date(expiration)));
    underTest = new RefreshingOAuth2CredentialsInterceptor(executorService, mockCredentials);
    underTest.getHeaderSafe();
  }
}
