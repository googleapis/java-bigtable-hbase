/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import com.google.api.client.util.Clock;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.grpc.io.OAuthCredentialsCache.CacheState;
import com.google.cloud.bigtable.grpc.io.OAuthCredentialsCache.HeaderCacheElement;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.*;
import org.threeten.bp.Duration;

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link OAuthCredentialsCache}
 */
@RunWith(JUnit4.class)
public class OAuthCredentialsCacheTest {
  private static Duration TIMEOUT = Duration.ofSeconds(5);

  private static ExecutorService executorService;

  @BeforeClass
  public static void setup() {
    executorService = Executors.newCachedThreadPool();
  }

  @AfterClass
  public static void shtudown() {
    executorService.shutdownNow();
  }

  private OAuthCredentialsCache underTest;

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
    OAuthCredentialsCache.clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return timeMs;
      }
    };
  }

  @Test
  public void testSyncRefresh() throws Exception {
    initialize(HeaderCacheElement.TOKEN_STALENESS_MS + 1);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testExecutorProblem() throws Exception {
    ExecutorService mockExecutor = Mockito.mock(ExecutorService.class);
    when(mockExecutor.submit(any(Callable.class))).thenThrow(new RuntimeException(""));
    underTest = new OAuthCredentialsCache(mockExecutor, mockCredentials);
    Assert.assertFalse(underTest.getHeader(TIMEOUT).getStatus().isOk());
  }

  @Test
  public void testStaleAndExpired() throws Exception {
    long expiration = HeaderCacheElement.TOKEN_STALENESS_MS + 1;
    initialize(expiration);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());
    long startTime = 2L;
    setTimeInMillieconds(startTime);
    Assert.assertEquals(CacheState.Stale, underTest.getHeaderCache().getCacheState());
    long expiredStaleDiff =
        HeaderCacheElement.TOKEN_STALENESS_MS - HeaderCacheElement.TOKEN_EXPIRES_MS;
    setTimeInMillieconds(startTime + expiredStaleDiff);
    Assert.assertEquals(CacheState.Expired, underTest.getHeaderCache().getCacheState());
  }

  @Test
  public void testNullExpiration() throws Exception {
    setTimeInMillieconds(100);
    Mockito.when(mockCredentials.refreshAccessToken()).thenReturn(
        new AccessToken("", null));
    underTest = new OAuthCredentialsCache(executorService, mockCredentials);
    underTest.asyncRefresh().get(100, TimeUnit.MILLISECONDS);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());

    // Make sure that the getHeader() doesn't expire in the distant future.
    setTimeInMillieconds(100000000000L);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());
  }

  @Test
  public void testRefreshAfterFailure() throws Exception {
    underTest = new OAuthCredentialsCache(executorService, mockCredentials);

    final AccessToken accessToken = new AccessToken("hi", new Date(HeaderCacheElement.TOKEN_STALENESS_MS + 1));

    //noinspection unchecked
    Mockito.when(mockCredentials.refreshAccessToken())
        // First call will throw Exception & bypass retries
        .thenThrow(new IOException())
        // Second call will succeed
        .thenReturn(accessToken);

    // First call
    OAuthCredentialsCache.HeaderToken token1 = underTest.getHeader(TIMEOUT);
    Assert.assertFalse(token1.getStatus().isOk());

    // Now the second token should be available
    OAuthCredentialsCache.HeaderToken token2 = underTest.getHeader(TIMEOUT);
    Assert.assertTrue(token2.getStatus().isOk());
    Assert.assertThat(token2.getHeader(), containsString("hi"));
    // Make sure that the token was only requested twice: once for the first failure & second time for background recovery
    Mockito.verify(mockCredentials, times(2)).refreshAccessToken();
  }

  @Test
  public void testRefreshAfterStale() throws Exception {
    underTest = new OAuthCredentialsCache(executorService, mockCredentials);

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
    OAuthCredentialsCache.HeaderToken firstResult = underTest.getHeader(TIMEOUT);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());
    Assert.assertThat(firstResult.getHeader(), containsString("stale"));

    // Fast forward until token is stale
    setTimeInMillieconds(10);

    // Second call - return stale token, but schedule refresh
    HeaderCacheElement secondResult = underTest.getHeaderUnsafe(TIMEOUT);
    Assert.assertEquals(CacheState.Stale, secondResult.getCacheState());
    Assert.assertThat(secondResult.getToken().getHeader(), containsString("stale"));

    // forward to expired
    setTimeInMillieconds(expires);

    // Third call - now returns good token
    OAuthCredentialsCache.HeaderToken thirdResult = underTest.getHeader(TIMEOUT);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());
    Assert.assertThat(thirdResult.getHeader(), containsString("good"));

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

  @Test
  public void testRevoke() throws Exception {
    long expiration = HeaderCacheElement.TOKEN_STALENESS_MS + 1;
    initialize(expiration);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());

    underTest.revokeUnauthToken(underTest.getHeaderCache().getToken());
    Assert.assertEquals(CacheState.Expired, underTest.getHeaderCache().getCacheState());

    underTest.getHeader(TIMEOUT);
    Assert.assertEquals(CacheState.Good, underTest.getHeaderCache().getCacheState());

    underTest.revokeUnauthToken(underTest.getHeaderCache().getToken());
    // Ensure that the revoke interceptor does not run so quickly after the previous revoke.
    // The cache state should stay good.
    Assert.assertTrue(underTest.getHeader(TIMEOUT).getStatus().isOk());
  }

  private void testHanging()
      throws Exception, InterruptedException, ExecutionException, TimeoutException {
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
        new OAuthCredentialsCache(executorService, mockCredentials);

    // At this point, the access token wasn't retrieved yet. The
    // RefreshingOAuth2CredentialsInterceptor considers null to be Expired.
    Assert.assertEquals(CacheState.Expired, underTest.getHeaderCache().getCacheState());

    syncCall(answer, new Date(expiration));

    // Check to make sure that the AccessToken was retrieved.
    Assert.assertEquals(CacheState.Stale, underTest.getHeaderCache().getCacheState());

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

  private void initialize(long expiration) throws Exception, ExecutionException, InterruptedException {
    Mockito.when(mockCredentials.refreshAccessToken()).thenReturn(
        new AccessToken("", new Date(expiration)));
    underTest = new OAuthCredentialsCache(executorService, mockCredentials);
    Assert.assertEquals(CacheState.Expired, underTest.getHeaderCache().getCacheState());
    underTest.asyncRefresh().get();
    Assert.assertFalse(underTest.isRefreshing());
  }
}
