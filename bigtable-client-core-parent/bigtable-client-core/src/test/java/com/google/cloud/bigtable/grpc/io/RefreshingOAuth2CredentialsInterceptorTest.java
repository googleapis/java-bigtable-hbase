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
import static org.mockito.Mockito.times;

import com.google.api.client.util.Clock;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.grpc.io.RefreshingOAuth2CredentialsInterceptor.CacheState;
import com.google.cloud.bigtable.grpc.io.RefreshingOAuth2CredentialsInterceptor.HeaderCacheElement;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


@RunWith(JUnit4.class)
public class RefreshingOAuth2CredentialsInterceptorTest {

  private static ExecutorService executor;

  private static void setTimeInMillieconds(final long timeMs) {
    RefreshingOAuth2CredentialsInterceptor.clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return timeMs;
      }
    };
  }

  private RefreshingOAuth2CredentialsInterceptor underTest;

  @Mock
  private OAuth2Credentials credentials;

  @BeforeClass
  public static void setup() {
    executor = Executors.newCachedThreadPool();
  }

  @AfterClass
  public static void shutdown() {
    executor.shutdownNow();
  }

  @Before
  public void setupMocks() {
    MockitoAnnotations.initMocks(this);
    setTimeInMillieconds(0L);
  }

  @After
  public void reset() {
    RefreshingOAuth2CredentialsInterceptor.clock = Clock.SYSTEM;
  }

  @Test
  public void testSyncRefresh() throws Exception {
    initialize(executor, HeaderCacheElement.TOKEN_STALENESS_MS + 1);
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
    Assert.assertFalse(underTest.isRefreshing());
  }

  @Test
  public void testDirectExecutor() throws Exception {
    initialize(MoreExecutors.newDirectExecutorService(), HeaderCacheElement.TOKEN_STALENESS_MS + 1);
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
    Assert.assertFalse(underTest.isRefreshing());
  }

  @Test
  public void testStaleAndExpired() throws Exception {
    initialize(executor, HeaderCacheElement.TOKEN_STALENESS_MS + 1);
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
  public void testNullExpiration() {
    setTimeInMillieconds(100);
    HeaderCacheElement element = new HeaderCacheElement(new AccessToken("", null));
    Assert.assertEquals(CacheState.Good, element.getCacheState());

    // Make sure that the header doesn't expire in the distant future.
    setTimeInMillieconds(100000000000L);
    Assert.assertEquals(CacheState.Good, element.getCacheState());
  }

  @Test
  public void testRefreshAfterFailure() throws Exception {
    underTest = new RefreshingOAuth2CredentialsInterceptor(executor, credentials);

    final AccessToken accessToken =
        new AccessToken("hi", new Date(HeaderCacheElement.TOKEN_STALENESS_MS + 1));

    //noinspection unchecked
    Mockito.when(credentials.refreshAccessToken())
        // First call will throw Exception & bypass retries
        .thenThrow(new IOException())
        // Second call will succeed
        .thenReturn(accessToken);

    // First call
    underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Exception, underTest.getCacheState());

    // Now the second token should be available
    HeaderCacheElement secondResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
    Assert.assertThat(secondResult.header, containsString("hi"));
    // Make sure that the token was only requested twice: once for the first failure & second time for background recovery
    Mockito.verify(credentials, times(2)).refreshAccessToken();
  }

  @Test
  public void testRefreshAfterStale() throws Exception {
    underTest = new RefreshingOAuth2CredentialsInterceptor(executor, credentials);

    AccessToken staleToken =
        new AccessToken("stale", new Date(HeaderCacheElement.TOKEN_STALENESS_MS + 1));
    AccessToken goodToken =
        new AccessToken("good", new Date(HeaderCacheElement.TOKEN_STALENESS_MS + 11));

    //noinspection unchecked
    Mockito.when(credentials.refreshAccessToken())
        // First call will setup a stale token
        .thenReturn(staleToken)
        // Second call will give a good token
        .thenReturn(goodToken);

    // First call - setup
    HeaderCacheElement firstResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Good, firstResult.getCacheState());
    Assert.assertThat(firstResult.header, containsString("stale"));
    Assert.assertFalse(underTest.isRefreshing());

    // Fast forward until token is stale
    setTimeInMillieconds(10);

    // Second call - return stale token, but schedule refresh
    HeaderCacheElement secondResult = underTest.getHeaderSafe();
    Assert.assertEquals(CacheState.Stale, secondResult.getCacheState());
    Assert.assertThat(secondResult.header, containsString("stale"));

    // Third call - now returns good token after the future resolves
    HeaderCacheElement thirdResult = underTest.syncRefresh();
    Assert.assertEquals(CacheState.Good, thirdResult.getCacheState());
    Assert.assertThat(thirdResult.header, containsString("good"));
    Assert.assertFalse(underTest.isRefreshing());

    // Make sure that the token was only requested twice: once for the stale token & second time for the good token
    Mockito.verify(credentials, times(2)).refreshAccessToken();
  }


  @Test
  /*
   * Test that checks that concurrent requests to RefreshingOAuth2CredentialsInterceptor refresh
   * logic doesn't cause hanging behavior.  Specifically, when an Expired condition occurs it
   * triggers a call to syncRefresh() which potentially waits for refresh that was initiated
   * from another thread either through syncRefresh() or asyncRefresh().  This test case simulates
   * that condition.
   */
  public void testRefreshDoesntHang() throws Exception {
    // Assume that the user starts at this time... it's an arbitrarily big number which will
    // assure that subtracting HeaderCacheElement.TOKEN_STALENESS_MS and TOKEN_EXPIRES_MS will not
    // be negative.
    long start = HeaderCacheElement.TOKEN_STALENESS_MS * 10;
    setTimeInMillieconds(start);

    // Create a mechanism that will allow us to control when the accessToken is returned.
    // credentials.refreshAccessToken() will get called asynchronously and will wait until the
    // lock is notified before returning.  That will allow us to set up multiple concurrent calls
    FutureAnswer<AccessToken> answer = new FutureAnswer<>();
    Mockito.when(credentials.refreshAccessToken()).thenAnswer(answer);

    underTest = new RefreshingOAuth2CredentialsInterceptor(executor, credentials);

    // At this point, the access token wasn't retrieved yet. The
    // RefreshingOAuth2CredentialsInterceptor considers null to be Expired.
    Assert.assertEquals(CacheState.Expired, underTest.getCacheState());

    // RefreshingOAuth2CredentialsInterceptor will show that the access token is stale.
    long expiration = start + HeaderCacheElement.TOKEN_EXPIRES_MS + 1;
    setTokenAndGetCacheElement(answer, expiration);
    // Check to make sure that the AccessToken was retrieved.
    Assert.assertEquals(CacheState.Stale, underTest.getCacheState());

    answer.reset();

    Future<HeaderCacheElement> firstFuture = refreshAndCheck();
    Future<HeaderCacheElement> secondFuture = refreshAndCheck();
    Future<HeaderCacheElement> thirdFuture = refreshAndCheck();

    Assert.assertEquals(firstFuture, secondFuture);
    Assert.assertEquals(secondFuture, thirdFuture);

    setTimeInMillieconds(expiration);
    setTokenAndGetCacheElement(answer, expiration + HeaderCacheElement.TOKEN_STALENESS_MS + 1);
    Assert.assertEquals(CacheState.Good, underTest.getCacheState());
  }

  private Future<HeaderCacheElement> refreshAndCheck() {
    Future<HeaderCacheElement> future = underTest.asyncRefresh();
    Assert.assertTrue(underTest.isRefreshing());
    return future;
  }

  private static class FutureAnswer<T> implements Answer<T> {
    SettableFuture<T> future = SettableFuture.create();

    public void reset() {
      future = SettableFuture.create();
    }

    @Override
    public T answer(InvocationOnMock invocation) throws Throwable {
      return future.get();
    }

    public void set(T accessToken) {
      future.set(accessToken);
    }
  }

  private void setTokenAndGetCacheElement(FutureAnswer<AccessToken> answer, long expiration)
      throws Exception {
    Future<HeaderCacheElement> cacheFuture = refreshAndCheck();
    answer.set(new AccessToken("hi", new Date(expiration)));

    // Wait for no more than a second to make sure that the call to underTest.syncRefresh()
    // completes properly.  If a second passes without syncRefresh() completing, future.get(..)
    // will throw a TimeoutException.
    cacheFuture.get(1, TimeUnit.SECONDS);

    Assert.assertFalse(underTest.isRefreshing());
  }

  private void initialize(ExecutorService executor, long expiration)
      throws IOException, InterruptedException, ExecutionException {
    Mockito.when(credentials.refreshAccessToken())
        .thenReturn(new AccessToken("hi", new Date(expiration)));
    underTest = new RefreshingOAuth2CredentialsInterceptor(executor, credentials);
    underTest.syncRefresh();
  }
}
