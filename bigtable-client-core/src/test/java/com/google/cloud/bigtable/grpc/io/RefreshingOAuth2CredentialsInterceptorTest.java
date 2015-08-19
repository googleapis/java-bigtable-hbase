/*
 * Copyright 2015 Google Inc. All Rights Reserved. Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.google.cloud.bigtable.grpc.io;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

import com.google.api.client.util.Clock;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigtable.grpc.io.RefreshingOAuth2CredentialsInterceptor.CacheState;
import com.google.cloud.bigtable.grpc.io.RefreshingOAuth2CredentialsInterceptor.HeaderCacheElement;

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
  private OAuth2Credentials credentials;

  @Before
  public void setupMocks() {
    MockitoAnnotations.initMocks(this);
    setTime(0L);
  }

  private void setTime(final long time) {
    RefreshingOAuth2CredentialsInterceptor.clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return time;
      }
    };
  }

  @Test
  public void testSyncRefresh() throws IOException {
    initialize(HeaderCacheElement.TOKEN_STALENESS_MS + 1);
    Assert.assertEquals(CacheState.Good,
      RefreshingOAuth2CredentialsInterceptor.getCacheState(underTest.headerCache.get()));
  }

  @Test
  public void testStaleAndExpired() throws IOException {
    int expiration = HeaderCacheElement.TOKEN_STALENESS_MS + 1;
    initialize(expiration);
    Assert.assertEquals(CacheState.Good,
      RefreshingOAuth2CredentialsInterceptor.getCacheState(underTest.headerCache.get()));
    setTime(2L);
    Assert.assertEquals(CacheState.Stale,
      RefreshingOAuth2CredentialsInterceptor.getCacheState(underTest.headerCache.get()));
    long expiredStaleDiff =
        HeaderCacheElement.TOKEN_STALENESS_MS - HeaderCacheElement.TOKEN_EXPIRES_MS;
    setTime(2L + expiredStaleDiff);
    Assert.assertEquals(CacheState.Expired,
      RefreshingOAuth2CredentialsInterceptor.getCacheState(underTest.headerCache.get()));
  }

  @Test
  /**
   * Test that checks that concurrent requests to RefreshingOAuth2CredentialsInterceptor refresh
   * logic doesn't cause hanging behavior.  Specifically, when an Expired condition occurs it 
   * triggers a call to syncRefresh() which potentially waits for refresh that was initiated
   * from another thread either through syncRefresh() or asyncRefresh().  This test case simulates
   * that condition. 
   */
  public void testRefreshDoesntHang() throws Exception,
      TimeoutException {
    // Assume that the user starts at this time... it's an arbitrarily big number which will
    // assure that subtracting HeaderCacheElement.TOKEN_STALENESS_MS and TOKEN_EXPIRES_MS will not 
    // be negative.
    long start = HeaderCacheElement.TOKEN_STALENESS_MS * 10;
    setTime(start);
    
    // RefreshingOAuth2CredentialsInterceptor will show that the access token is stale.
    final long expiration = start + HeaderCacheElement.TOKEN_EXPIRES_MS + 1;
    
    // Create a mechanism that will allow us to control when the accessToken is returned.  
    // credentials.refreshAccessToken() will get called asynchronously and will wait until the 
    // lock is notified before returning.  That will allow us to set up multiple concurrent calls
    final Object lock = new String("");
    Mockito.when(credentials.refreshAccessToken()).thenAnswer(new Answer<AccessToken>() {
      @Override
      public AccessToken answer(InvocationOnMock invocation) throws Throwable {
        synchronized (lock) {
          lock.wait();
        }
        return new AccessToken("", new Date(expiration));
      }
    });

    // Force a synchronous refresh.  This ought to wait until a refresh happening in another thread
    // completes.
    Callable<Void> syncRefreshCallable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        underTest.syncRefresh();
        return null;
      }
    };

    underTest = new RefreshingOAuth2CredentialsInterceptor(executorService, credentials);

    // At this point, the access token wasn't retrieved yet. The
    // RefreshingOAuth2CredentialsInterceptor considers null to be Expired.
    Assert.assertEquals(CacheState.Expired,
      RefreshingOAuth2CredentialsInterceptor.getCacheState(underTest.headerCache.get()));

    Future<Void> future = executorService.submit(syncRefreshCallable);
    
    // let the Thread running syncRefreshCallable() have a turn so that it can initiate the call
    // to refreshAccessToken().
    Thread.yield();
    synchronized(lock) {
      lock.notifyAll();
    }

    // Try to get the access token, which should be calculated at this point.  There's
    // a possibility that some hanging occurs in the test code.  If the operation times out
    // so timeout after 1 second, this will throw a TimeoutException.
    future.get(1, TimeUnit.SECONDS);

    // Check to make sure that the AccessToken was retrieved.
    Assert.assertEquals(CacheState.Stale,
      RefreshingOAuth2CredentialsInterceptor.getCacheState(underTest.headerCache.get()));

    // Check to make sure we're no longer refreshing.
    Assert.assertFalse(underTest.isRefreshing.get());

    // Kick off a couple of asynchronous refreshes. Kicking off more than one shouldn't be
    // necessary, but also should not be harmful, since there are likely to be multiple concurrent
    // requests that call asyncRefresh() when the token turns stale.
    underTest.asyncRefresh();
    underTest.asyncRefresh();
    underTest.asyncRefresh();

    future = executorService.submit(syncRefreshCallable);
    // Let the asyncRefreshes do their thing.
    Thread.yield();
    
    // There should be a single thread kicked off by the underTest.asyncRefresh() calls about
    // actually doing a refresh at this point; the other ones will have see that a refresh is in
    // progress and finish the invocation of the Thread without performing a refres().. Make sure
    // that at least 1 refresh process is in progress.
    Assert.assertTrue(underTest.isRefreshing.get());

    synchronized(lock) {
      // Release the lock so that all of the async refreshing can complete.
      lock.notifyAll();
    }
    // Wait for no more than a second to make sure that the call to underTest.syncRefresh()
    // completes properly.  If a second passes without syncRefresh() completing, future.get(..)
    // will throw a TimeoutException.
    future.get(1, TimeUnit.SECONDS);
    Assert.assertFalse(underTest.isRefreshing.get());
  }

  private void initialize(long expiration) throws IOException {
    Mockito.when(credentials.refreshAccessToken()).thenReturn(
      new AccessToken("", new Date(expiration)));
    underTest = new RefreshingOAuth2CredentialsInterceptor(executorService, credentials);
    Assert.assertTrue(underTest.doRefresh());
  }
}
