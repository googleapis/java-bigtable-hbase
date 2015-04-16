package com.google.cloud.bigtable.hbase;

import com.google.bigtable.repackaged.com.google.common.util.concurrent.ListenableFuture;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BigtableConnection;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  @Mock
  BatchExecutor executor;

  private BigtableBufferedMutator underTest;


  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    setup();
  }

  private void setup() {
    setup(new BufferedMutator.ExceptionListener() {
      @Override
      public void onException(RetriesExhaustedWithDetailsException exception,
          BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
        throw exception;
      }
    });
  }

  private void setup(ExceptionListener listener) {
    underTest = new BigtableBufferedMutator(executor,
        BigtableConnection.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_DEFAULT,
        listener,
        null,
        BigtableConnection.MAX_INFLIGHT_RPCS_DEFAULT,
        TableName.valueOf("TABLE"));
  }

  @Test
  public void testMutation() throws IOException {
    Mockito.when(executor.issueRequest(Matchers.any(Row.class))).thenReturn(
        Mockito.mock(ListenableFuture.class));
    underTest.mutate(new Put(new byte[1]));
    Mockito.verify(executor, Mockito.times(1)).issueRequest(Matchers.any(Row.class));
    Long id = underTest.sizeManager.pendingOperationsWithSize.keySet().iterator().next();
    underTest.sizeManager.operationComplete(id);
    Assert.assertTrue(underTest.sizeManager.pendingOperationsWithSize.isEmpty());
  }

  @Test
  public void testException() {
    underTest.hasExceptions.set(true);
    underTest.globalExceptions.add(
        new BigtableBufferedMutator.MutationException(null, new Exception()));
    
    try {
      underTest.handleExceptions();
      Assert.fail("expected RetriesExhaustedWithDetailsException");
    } catch (RetriesExhaustedWithDetailsException expected) {
      // Expected
    }
  }
}
