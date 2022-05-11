/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

/**
 * MirroringClient uses asynchronous tasks to perform operations on the secondary database.
 * Operations on the secondary database can take longer than operation on primary and thus we might
 * have a long tail of scheduled or in-flight operations (the length of the tail is limited by the
 * {@link com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController}). For this
 * reason we have to count scheduled requests and prevent closing resources they use (Tables,
 * ResultScanners, Connections, etc.) before all requests using that resource are completed. To
 * achieve this we are using manual reference counting - each scheduled asynchronous operation
 * increments reference counter of all resources it is using (e.g. scheduled {@link Table#get(Get)}
 * increments reference counter of the Table and Connection, {@link ResultScanner#next()} increments
 * ResultScanner's, Table's and Connection's reference counters).
 *
 * <p>There are other, less complicated approaches possible (for example we could have only a single
 * reference counter for Connection object, or a bit simpler hierarchical approach in which every
 * level of the hierarchy (Connection, Table, ResultScanner) counts its requests and only increments
 * (decrements) reference counter of their parent level when it is created (closed)) but they
 * wouldn't allow us to achieve two goals of our design:
 *
 * <ul>
 *   <li>If the user calls close() on a Mirroring* object, we should also call close() on underlying
 *       secondary object, but only after all requests using that object have finished.
 *   <li>If the user doesn't call close() on a Mirroring* object (because some users might not close
 *       their Tables) we shouldn't close underlying secondary as well, but this shouldn't prevent
 *       closing its parent Connection object if the user closes it.
 * </ul>
 *
 * The second point is critical for us because {@link MirroringConnection#close()} call blocks until
 * all requests using that Connection are finished. Counting created Tables (which would happen in
 * the second example of simpler reference counting scheme described above) would cause deadlocks.
 */
public class HierarchicalReferenceCounter implements ReferenceCounter {

  /** Counter of asynchronous requests from current hierarchy level. */
  public final ListenableReferenceCounter current;
  /** Counter of asynchronous requests from hierarchy above the current level. */
  public final ReferenceCounter parent;

  public HierarchicalReferenceCounter(ReferenceCounter parentReferenceCounter) {
    this.current = new ListenableReferenceCounter();
    this.parent = parentReferenceCounter;
  }

  @Override
  public void incrementReferenceCount() {
    this.current.incrementReferenceCount();
    this.parent.incrementReferenceCount();
  }

  @Override
  public void decrementReferenceCount() {
    this.parent.decrementReferenceCount();
    this.current.decrementReferenceCount();
  }
}
