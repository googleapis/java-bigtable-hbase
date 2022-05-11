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
package com.google.cloud.bigtable.mirroring.core.utils.flowcontrol;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.MirroringOptions;
import java.util.List;
import org.apache.hadoop.hbase.client.Table;

/**
 * Implementation of {@link FlowControlStrategy} that keeps track of the number of entries to be
 * fetched from secondary database or verified. A single request to the database can increase the
 * counter by more than one, for example when calling {@link Table#get(List)}, the number of
 * elements in list is counted.
 *
 * <p>If the number of scheduled entries reaches {@link Ledger#outstandingRequestsThreshold} then
 * {@link #tryAcquireResource(RequestResourcesDescription)} will return false and reservation
 * request from {@link FlowController#asyncRequestResource(RequestResourcesDescription)} won't be
 * resolved immediately.
 *
 * <p>Requests that want to acquire more tickets than {@link Ledger#outstandingRequestsThreshold}
 * are allowed to perform their actions only if all other resources were released. Along with
 * FlowController's guarantees of waking requests in order of arrival it guarantees that an
 * over-sized request will be the only running request, without any other running concurrently. It
 * also means that:
 *
 * <ul>
 *   <li>over-sized requests are executed after all the requests that have arrived earlier have
 *       finished, and
 *   <li>any request that arrives after the over-sized request will be allowed to acquire resources
 *       after the over-sized request have finished.
 * </ul>
 *
 * <p>For those reasons such requests can greatly reduce concurrency and the limit should be chosen
 * with care.
 */
@InternalApi("For internal usage only")
public class RequestCountingFlowControlStrategy extends SingleQueueFlowControlStrategy {
  public RequestCountingFlowControlStrategy(
      int outstandingRequestsThreshold, int usedBytesThreshold) {
    super(new Ledger(outstandingRequestsThreshold, usedBytesThreshold));
  }

  public RequestCountingFlowControlStrategy(MirroringOptions options) {
    this(options.flowControllerMaxOutstandingRequests, options.flowControllerMaxUsedBytes);
  }

  /** Not thread-safe, access is synchronized by {@link SingleQueueFlowControlStrategy}. */
  private static class Ledger implements SingleQueueFlowControlStrategy.Ledger {
    private final int usedBytesThreshold;
    private final int outstandingRequestsThreshold;

    private int primaryReadsAdvantage; // = completedPrimaryReads - completedSecondaryReads
    private int usedBytes;

    private Ledger(int outstandingRequestsThreshold, int usedBytesThreshold) {
      this.outstandingRequestsThreshold = outstandingRequestsThreshold;
      this.usedBytesThreshold = usedBytesThreshold;
      this.primaryReadsAdvantage = 0;
      this.usedBytes = 0;
    }

    public boolean canAcquireResource(RequestResourcesDescription requestResourcesDescription) {
      int neededEntries = requestResourcesDescription.numberOfResults;
      if (this.primaryReadsAdvantage == 0) {
        // Always allow at least one request into the flow controller, regardless of its size.
        return true;
      }
      return this.primaryReadsAdvantage + neededEntries <= this.outstandingRequestsThreshold
          && this.usedBytes + requestResourcesDescription.sizeInBytes <= this.usedBytesThreshold;
    }

    @Override
    public boolean tryAcquireResource(RequestResourcesDescription requestResourcesDescription) {
      if (this.canAcquireResource(requestResourcesDescription)) {
        this.primaryReadsAdvantage += requestResourcesDescription.numberOfResults;
        this.usedBytes += requestResourcesDescription.sizeInBytes;
        return true;
      }
      return false;
    }

    @Override
    public void accountReleasedResources(RequestResourcesDescription requestResourcesDescription) {
      this.primaryReadsAdvantage -= requestResourcesDescription.numberOfResults;
      this.usedBytes -= requestResourcesDescription.sizeInBytes;
    }
  }

  public static class Factory implements FlowControlStrategy.Factory {
    @Override
    public FlowControlStrategy create(MirroringOptions options) {
      return new RequestCountingFlowControlStrategy(options);
    }
  }
}
