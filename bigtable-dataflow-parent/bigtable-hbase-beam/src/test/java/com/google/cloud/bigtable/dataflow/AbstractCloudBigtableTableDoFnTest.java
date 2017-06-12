/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.repackaged.io.grpc.Status;


@RunWith(JUnit4.class)
public class AbstractCloudBigtableTableDoFnTest {

  @Test
  public void testLogRetriesExhaustedWithDetailsException(){
    // Make sure that the logging doesn't have any exceptions. The details can be manually verified.
    // TODO: add a mock logger to confirm that the logging is correct.
    Logger log = LoggerFactory.getLogger(getClass());
    List<Throwable> exceptions = new ArrayList<>();
    List<Row> actions = new ArrayList<>();
    List<String> hostnameAndPort = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      exceptions.add(Status.CANCELLED.asException());
      actions.add(new Get(RandomStringUtils.randomAlphabetic(8).getBytes()));
      hostnameAndPort.add("");
    }
    AbstractCloudBigtableTableDoFn.logRetriesExhaustedWithDetailsException(log, null,
      new RetriesExhaustedWithDetailsException(exceptions, actions, hostnameAndPort));
  }
}
