/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.beam.sequencefiles;

import com.google.cloud.bigtable.beam.hbasesnapshots.ImportJobFromHbaseSnapshot;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import java.nio.charset.StandardCharsets;


public class TenantIdCountFn {
    /**
     * extract prefixes from hbase row keys
     */
    public static class ExtractTenantIdsFn extends DoFn<KV<ImmutableBytesWritable, Result>, String>  {
        @ProcessElement
        public void processElement(ProcessContext context) {

            ImportJobFromHbaseSnapshot.ImportOptions.CustomPipelineOptions customOpts =
                    context.getPipelineOptions().as(ImportJobFromHbaseSnapshot.ImportOptions.CustomPipelineOptions.class);

            KV<ImmutableBytesWritable, Result> kv = context.element();
            String tenantId = new String(kv.getKey().get(), StandardCharsets.UTF_8).split(customOpts.getDelimiter().toString())[0];
            context.output(tenantId);
        }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return "Tenant Id: " + input.getKey() + ", Count: " + input.getValue();
        }
    }
}