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

package com.google.cloud.dataflow.examples;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * An example that writes ngrams to BigTable.
 *
 * <p>Concepts:
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting a PCollection
 *   4. Writing data to Cloud Storage as text files
 * </pre>
 *
 * <p>To execute this pipeline, first edit the code to set your project ID, the staging
 * location, and the output location. The specified GCS bucket(s) must already exist.
 *
 * <p>Then, run the pipeline as described in the README. It will be deployed and run using the
 * Dataflow service. No args are required to run the pipeline. You can see the results in your
 * output bucket in the GCS browser.
 */
public class WriteBigTable {

  public static void main(String[] args) {
    // Create a DataflowPipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the associated Cloud Platform project and the location
    // in Google Cloud Storage to stage files.
    DataflowPipelineOptions options = PipelineOptionsFactory.create()
      .as(DataflowPipelineOptions.class);
    options.setRunner(BlockingDataflowPipelineRunner.class);
    // CHANGE 1/3: Your project ID is required in order to run your pipeline on the Google Cloud.
    options.setProject("autocomplete-168915");
    // CHANGE 2/3: Your Google Cloud Storage path is required for staging local files.
    options.setStagingLocation("gs://pradipta-catalog/min-wc-staging");

    CloudBigtableOptions cbtoptions =  PipelineOptionsFactory.create().as(CloudBigtableOptions.class);

    cbtoptions.setBigtableProjectId("autocomplete-168915");
    cbtoptions.setBigtableTableId("ngram");
    cbtoptions.setBigtableInstanceId("catalog");

    // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
    CloudBigtableTableConfiguration cbtconfig =
        CloudBigtableTableConfiguration.fromCBTOptions(cbtoptions);

    // Create the Pipeline object with the options we defined above.
    Pipeline p = Pipeline.create(options);

    CloudBigtableIO.initializeForWrite(p);

    // Apply the pipeline's transforms.

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text - product name.
    p.apply(TextIO.Read.from("gs://pradipta-catalog/ngrams.txt"))
     // Concept #2: Apply a ParDo transform to our PCollection of text lines. 
     .apply(ParDo.named("ExtractWords").of(new DoFn<String, Mutation>() {
                       @Override
                       public void processElement(ProcessContext c) {
                             //Write to BigTable - table:ngram, columnfamily: cf
                             //first phrase, second phrase, count
                             //word[0], cf:word[1], word[2] 
                             String[] row = c.element().split("\\s");
                             if (!row[0].isEmpty() && !row[1].isEmpty() && !row[2].isEmpty()) { 
                                Put put = new Put(Bytes.toBytes(row[0]));
                                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(row[1]), Bytes.toBytes(row[2]));
                                c.output(put);
                             }
                       }
                     }))

    .apply(CloudBigtableIO.writeToTable(cbtconfig));    // Run the pipeline.
    p.run();
  }
}
