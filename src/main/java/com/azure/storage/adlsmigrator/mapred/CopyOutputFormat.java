/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.azure.storage.adlsmigrator.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import com.azure.storage.adlsmigrator.AdlsMigratorConstants;

import java.io.IOException;

/**
 * The CopyOutputFormat is the Hadoop OutputFormat used in AdlsMigrator.
 * It sets up the Job's Configuration (in the Job-Context) with the settings
 * for the work-directory, final commit-directory, etc. It also sets the right
 * output-committer.
 * @param <K>
 * @param <V>
 */
public class CopyOutputFormat<K, V> extends TextOutputFormat<K, V> {

  /** {@inheritDoc} */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    return new CopyCommitter(getOutputPath(context), context);
  }
}
