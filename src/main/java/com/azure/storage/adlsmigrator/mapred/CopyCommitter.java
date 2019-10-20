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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import com.azure.storage.adlsmigrator.CopyListing;
import com.azure.storage.adlsmigrator.CopyListingFileStatus;
import com.azure.storage.adlsmigrator.AdlsMigratorConstants;
import com.azure.storage.adlsmigrator.AdlsMigratorOptionSwitch;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions.FileAttribute;
import com.azure.storage.adlsmigrator.GlobbedCopyListing;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

/**
 * The CopyCommitter class is AdlsMigrator's OutputCommitter implementation. It is
 * responsible for handling the completion/cleanup of the AdlsMigrator run.
 * Specifically, it does the following:
 *  1. Cleanup of the meta-folder (where AdlsMigrator maintains its file-list, etc.)
 *  2. Preservation of user/group/replication-factor on any directories that
 *     have been copied. (Files are taken care of in their map-tasks.)
 *  3. Atomic-move of data from the temporary work-folder to the final path
 *     (if atomic-commit was opted for).
 *  4. Deletion of files from the target that are missing at source (if opted for).
 *  5. Cleanup of any partially copied files, from previous, failed attempts.
 */
public class CopyCommitter extends FileOutputCommitter {
  private static final Log LOG = LogFactory.getLog(CopyCommitter.class);

  /**
   * Create a output committer
   *
   * @param outputPath the job's output path
   * @param context    the task's context
   * @throws IOException - Exception if any
   */
  public CopyCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  /** {@inheritDoc} */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    try {
      super.commitJob(jobContext);
    } finally {
      cleanup(jobContext.getConfiguration());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void abortJob(JobContext jobContext,
                       JobStatus.State state) throws IOException {
    try {
      super.abortJob(jobContext, state);
    } finally {
      cleanup(jobContext.getConfiguration());
    }
  }

  /**
   * Cleanup meta folder and other temporary files
   *
   * @param conf - Job Configuration
   */
  private void cleanup(Configuration conf) {
    Path metaFolder = new Path(conf.get(AdlsMigratorConstants.CONF_LABEL_META_FOLDER));
    try {
      FileSystem fs = metaFolder.getFileSystem(conf);
      LOG.info("Cleaning up temporary work folder: " + metaFolder);
      fs.delete(metaFolder, true);
    } catch (IOException ignore) {
      LOG.error("Exception encountered ", ignore);
    }
  }
}
