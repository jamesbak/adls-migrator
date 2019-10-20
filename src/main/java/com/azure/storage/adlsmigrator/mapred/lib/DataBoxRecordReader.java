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

package com.azure.storage.adlsmigrator.mapred.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;
import com.azure.storage.adlsmigrator.AdlsMigratorConstants;
import com.azure.storage.adlsmigrator.CopyListingFileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The DataBoxRecordReader is used in conjunction with the DynamicInputFormat
 * to implement the "Worker pattern" for AdlsMigrator.
 * The DataBoxRecordReader is responsible for:
 * 1. Presenting the contents of each split to AdlsMigrator's mapper.
 * 2. Acquiring a new split when the current split has been completely consumed,
 *    transparently.
 */
public class DataBoxRecordReader extends RecordReader<Text, CopyListingFileStatus> {
  private static final Log LOG = LogFactory.getLog(DataBoxRecordReader.class);
  private TaskAttemptContext taskAttemptContext;
  private DataBoxSplit split;
  private TaskID taskId;

  // Data required for progress indication.
  private int numRecordsProcessedByThisMap = 0;
  private DataBoxContext ctx;

  DataBoxRecordReader(DataBoxContext ctx) {
    this.ctx = ctx;
  }

  /**
   * Implementation for RecordReader::initialize(). Initializes the internal
   * RecordReader to read from splits.
   * @param inputSplit The InputSplit for the map. Ignored entirely.
   * @param taskAttemptContext The AttemptContext.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext)
                         throws IOException, InterruptedException {
    this.taskAttemptContext = taskAttemptContext;
    taskId = taskAttemptContext.getTaskAttemptID().getTaskID();
    split = ctx.acquire(this.taskAttemptContext, (DataBoxSplit.TaskSplit)inputSplit);
    LOG.debug("Starting split input reader for task: " + taskAttemptContext.getTaskAttemptID().getTaskID()
            + ", using split: " + split.getSplitName());
  }

  /**
   * Implementation of RecordReader::nextValue().
   * Reads the contents of the current split and returns them. When a split has
   * been completely exhausted, an new split is acquired and read,
   * transparently.
   * @return True, if the nextValue() could be traversed to. False, otherwise.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public boolean nextKeyValue()
      throws IOException, InterruptedException {

    if (split == null) {
      if (LOG.isDebugEnabled())
        LOG.debug(taskId + ": RecordReader is null. No records to be read.");
      return false;
    }
    LOG.debug("Current SequenceFile progress for split: " + split.getSplitName() + ", " + split.getReader().getProgress());
    if (split.getReader().nextKeyValue()) {
      ++numRecordsProcessedByThisMap;
      return true;
    }
    LOG.debug("Finished task input reader for split: " + split.getSplitName() 
            + ". Processed: " + numRecordsProcessedByThisMap + " records.");
    return false;
  }

  /**
   * Implementation of RecordReader::getCurrentKey().
   * @return The key of the current record. (i.e. the source-path.)
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public Text getCurrentKey()
      throws IOException, InterruptedException {
    return split.getReader().getCurrentKey();
  }

  /**
   * Implementation of RecordReader::getCurrentValue().
   * @return The value of the current record. (i.e. the target-path.)
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public CopyListingFileStatus getCurrentValue()
      throws IOException, InterruptedException {
    return split.getReader().getCurrentValue();
  }

  /**
   * Implementation of RecordReader::getProgress().
   * @return A fraction [0.0,1.0] indicating the progress of a AdlsMigrator mapper.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public float getProgress()
      throws IOException, InterruptedException {
    return split.getReader().getProgress();
  }

  /**
   * Implementation of RecordReader::close().
   * Closes the RecordReader.
   * @throws IOException
   */
  @Override
  public void close()
      throws IOException {
    if (split != null)
        split.close();
  }
}
