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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.azure.storage.adlsmigrator.AdlsMigratorConstants;

import java.io.IOException;
import java.net.URI;

/**
 * Class to initialize the DynamicInputChunk invariants.
 */
class DataBoxContext {

  private static Log LOG = LogFactory.getLog(DataBoxContext.class);
  private Configuration configuration;
  private Path splitsRootPath = null;
  private String splitFilePrefix;
  private FileSystem fs;

  public DataBoxContext(Configuration config) throws IOException {
    this.configuration = config;
    Path listingFilePath = new Path(getListingFilePath(configuration));
    splitsRootPath = new Path(listingFilePath.getParent(), "splitsDir");
    fs = splitsRootPath.getFileSystem(configuration);
    splitFilePrefix = listingFilePath.getName() + ".split.";
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Path getSplitsRootPath() {
    return splitsRootPath;
  }

  public String getSplitFilePrefix() {
    return splitFilePrefix;
  }

  public FileSystem getFs() {
    return fs;
  }

  private static String getListingFilePath(Configuration configuration) {
    final String listingFileString = configuration.get(AdlsMigratorConstants.CONF_LABEL_LISTING_FILE_PATH, "");
    assert !listingFileString.equals("") : "Listing file not found.";
    return listingFileString;
  }

  public DataBoxSplit acquire(TaskAttemptContext taskAttemptContext, DataBoxSplit.TaskSplit inputSplit)
      throws IOException, InterruptedException {
    LOG.debug("Acquiring split for task: " + taskAttemptContext.getTaskAttemptID().toString() + ", " +
              "Listing file: " + inputSplit.getPath().toString() + ", " +
              "Target Data Box: " + inputSplit.getDataBoxBaseUri().toString());
    return new DataBoxSplit(inputSplit.getPath(), inputSplit, taskAttemptContext, this);
  }

  public DataBoxSplit createSplitForWrite(int splitId, Path targetDataBox) throws IOException {
    return createSplitForWrite(String.format("%05d", splitId), targetDataBox);
  }

  public DataBoxSplit createSplitForWrite(String splitId, Path targetDataBox) throws IOException {
    return new DataBoxSplit(splitId, this, targetDataBox);
  }
}
