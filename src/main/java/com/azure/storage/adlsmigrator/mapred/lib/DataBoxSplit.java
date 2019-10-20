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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import com.azure.storage.adlsmigrator.CopyListingFileStatus;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;
import com.google.common.io.Files;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;

/**
 * The DataBoxSplit represents a single split of work, when used in
 * conjunction with the DynamicInputFormat and the DynamicRecordReader.
 * The records in the DynamicInputFormat's input-file are split across various
 * DataBoxSplits. Each one is claimed and processed in an iteration of
 * a dynamic-mapper. When a DataBoxSplit has been exhausted, the faster
 * mapper may claim another and process it, until there are no more to be
 * consumed.
 */
public class DataBoxSplit {
  private static Log LOG = LogFactory.getLog(DataBoxSplit.class);
  private Path splitFilePath;
  private Path targetDataBox;
  private String splitName;
  private SequenceFileRecordReader<Text, CopyListingFileStatus> reader;
  private SequenceFile.Writer writer;
  private DataBoxContext context;

  DataBoxSplit(String splitId, DataBoxContext context, Path targetDataBox) throws IOException {
    this.context = context;
    splitFilePath = new Path(context.getSplitsRootPath(), context.getSplitFilePrefix() + splitId);
    splitName = splitFilePath.getName();
    this.targetDataBox = targetDataBox;
    openForWrite();
  }

  private void openForWrite() throws IOException {
    writer = SequenceFile.createWriter(context.getFs(), context.getConfiguration(),
            splitFilePath, Text.class, CopyListingFileStatus.class, SequenceFile.CompressionType.NONE);
    LOG.debug("Initialized split writer: " + splitName);
  }

  /**
   * Method to write records into a split.
   * @param key Key from the listing file.
   * @param value Corresponding value from the listing file.
   * @throws IOException Exception on failure to write to the file.
   */
  public void write(Text key, CopyListingFileStatus value) throws IOException {
    writer.append(key, value);
  }

  /**
   * Closes streams opened to the split-file.
   */
  public void close() {
    IOUtils.cleanup(LOG, reader, writer);
  }

  public DataBoxSplit(Path splitFilePath, TaskSplit inputSplit, TaskAttemptContext taskAttemptContext, DataBoxContext context) 
      throws IOException, InterruptedException {

    this.splitFilePath = splitFilePath;
    this.targetDataBox = inputSplit.getDataBoxBaseUri();
    this.context = context;
    splitName = splitFilePath.getName();
    openForRead(taskAttemptContext, inputSplit);
  }

  private void openForRead(TaskAttemptContext taskAttemptContext, TaskSplit inputSplit)
          throws IOException, InterruptedException {

    reader = new SequenceFileRecordReader<Text, CopyListingFileStatus>();
    reader.initialize(new FileSplit(inputSplit.getPath(), 
                                    0,
                                    AdlsMigratorUtils.getFileSize(inputSplit.getPath(), taskAttemptContext.getConfiguration()), 
                                    null),
                        taskAttemptContext);
    LOG.debug("Initialized split reader: " + splitName);
  }

  /**
   * Method to be called to relinquish an acquired split. All streams open to
   * the split are closed, and the split-file is deleted.
   * @throws IOException Exception thrown on failure to release (i.e. delete)
   * the split file.
   */
  public void release() throws IOException {
    close();
    if (!context.getFs().delete(splitFilePath, false)) {
      LOG.error("Unable to release split at path: " + splitFilePath);
      throw new IOException("Unable to release split at path: " + splitFilePath);
    }
  }

  /**
   * Getter for the split-file's path, on HDFS.
   * @return The qualified path to the split-file.
   */
  public Path getPath() {
    return splitFilePath;
  }

  public Path getTargetDataBox() {
    return targetDataBox;
  }

  public String getSplitName() {
    return splitName;
  }

  /**
   * Getter for the record-reader, opened to the split-file.
   * @return Opened Sequence-file reader.
   */
  public SequenceFileRecordReader<Text, CopyListingFileStatus> getReader() {
    assert reader != null : "Reader un-initialized!";
    return reader;
  }

  /** 
   * Our splitter needs to know where its writing the data to
   */
  public static class TaskSplit extends FileSplit {
    private Path dataBoxBaseUri;

    public TaskSplit() {
    }
    
    public TaskSplit(Path file, Path dataBoxBaseUri, long length) {
      super(file, 0, length, null);
      this.dataBoxBaseUri = dataBoxBaseUri;
    }

    public Path getDataBoxBaseUri() {
      return dataBoxBaseUri;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeString(out, dataBoxBaseUri.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      dataBoxBaseUri = new Path(Text.readString(in));
    }    
  }
}
