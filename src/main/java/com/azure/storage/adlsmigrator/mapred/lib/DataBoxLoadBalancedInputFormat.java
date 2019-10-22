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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.azure.storage.adlsmigrator.CopyListingFileStatus;
import com.azure.storage.adlsmigrator.AdlsMigratorConstants;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions;
import com.azure.storage.adlsmigrator.IdentityMap;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;
import java.util.HashSet;

/**
 * DataBoxLoadBalancedInputFormat extends the InputFormat class, to produce
 * input-splits for AdlsMigrator.
 * The splits are calculated by first assigning each file in the listing to 
 * a Data Box, taking into account the size of each Box. Each Box's content
 * is then evenly distributed across the tasks to provide uniform throughput
 * load on each Data Box.
 */
public class DataBoxLoadBalancedInputFormat extends InputFormat<Text, CopyListingFileStatus> {

  private static final Log LOG = LogFactory.getLog(DataBoxLoadBalancedInputFormat.class);

  /**
   * Implementation of InputFormat::getSplits(). Returns a list of InputSplits,
   * such that the the size of files are evenly distributed across the configured
   * Data Boxes.
   * @param context JobContext for the job.
   * @return The list of uniformly-distributed input-splits.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext context)
                      throws IOException, InterruptedException {
    return getSplits(context.getConfiguration());
  }

  static class AllocatedDataBox extends AdlsMigratorOptions.DataBoxItem {
    static class AllocatedSplit {
      public long spaceAvailable;
      private DataBoxSplit split;
      private AllocatedDataBox dataBox;
      private int numRecords = 0;

      public AllocatedSplit(AllocatedDataBox dataBox, long splitSize, DataBoxSplit split) {
        this.spaceAvailable = splitSize;
        this.split = split;
        this.dataBox = dataBox;
      }

      public void assignFile(Text filePath, CopyListingFileStatus fileStatus) throws IOException {
        split.write(filePath, fileStatus);
        spaceAvailable -= fileStatus.getSizeToCopy();
        dataBox.spaceAvailable -= fileStatus.getSizeToCopy();
        numRecords++;
      }

      public int getNumRecords() {
        return numRecords;
      }
    }
    public long spaceAvailable;
    public AllocatedSplit[] splits;
  }

  // The pro-rating is not linear, so we use a lookup based on known throughput capabilities of each Data Box size.
  // Note: this array must be declared in ascending order of size.
  static final TreeMap<Long, Integer> dataBoxSizeFactors = new TreeMap<Long, Integer>() {{
    put(100L * 2 ^ 40, 1);
    put(1L * 2 ^ 50, 4);
    put(Long.MAX_VALUE, 4);
  }};

  private List<InputSplit> getSplits(Configuration configuration) throws IOException {
    LOG.debug("Calculating mapper splits for input files");

    List<InputSplit> splits;
    SequenceFile.Reader reader = null;
    try {
      DataBoxContext ctx = new DataBoxContext(configuration);
      String targetContainer = configuration.get(AdlsMigratorConstants.CONF_LABEL_TARGET_CONTAINER);
      AllocatedDataBox[] dataBoxAllocations = AdlsMigratorUtils.parseDataBoxesFromJson(
        configuration.get(AdlsMigratorConstants.CONF_LABEL_DATABOX_CONFIG),
        AllocatedDataBox[].class);
      // We sort the configured Data Boxes here from smallest to largest. This is due we get better
      // throughput::capacity for smaller Data Boxes.
      Arrays.sort(dataBoxAllocations, 
        new Comparator<AllocatedDataBox>() {
          @Override
          public int compare(AllocatedDataBox lhs, AllocatedDataBox rhs) {
            return Long.compare(lhs.getSizeInBytes(), rhs.getSizeInBytes());
          }
        });
      LOG.debug("Sorted Data Boxes: " + Arrays.toString(dataBoxAllocations));
      // Allocate sufficient splits for the number of maps assigned to each Data Box.
      // The CONF_LABEL_NUM_TASKS_PER_DATABOX configuration value is assigned based on the standard Data Box size of 100TB.
      // It is adjusted up or down based on the configured size of the Data Box.
      int numMapsPerStdDataBox = AdlsMigratorUtils.getInt(configuration, AdlsMigratorConstants.CONF_LABEL_NUM_TASKS_PER_DATABOX);
      int maxSplits = AdlsMigratorUtils.getInt(configuration, JobContext.NUM_MAPS);
      int actualNumSplits = maxSplits;
      int idealNumSplits = 0;
      for (AllocatedDataBox dataBox : dataBoxAllocations) {
        idealNumSplits += numMapsPerStdDataBox * dataBoxSizeFactors.ceilingEntry(dataBox.getSizeInBytes()).getValue();
      }
      LOG.debug("Calculated " + idealNumSplits + " splits. Configured max: " + maxSplits);
      double adjustmentFactor = 1.0;
      if (idealNumSplits <= maxSplits) {
        AdlsMigratorUtils.publish(configuration, JobContext.NUM_MAPS, actualNumSplits = idealNumSplits);
      } else {
        adjustmentFactor = (double)maxSplits / idealNumSplits;
      }
      // Allocate & initialize the splits to each Data Box
      int splitCounter = 0;
      for (AllocatedDataBox dataBox : dataBoxAllocations) {
        int numSplitsForDataBox = (int)((double)numMapsPerStdDataBox 
                                  * dataBoxSizeFactors.ceilingEntry(dataBox.getSizeInBytes()).getValue() 
                                  * adjustmentFactor);
        numSplitsForDataBox = Math.max(numSplitsForDataBox, 1);
        LOG.debug("Data Box: " + dataBox.getDataBoxDns() + ", Number of splits: " + numSplitsForDataBox);
        dataBox.spaceAvailable = dataBox.getSizeInBytes();
        dataBox.splits = new AllocatedDataBox.AllocatedSplit[numSplitsForDataBox];
        for (int splitIdx = 0; splitIdx < numSplitsForDataBox; splitIdx++) {
          dataBox.splits[splitIdx] = new AllocatedDataBox.AllocatedSplit(dataBox, 
                                                                         dataBox.getSizeInBytes() / numSplitsForDataBox,
                                                                         ctx.createSplitForWrite(splitCounter++, 
                                                                                                 dataBox.getTargetPath(targetContainer)));
        }
      }
      // Assign the source files to splits. This is deliberately a trivial assignment algo because we want the smaller data boxes to 
      // be assigned maximum load and there's no benefit to a more optimized placement algo.
      Text srcRelPath = new Text();
      CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
      List<String> skippedFiles = new ArrayList<String>();
      HashSet<IdentityMap> identities = new HashSet<IdentityMap>();
      
      reader = getListingFileReader(configuration);
      while (reader.next(srcRelPath, srcFileStatus)) {
        boolean dataBoxAssigned = false;
        for (AllocatedDataBox dataBox : dataBoxAllocations) {
          long fileSize = srcFileStatus.getSizeToCopy();
          if (fileSize <= dataBox.spaceAvailable) {
            // Same algo to assign splits
            boolean splitAssigned = false;
            for (AllocatedDataBox.AllocatedSplit split : dataBox.splits) {
              if (fileSize < split.spaceAvailable) {
                LOG.debug("Assigning source file: " + srcRelPath 
                  + ", size: " + fileSize
                  + " to split: " + split.split.getSplitName());
                split.assignFile(srcRelPath, srcFileStatus);
                splitAssigned = true;
                break;
              }
            }
            if (!splitAssigned) {
              // This situation can occur for single files that are bigger than [box size] / [num splits]. To handle this case
              // select the split with the most available space & over-provision it.
              AllocatedDataBox.AllocatedSplit split = Collections.max(Arrays.asList(dataBox.splits), 
                              new Comparator<AllocatedDataBox.AllocatedSplit>() {
                                @Override
                                public int compare(AllocatedDataBox.AllocatedSplit lhs, AllocatedDataBox.AllocatedSplit rhs) {
                                  return Long.compare(lhs.spaceAvailable, rhs.spaceAvailable);
                                }
                              });
              split.assignFile(srcRelPath, srcFileStatus);
            }
            dataBoxAssigned = true;
            break;
          }
        }
        if (!dataBoxAssigned) {
          skippedFiles.add(srcFileStatus.getPath().toString());
          LOG.warn("The file: " + srcFileStatus.getPath()
                  + ", size: " + srcFileStatus.getSizeToCopy()
                  + " could not be placed on a Data Box.");
        }
        // Add the file's identites to our map
        identities.add(new IdentityMap(srcFileStatus.getOwner()));
        LOG.debug("Src File Status: " + srcFileStatus.toString());
        identities.add(new IdentityMap(srcFileStatus.getGroup()));
        for (AclEntry entry : srcFileStatus.getAclEntries()) {
          if (StringUtils.isNotBlank(entry.getName())) {
            identities.add(new IdentityMap(entry.getName()));
          }
        }
      }
      // If we have source files that couldn't be placed on a Data Box & we've specified a logfile, write it out now
      AdlsMigratorUtils.writeSkippedFiles(configuration, skippedFiles, "%s - cannot locate on Data Box\n");
      // Dump the identity map (if configured)
      try {
        IdentityMap.saveToJsonFile(identities, IdentityMap.getIdentitiesMapFile(configuration), configuration);
      } catch (IOException ex) {
        // Log and swallow the exception
        LOG.warn("Failed to save identities map. Details: " + ex);
      }
      // Dump out all of the splits for the AM
      splits = new ArrayList<InputSplit>(actualNumSplits);
      for (AllocatedDataBox dataBox : dataBoxAllocations) {
        for (AllocatedDataBox.AllocatedSplit split : dataBox.splits) {
          LOG.debug("Split: " + split.split.getPath() 
            + " for Data Box: " + split.dataBox.getDataBoxDns() 
            + ", Number of records: " + split.numRecords 
            + ", Space remaining: " + split.spaceAvailable);
          split.split.close();
          splits.add(new DataBoxSplit.TaskSplit(split.split.getPath(), 
                                                dataBox.getTargetPath(targetContainer), 
                                                Long.MAX_VALUE));
        }
      }
    } finally {
      IOUtils.closeStream(reader);
    }

    return splits;
  }

  private static Path getListingFilePath(Configuration configuration) {
    final String listingFilePathString =
            configuration.get(AdlsMigratorConstants.CONF_LABEL_LISTING_FILE_PATH, "");

    assert !listingFilePathString.equals("")
              : "Couldn't find listing file. Invalid input.";
    return new Path(listingFilePathString);
  }

  private SequenceFile.Reader getListingFileReader(Configuration configuration) {

    final Path listingFilePath = getListingFilePath(configuration);
    try {
      final FileSystem fileSystem = listingFilePath.getFileSystem(configuration);
      if (!fileSystem.exists(listingFilePath))
        throw new IllegalArgumentException("Listing file doesn't exist at: "
                                           + listingFilePath);

      return new SequenceFile.Reader(configuration,
                                     SequenceFile.Reader.file(listingFilePath));
    }
    catch (IOException exception) {
      LOG.error("Couldn't find listing file at: " + listingFilePath, exception);
      throw new IllegalArgumentException("Couldn't find listing-file at: "
                                         + listingFilePath, exception);
    }
  }

  /**
   * Implementation of InputFormat::createRecordReader().
   * @param split The split for which the RecordReader is sought.
   * @param context The context of the current task-attempt.
   * @return A SequenceFileRecordReader instance, (since the copy-listing is a
   * simple sequence-file.)
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader<Text, CopyListingFileStatus> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    //return new SequenceFileRecordReader<Text, CopyListingFileStatus>();
    return new DataBoxRecordReader(new DataBoxContext(context.getConfiguration()));
  }
}

