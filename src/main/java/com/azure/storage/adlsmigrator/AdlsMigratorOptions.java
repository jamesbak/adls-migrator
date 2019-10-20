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

package com.azure.storage.adlsmigrator;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.annotate.JsonIgnore;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;

/**
 * The Options class encapsulates all AdlsMigrator options.
 * These may be set from command-line (via the OptionsParser)
 * or may be set manually.
 */
public class AdlsMigratorOptions {

  /**
   * The DataBoxItem class holds information about the name & capabilities of each target Data Box.
   * This is commonly populated by reading from a JSON configuration file.
   */
  public static class DataBoxItem {
    protected String dataBoxDns;
    protected String accountKey;
    protected long sizeInBytes;
    protected String container;

    public DataBoxItem() {
    }

    public DataBoxItem(String dataBoxDns) {
      this.dataBoxDns = dataBoxDns;
    }

    public String getDataBoxDns() {
      return dataBoxDns;
    }

    public String getAccountKey() {
      return accountKey;
    }

    public long getSizeInBytes() {
      return sizeInBytes;
    }

    public String getContainer() {
      return container;
    }

    @JsonIgnore
    public boolean isUnsized() {
      return sizeInBytes == -1;
    }

    @JsonIgnore
    public boolean isDnsFullUri() {
      //If we don't have an account key, we assume that the DNS is a full WASB Uri
      return StringUtils.isBlank(accountKey);
    }

    @JsonIgnore
    public boolean isContainerSpecified() {
      return !StringUtils.isBlank(container);
    }

    public Path getTargetPath(String containerName) {
      //If we don't have an account key, assume the DNS name is actually a URI
      if (isDnsFullUri()) {
        return new Path(URI.create(dataBoxDns));
      } else {
        return new Path("wasb", 
          (isContainerSpecified() ? this.container : containerName) + 
          "@" + 
          dataBoxDns, "/");
      }
    }

    @Override
    public String toString() {
      return  "DataBoxItem {" + 
                "DataBox: " + dataBoxDns + ", " +
                (isContainerSpecified() ? "Container: " + container + ", " : "") +
                "Key: " + (isDnsFullUri() ? "Not specified" : "Supplied") + ", " +
                "Size: " + (isUnsized() ? "Unsized" : sizeInBytes) + 
              "}";
    }  
  }

  private boolean syncFolder = false;
  private boolean ignoreFailures = false;
  private boolean overwrite = false;
  private boolean append = false;
  private boolean skipCRC = false;
  private boolean blocking = true;
  // When "-diff s1 s2 src tgt" is passed, apply forward snapshot diff (from s1
  // to s2) of source cluster to the target cluster to sync target cluster with
  // the source cluster. Referred to as "Fdiff" in the code.
  // It's required that s2 is newer than s1.
  private boolean useDiff = false;

  /** Whether to log additional info (path, size) in the SKIP/COPY log. */
  private boolean verboseLog = false;

  // For both -diff and -rdiff, given the example command line switches, two
  // steps are taken:
  //   1. Sync Step. This step does renaming/deletion ops in the snapshot diff,
  //      so to avoid copying files copied already but renamed later(HDFS-7535)
  //   2. Copy Step. This step copy the necessary files from src to tgt
  //      2.1 For -diff, it copies from snapshot s2 of src (HDFS-8828)
  //      2.2 For -rdiff, it copies from snapshot s1 of src, where the src
  //          could be the tgt itself (HDFS-9820).
  //

  public static final int maxNumListstatusThreads = 40;
  private int numListstatusThreads = 0;  // Indicates that flag is not set.
  private int maxMaps = AdlsMigratorConstants.DEFAULT_MAPS;
  private int mapBandwidth = AdlsMigratorConstants.DEFAULT_BANDWIDTH_MB;

  private String sslConfigurationFile;

  private EnumSet<FileAttribute> preserveStatus = EnumSet.noneOf(FileAttribute.class);

  private boolean preserveRawXattrs;

  private Path logPath;

  private Path sourceFileListing;
  private List<Path> sourcePaths;

  private String fromSnapshot;
  private String toSnapshot;

  private DataBoxItem[] dataBoxes;

  private String targetContainer;

  private int tasksPerDataBox = AdlsMigratorConstants.DEFAULT_TASKS_PER_DATABOX;

  /**
   * The path to a file containing a list of paths to filter out of the copy.
   */
  private String filtersFile;

  /**
   * The path to a logfile listing all skipped files.
   */
  private String skippedFilesLog;

  /**
   * The path to a JSON file containing identities to be mapped.
   */
  private String identitiesMapFile;

  // targetPathExist is a derived field, it's initialized in the
  // beginning of adlsmigrator.
  private boolean targetPathExists = true;

  /**
   * The copyBufferSize to use in RetriableFileCopyCommand
   */
  private int copyBufferSize = AdlsMigratorConstants.COPY_BUFFER_SIZE_DEFAULT;

  public static enum FileAttribute{
    REPLICATION, BLOCKSIZE, USER, GROUP, PERMISSION, CHECKSUMTYPE, ACL, XATTR, TIMES;

    public static FileAttribute getAttribute(char symbol) {
      for (FileAttribute attribute : values()) {
        if (attribute.name().charAt(0) == Character.toUpperCase(symbol)) {
          return attribute;
        }
      }
      throw new NoSuchElementException("No attribute for " + symbol);
    }
  }

  /**
   * Constructor, to initialize source/target paths.
   * @param sourcePaths List of source-paths (including wildcards)
   *                     to be copied to target.
   */
  public AdlsMigratorOptions(List<Path> sourcePaths) {
    assert sourcePaths != null && !sourcePaths.isEmpty() : "Invalid source paths";

    this.sourcePaths = sourcePaths;
  }

  /**
   * Constructor, to initialize source/target paths.
   * @param sourceFileListing File containing list of source paths
   */
  public AdlsMigratorOptions(Path sourceFileListing) {
    assert sourceFileListing != null : "Invalid source paths";

    this.sourceFileListing = sourceFileListing;
  }

  /**
   * Copy constructor.
   * @param that AdlsMigratorOptions being copied from.
   */
  public AdlsMigratorOptions(AdlsMigratorOptions that) {
    if (this != that && that != null) {
      this.syncFolder = that.syncFolder;
      this.ignoreFailures = that.ignoreFailures;
      this.overwrite = that.overwrite;
      this.skipCRC = that.skipCRC;
      this.blocking = that.blocking;
      this.useDiff = that.useDiff;
      this.numListstatusThreads = that.numListstatusThreads;
      this.maxMaps = that.maxMaps;
      this.mapBandwidth = that.mapBandwidth;
      this.sslConfigurationFile = that.getSslConfigurationFile();
      this.preserveStatus = that.preserveStatus;
      this.preserveRawXattrs = that.preserveRawXattrs;
      this.logPath = that.getLogPath();
      this.sourceFileListing = that.getSourceFileListing();
      this.sourcePaths = that.getSourcePaths();
      this.dataBoxes = that.getDataBoxes();
      this.targetContainer = that.getTargetContainer();
      this.tasksPerDataBox = that.getTasksPerDataBox();
      this.targetPathExists = that.getTargetPathExists();  
      this.filtersFile = that.getFiltersFile();
      this.skippedFilesLog = that.getSkippedFilesLog();
      this.identitiesMapFile = that.getIdentitiesMapFile();
      this.copyBufferSize = that.copyBufferSize;
      this.verboseLog = that.verboseLog;
    }
  }

  /**
   * Should the data be sync'ed between source and target paths?
   *
   * @return true if data should be sync'ed up. false otherwise
   */
  public boolean shouldSyncFolder() {
    return syncFolder;
  }

  /**
   * Set if source and target folder contents be sync'ed up
   *
   * @param syncFolder - boolean switch
   */
  public void setSyncFolder(boolean syncFolder) {
    validate(AdlsMigratorOptionSwitch.SYNC_FOLDERS, syncFolder);
    this.syncFolder = syncFolder;
  }

  /**
   * Should failures be logged and ignored during copy?
   *
   * @return true if failures are to be logged and ignored. false otherwise
   */
  public boolean shouldIgnoreFailures() {
    return ignoreFailures;
  }

  /**
   * Set if failures during copy be ignored
   *
   * @param ignoreFailures - boolean switch
   */
  public void setIgnoreFailures(boolean ignoreFailures) {
    this.ignoreFailures = ignoreFailures;
  }

  /**
   * Should AdlsMigrator be running in blocking mode
   *
   * @return true if should run in blocking, false otherwise
   */
  public boolean shouldBlock() {
    return blocking;
  }

  /**
   * Set if Disctp should run blocking or non-blocking
   *
   * @param blocking - boolean switch
   */
  public void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  /**
   * Should files be overwritten always?
   *
   * @return true if files in target that may exist before adlsmigrator, should always
   *         be overwritten. false otherwise
   */
  public boolean shouldOverwrite() {
    return overwrite;
  }

  /**
   * Set if files should always be overwritten on target
   *
   * @param overwrite - boolean switch
   */
  public void setOverwrite(boolean overwrite) {
    validate(AdlsMigratorOptionSwitch.OVERWRITE, overwrite);
    this.overwrite = overwrite;
  }

  /**
   * @return whether we can append new data to target files
   */
  public boolean shouldAppend() {
    return append;
  }

  /**
   * Set if we want to append new data to target files. This is valid only with
   * update option and CRC is not skipped.
   */
  public void setAppend(boolean append) {
    validate(AdlsMigratorOptionSwitch.APPEND, append);
    this.append = append;
  }

  public boolean shouldUseDiff() {
    return this.useDiff;
  }

  public boolean shouldUseSnapshotDiff() {
    return shouldUseDiff();
  }

  public String getFromSnapshot() {
    return this.fromSnapshot;
  }

  public String getToSnapshot() {
    return this.toSnapshot;
  }

  public void setUseDiff(String fromSS, String toSS) {
    this.fromSnapshot = fromSS;
    this.toSnapshot = toSS;
    validate(AdlsMigratorOptionSwitch.DIFF, true);
    this.useDiff = true;
  }

  /**
   * Should CRC/checksum check be skipped while checking files are identical
   *
   * @return true if checksum check should be skipped while checking files are
   *         identical. false otherwise
   */
  public boolean shouldSkipCRC() {
    return skipCRC;
  }

  /**
   * Set if checksum comparison should be skipped while determining if
   * source and destination files are identical
   *
   * @param skipCRC - boolean switch
   */
  public void setSkipCRC(boolean skipCRC) {
    validate(AdlsMigratorOptionSwitch.SKIP_CRC, skipCRC);
    this.skipCRC = skipCRC;
  }

  /** Get the number of threads to use for listStatus
   *
   * @return Number of threads to do listStatus
   */
  public int getNumListstatusThreads() {
    return numListstatusThreads;
  }

  /** Set the number of threads to use for listStatus. We allow max 40
   *  threads. Setting numThreads to zero signify we should use the value
   *  from conf properties.
   *
   * @param numThreads - Number of threads
   */
  public void setNumListstatusThreads(int numThreads) {
    if (numThreads > maxNumListstatusThreads) {
      this.numListstatusThreads = maxNumListstatusThreads;
    } else if (numThreads > 0) {
      this.numListstatusThreads = numThreads;
    } else {
      this.numListstatusThreads = 0;
    }
  }

  /** Get the max number of maps to use for this copy
   *
   * @return Max number of maps
   */
  public int getMaxMaps() {
    return maxMaps;
  }

  /**
   * Set the max number of maps to use for copy
   *
   * @param maxMaps - Number of maps
   */
  public void setMaxMaps(int maxMaps) {
    this.maxMaps = Math.max(maxMaps, 1);
  }

  /** Get the map bandwidth in MB
   *
   * @return Bandwidth in MB
   */
  public int getMapBandwidth() {
    return mapBandwidth;
  }

  /**
   * Set per map bandwidth
   *
   * @param mapBandwidth - per map bandwidth
   */
  public void setMapBandwidth(int mapBandwidth) {
    assert mapBandwidth > 0 : "Bandwidth " + mapBandwidth + " is invalid (should be > 0)";
    this.mapBandwidth = mapBandwidth;
  }

  /**
   * Get path where the ssl configuration file is present to use for hftps://
   *
   * @return Path on local file system
   */
  public String getSslConfigurationFile() {
    return sslConfigurationFile;
  }

  /**
   * Set the SSL configuration file path to use with hftps:// (local path)
   *
   * @param sslConfigurationFile - Local ssl config file path
   */
  public void setSslConfigurationFile(String sslConfigurationFile) {
    this.sslConfigurationFile = sslConfigurationFile;
  }

  /**
   * Returns an iterator with the list of file attributes to preserve
   *
   * @return iterator of file attributes to preserve
   */
  public Iterator<FileAttribute> preserveAttributes() {
    return preserveStatus.iterator();
  }

  /**
   * Checks if the input attribute should be preserved or not.
   *
   * @param attribute - Attribute to check
   * @return True if attribute should be preserved, false otherwise
   */
  public boolean shouldPreserve(FileAttribute attribute) {
    return preserveStatus.contains(attribute);
  }

  /**
   * Add file attributes that need to be preserved. This method may be
   * called multiple times to add attributes.
   *
   * @param fileAttribute - Attribute to add, one at a time
   */
  public void preserve(FileAttribute fileAttribute) {
    for (FileAttribute attribute : preserveStatus) {
      if (attribute.equals(fileAttribute)) {
        return;
      }
    }
    preserveStatus.add(fileAttribute);
  }

  /**
   * Return true if raw.* xattrs should be preserved.
   * @return true if raw.* xattrs should be preserved.
   */
  public boolean shouldPreserveRawXattrs() {
    return preserveRawXattrs;
  }

  /**
   * Indicate that raw.* xattrs should be preserved
   */
  public void preserveRawXattrs() {
    preserveRawXattrs = true;
  }

  /** Get output directory for writing adlsmigrator logs. Otherwise logs
   * are temporarily written to JobStagingDir/_logs and deleted
   * upon job completion
   *
   * @return Log output path on the cluster where adlsmigrator job is run
   */
  public Path getLogPath() {
    return logPath;
  }

  /**
   * Set the log path where adlsmigrator output logs are stored
   * Uses JobStagingDir/_logs by default
   *
   * @param logPath - Path where logs will be saved
   */
  public void setLogPath(Path logPath) {
    this.logPath = logPath;
  }

  /**
   * File path (hdfs:// or file://) that contains the list of actual
   * files to copy
   *
   * @return - Source listing file path
   */
  public Path getSourceFileListing() {
    return sourceFileListing;
  }

  /**
   * Getter for sourcePaths.
   * @return List of source-paths.
   */
  public List<Path> getSourcePaths() {
    return sourcePaths;
  }

  /**
   * Setter for sourcePaths.
   * @param sourcePaths The new list of source-paths.
   */
  public void setSourcePaths(List<Path> sourcePaths) {
    assert sourcePaths != null && sourcePaths.size() != 0;
    this.sourcePaths = sourcePaths;
  }

  /**
   * Getter for dataBoxesListing.
   * @return Target Data Box listing path.
   */
  public DataBoxItem[] getDataBoxes() {
    return dataBoxes;
  }

  public void setDataBoxesConfigFile(String dataBoxesConfigFile) throws IOException {
    try (Reader input = new InputStreamReader(new FileInputStream(dataBoxesConfigFile), "UTF-8")) {
      dataBoxes = AdlsMigratorUtils.parseDataBoxesFromJson(input, AdlsMigratorOptions.DataBoxItem[].class);
    }
  }

  /**
   * Setter for singleDataBox.
   * @param singleDataBox The DNS name of the single target Data Box.
   */
  public void setSingleDataBox(String singleDataBox) {
    assert singleDataBox != null;
    dataBoxes = new DataBoxItem[1];
    dataBoxes[0] = new DataBoxItem(singleDataBox);
  }

  /**
   * Getter for the targetContainer.
   * @return The target container name.
   */
  public String getTargetContainer() {
    return targetContainer;
  }
  
  /**
   * Set targetContainer.
   * @param targetContainer The name of the target container.
   */
  public String setTargetContainer(String targetContainer) {
    return this.targetContainer = targetContainer;
  }

  /**
   * Getter for tasksPerDataBox.
   * @return The number of mapper tasks per 100TB Data Box (value is pro-rated for specified box size)
   */
  public int getTasksPerDataBox() {
    return tasksPerDataBox;
  }
  
  /**
   * Set tasksPerDataBox.
   * @param tasksPerDataBox The number of mapper tasks per 100TB Data Box (value is pro-rated for specified box size).
   */
  public int setTasksPerDataBox(int tasksPerDataBox) {
    return this.tasksPerDataBox = tasksPerDataBox;
  }

  /**
   * Getter for the targetPathExists.
   * @return The target-path.
   */
  public boolean getTargetPathExists() {
    return targetPathExists;
  }
  
  /**
   * Set targetPathExists.
   * @param targetPathExists Whether the target path of adlsmigrator exists.
   */
  public boolean setTargetPathExists(boolean targetPathExists) {
    return this.targetPathExists = targetPathExists;
  }

  /**
   * File path that contains the list of patterns
   * for paths to be filtered from the file copy.
   * @return - Filter  file path.
   */
  public final String getFiltersFile() {
    return filtersFile;
  }

  /**
   * Set filtersFile.
   * @param filtersFilename The path to a list of patterns to exclude from copy.
   */
  public final void setFiltersFile(String filtersFilename) {
    this.filtersFile = filtersFilename;
  }

  /**
   * File path for logfile listing all skipped files.
   * @return - Skipped files logfile path.
   */
  public final String getSkippedFilesLog() {
    return skippedFilesLog;
  }

  /**
   * Set skippedFilesLog.
   * @param skippedFilesLog The path to a logfile listing all skipped files.
   */
  public final void setSkippedFilesLog(String skippedFilesLog) {
    this.skippedFilesLog = skippedFilesLog;
  }

  /**
   * Local filesystem path for JSON file containing identities map
   * @return - Identities map path.
   */
  public final String getIdentitiesMapFile() {
    return identitiesMapFile;
  }

  /**
   * Set identitiesMapFile.
   * @param identitiesMapFile The path to the identities map file.
   */
  public final void setIdentitiesMapFile(String identitiesMapFile) {
    this.identitiesMapFile = identitiesMapFile;
  }

  public final void setCopyBufferSize(int newCopyBufferSize) {
    this.copyBufferSize =
        newCopyBufferSize > 0 ? newCopyBufferSize
            : AdlsMigratorConstants.COPY_BUFFER_SIZE_DEFAULT;
  }

  public int getCopyBufferSize() {
    return this.copyBufferSize;
  }

  public void setVerboseLog(boolean newVerboseLog) {
    validate(AdlsMigratorOptionSwitch.VERBOSE_LOG, newVerboseLog);
    this.verboseLog = newVerboseLog;
  }

  public boolean shouldVerboseLog() {
    return verboseLog;
  }

  public void validate(AdlsMigratorOptionSwitch option, boolean value) {

    boolean syncFolder = (option == AdlsMigratorOptionSwitch.SYNC_FOLDERS ?
        value : this.syncFolder);
    boolean overwrite = (option == AdlsMigratorOptionSwitch.OVERWRITE ?
        value : this.overwrite);
    boolean skipCRC = (option == AdlsMigratorOptionSwitch.SKIP_CRC ?
        value : this.skipCRC);
    boolean append = (option == AdlsMigratorOptionSwitch.APPEND ? value : this.append);
    boolean useDiff = (option == AdlsMigratorOptionSwitch.DIFF ? value : this.useDiff);
    boolean shouldVerboseLog = (option == AdlsMigratorOptionSwitch.VERBOSE_LOG ?
        value : this.verboseLog);

    if (overwrite && syncFolder) {
      throw new IllegalArgumentException("Overwrite and update options are " +
          "mutually exclusive");
    }

    if (!syncFolder && skipCRC) {
      throw new IllegalArgumentException("Skip CRC is valid only with update options");
    }

    if (!syncFolder && append) {
      throw new IllegalArgumentException(
          "Append is valid only with update options");
    }
    if (skipCRC && append) {
      throw new IllegalArgumentException(
          "Append is disallowed when skipping CRC");
    }
    if (!syncFolder && (useDiff)) {
      throw new IllegalArgumentException(
          "-diff is valid only with -update option");
    }

    if (useDiff) {
      if (StringUtils.isBlank(fromSnapshot) ||
          StringUtils.isBlank(toSnapshot)) {
        throw new IllegalArgumentException(
            "Must provide both the starting and ending " +
            "snapshot names for -diff");
      }
    }

    if (shouldVerboseLog && logPath == null) {
      throw new IllegalArgumentException("-v is valid only with -log option");
    }
  }

  /**
   * Add options to configuration. These will be used in the Mapper/committer
   *
   * @param conf - Configuration object to which the options need to be added
   */
  public void appendToConf(Configuration conf) throws IOException {

    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.IGNORE_FAILURES, String.valueOf(ignoreFailures));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.SYNC_FOLDERS, String.valueOf(syncFolder));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.OVERWRITE, String.valueOf(overwrite));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.APPEND, String.valueOf(append));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.DIFF, String.valueOf(useDiff));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.SKIP_CRC, String.valueOf(skipCRC));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.BANDWIDTH, String.valueOf(mapBandwidth));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.PRESERVE_STATUS, AdlsMigratorUtils.packAttributes(preserveStatus));
    if (StringUtils.isNotBlank(filtersFile)) {
      AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.FILTERS, filtersFile);
    }
    if (StringUtils.isNotBlank(skippedFilesLog)) {
      AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.SKIPPED_FILES_LOG, skippedFilesLog);
    }
    if (StringUtils.isNotBlank(identitiesMapFile)) {
      AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.IDENTITIES_MAP, identitiesMapFile);
    }
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.COPY_BUFFER_SIZE, String.valueOf(copyBufferSize));
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.VERBOSE_LOG, String.valueOf(verboseLog));

    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.TARGET_CONTAINER, targetContainer);
    AdlsMigratorOptionSwitch.addToConf(conf, AdlsMigratorOptionSwitch.NUM_TASKS_PER_DATABOX, String.valueOf(tasksPerDataBox));
    conf.set(AdlsMigratorConstants.CONF_LABEL_DATABOX_CONFIG, AdlsMigratorUtils.getDataBoxesAsJson(dataBoxes));
  }

  /**
   * Utility to easily string-ify Options, for logging.
   *
   * @return String representation of the Options.
   */
  @Override
  public String toString() {
    return "AdlsMigratorOptions{" +
        ", syncFolder=" + syncFolder +
        ", ignoreFailures=" + ignoreFailures +
        ", overwrite=" + overwrite +
        ", append=" + append +
        ", useDiff=" + useDiff +
        ", fromSnapshot=" + fromSnapshot +
        ", toSnapshot=" + toSnapshot +
        ", skipCRC=" + skipCRC +
        ", blocking=" + blocking +
        ", numListstatusThreads=" + numListstatusThreads +
        ", maxMaps=" + maxMaps +
        ", mapBandwidth=" + mapBandwidth +
        ", sslConfigurationFile='" + sslConfigurationFile + '\'' +
        ", preserveStatus=" + preserveStatus +
        ", preserveRawXattrs=" + preserveRawXattrs +
        ", logPath=" + logPath +
        ", sourceFileListing=" + sourceFileListing +
        ", sourcePaths=" + sourcePaths +
        ", dataBoxes=" + Arrays.toString(dataBoxes) +
        ", targetContainer=" + targetContainer +
        ", taskPerDataBox=" + tasksPerDataBox +
        ", targetPathExists=" + targetPathExists +
        ", filtersFile='" + filtersFile + '\'' +
        ", skippedFilesLog='" + skippedFilesLog + '\'' +
        ", identitiesMapFile='" + identitiesMapFile + '\'' +
        ", copyBufferSize=" + copyBufferSize +
        ", verboseLog=" + verboseLog +
        '}';
  }

  @Override
  protected AdlsMigratorOptions clone() throws CloneNotSupportedException {
    return (AdlsMigratorOptions) super.clone();
  }
}
