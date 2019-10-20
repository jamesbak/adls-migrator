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

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

/**
 * Enumeration mapping configuration keys to adlsmigrator command line
 * options.
 */
public enum AdlsMigratorOptionSwitch {

  /**
   * Ignores any failures during copy, and continues with rest.
   * Logs failures in a file
   */
  IGNORE_FAILURES(AdlsMigratorConstants.CONF_LABEL_IGNORE_FAILURES,
      new Option("i", false, "Ignore failures during copy")),

  /**
   * Preserves status of file/path in the target.
   * Default behavior with -p, is to preserve replication,
   * block size, user, group, permission, checksum type and timestamps on the 
   * target file. Note that when preserving checksum type, block size is also 
   * preserved.
   *
   * If any of the optional switches are present among rbugpcaxt, then
   * only the corresponding file attribute is preserved.
   */
  PRESERVE_STATUS(AdlsMigratorConstants.CONF_LABEL_PRESERVE_STATUS,
      new Option("p", true, "preserve status (rbugpcaxt)(replication, " +
          "block-size, user, group, permission, checksum-type, ACL, XATTR, " +
          "timestamps). If -p is specified with no <arg>, then preserves " +
          "replication, block size, user, group, permission, checksum type " +
          "and timestamps. " +
          "raw.* xattrs are preserved when both the source and destination " +
          "paths are in the /.reserved/raw hierarchy (HDFS only). raw.* xattr" +
          "preservation is independent of the -p flag. " +
          "Refer to the adlsmigrator documentation for more details.")),

  /**
   * Update target location by copying only files that are missing
   * in the target. This can be used to periodically sync two folders
   * across source and target. Typically used with DELETE_MISSING
   * Incompatible with ATOMIC_COMMIT
   */
  SYNC_FOLDERS(AdlsMigratorConstants.CONF_LABEL_SYNC_FOLDERS,
      new Option("update", false, "Update target, copying only missing" +
          "files or directories")),

  /**
   * Configuration file to use with hftps:// for securely copying
   * files across clusters. Typically the configuration file contains
   * truststore/keystore information such as location, password and type
   */
  SSL_CONF(AdlsMigratorConstants.CONF_LABEL_SSL_CONF,
      new Option("mapredSslConf", true, "Configuration for ssl config file" +
          ", to use with hftps://. Must be in the classpath.")),
  /**
   * Number of threads for building source file listing (before map-reduce
   * phase, max one listStatus per thread at a time).
   */
  NUM_LISTSTATUS_THREADS(AdlsMigratorConstants.CONF_LABEL_LISTSTATUS_THREADS,
      new Option("numListstatusThreads", true, "Number of threads to " +
          "use for building file listing (max " +
          AdlsMigratorOptions.maxNumListstatusThreads + ").")),
  /**
   * Max number of maps to use during copy. adlsmigrator will split work
   * as equally as possible among these maps
   */
  MAX_MAPS(AdlsMigratorConstants.CONF_LABEL_MAX_MAPS,
      new Option("m", true, "Max number of concurrent maps to use for copy")),

  /**
   * Source file listing can be provided to adlsmigrator in a file.
   * This allows adlsmigrator to copy random list of files from source
   * and copy them to target
   */
  SOURCE_FILE_LISTING(AdlsMigratorConstants.CONF_LABEL_SOURCE_LISTING,
      new Option("f", true, "List of files that need to be copied")),

  /**
   * The list of Data Boxes to copy to. This is a JSON file that includes
   * the DNS name of each Data Box, the account key & size.
   * This JSON schema matches that defined in {@link com.azure.storage.adlsmigrator.AdlsMigratorOptions.DataBoxItem}.
   */
  DATABOX_FILE_LISTING(AdlsMigratorConstants.CONF_LABEL_DATABOX_LISTING,
      new Option("d", true, "JSON file specifying the list of Data Boxes with sizes to copy data to")),

  /**
   * The name of the container on each Data Box to copy the data to.
   */
  TARGET_CONTAINER(AdlsMigratorConstants.CONF_LABEL_TARGET_CONTAINER,
      new Option("c", true, "Target container name on each Data Box")),

  /**
   * The name of the container on each Data Box to copy the data to.
   */
  NUM_TASKS_PER_DATABOX(AdlsMigratorConstants.CONF_LABEL_NUM_TASKS_PER_DATABOX,
      new Option("n", true, "Number of maps for each 100TB Data Box. Actual value is pro-rated for specified Data Box size. " +
                            "This value can be overridden by the -m option, which specifies the total number of maps.")),

  /**
   * Log path where adlsmigrator output logs are written to
   */
  LOG_PATH(AdlsMigratorConstants.CONF_LABEL_LOG_PATH,
      new Option("log", true, "Folder on DFS where adlsmigrator execution logs are saved")),

  /**
   * Log additional info (path, size) in the SKIP/COPY log.
   */
  VERBOSE_LOG(AdlsMigratorConstants.CONF_LABEL_VERBOSE_LOG,
      new Option("v", false,
          "Log additional info (path, size) in the SKIP/COPY log")),

  /**
   * Skip CRC checks between source and target, when determining what
   * files need to be copied.
   */
  SKIP_CRC(AdlsMigratorConstants.CONF_LABEL_SKIP_CRC,
      new Option("skipcrccheck", false, "Whether to skip CRC checks between " +
          "source and target paths.")),

  /**
   * Overwrite target-files unconditionally.
   */
  OVERWRITE(AdlsMigratorConstants.CONF_LABEL_OVERWRITE,
      new Option("overwrite", false, "Choose to overwrite target files " +
          "unconditionally, even if they exist.")),

  APPEND(AdlsMigratorConstants.CONF_LABEL_APPEND,
      new Option("append", false,
          "Reuse existing data in target files and append new data to them if possible")),

  DIFF(AdlsMigratorConstants.CONF_LABEL_DIFF,
      new Option("diff", false,
      "Use snapshot diff report to identify the difference between source and target"),
      2),

  /**
   * Should AdlsMigrator be blocking
   */
  BLOCKING("",
      new Option("async", false, "Should adlsmigrator execution be blocking")),

  /**
   * Configurable copy buffer size.
   */
  COPY_BUFFER_SIZE(AdlsMigratorConstants.CONF_LABEL_COPY_BUFFER_SIZE,
      new Option("copybuffersize", true, "Size of the copy buffer to use. "
          + "By default <copybuffersize> is "
          + AdlsMigratorConstants.COPY_BUFFER_SIZE_DEFAULT + "B.")),

  /**
   * Specify bandwidth per map in MB
   */
  BANDWIDTH(AdlsMigratorConstants.CONF_LABEL_BANDWIDTH_MB,
      new Option("bandwidth", true, "Specify bandwidth per map in MB")),

  /**
   * Path containing a list of strings, which when found in the path of
   * a file to be copied excludes that file from the copy job.
   */
  FILTERS(AdlsMigratorConstants.CONF_LABEL_FILTERS_FILE,
      new Option("filters", true, "The path to a file containing a list of"
          + " strings for paths to be excluded from the copy.")),

  /**
   * Path specifying output log listing files that cannot be placed on any Data Box.
   */
  SKIPPED_FILES_LOG(AdlsMigratorConstants.CONF_LABEL_SKIPPED_FILES_LOGFILE,
      new Option("skippedlog", true, "The path specifying output log listing files that cannot be placed on any Data Box")),

  /**
   * Local path specifying JSON file containing list of source file identities.
   */
  IDENTITIES_MAP(AdlsMigratorConstants.CONF_LABEL_IDENTITIES_MAP_FILE,
      new Option("identitymap", true, "The local path specifying location of a JSON file that contains identities to be mapped"));


  public static final String PRESERVE_STATUS_DEFAULT = "-prbugpct";
  private final String confLabel;
  private final Option option;

  AdlsMigratorOptionSwitch(String confLabel, Option option) {
    this.confLabel = confLabel;
    this.option = option;
  }

  AdlsMigratorOptionSwitch(String confLabel, Option option, int argNum) {
    this(confLabel, option);
    this.option.setArgs(argNum);
  }

  /**
   * Get Configuration label for the option
   * @return configuration label name
   */
  public String getConfigLabel() {
    return confLabel;
  }

  /**
   * Get CLI Option corresponding to the adlsmigrator option
   * @return option
   */
  public Option getOption() {
    return option;
  }

  /**
   * Get Switch symbol
   * @return switch symbol char
   */
  public String getSwitch() {
    return option.getOpt();
  }

  @Override
  public String toString() {
    return  super.name() + " {" +
        "confLabel='" + confLabel + '\'' +
        ", option=" + option + '}';
  }

  /**
   * Helper function to add an option to hadoop configuration object
   * @param conf - Configuration object to include the option
   * @param option - Option to add
   * @param value - Value
   */
  public static void addToConf(Configuration conf,
                               AdlsMigratorOptionSwitch option,
                               String value) {
    conf.set(option.getConfigLabel(), value);
  }

  /**
   * Helper function to set an option to hadoop configuration object
   * @param conf - Configuration object to include the option
   * @param option - Option to add
   */
  public static void addToConf(Configuration conf,
                               AdlsMigratorOptionSwitch option) {
    conf.set(option.getConfigLabel(), "true");
  }
}
