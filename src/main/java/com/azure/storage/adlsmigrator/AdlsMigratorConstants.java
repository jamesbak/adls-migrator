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

import org.apache.hadoop.fs.Path;

/**
 * Utility class to hold commonly used constants.
 */
public class AdlsMigratorConstants {

  /* Default number of threads to use for building file listing */
  public static final int DEFAULT_LISTSTATUS_THREADS = 1;

  /* Default number of maps to use for AdlsMigrator */
  public static final int DEFAULT_MAPS = 20;

  /* Default number of maps per Databox */
  public static final int DEFAULT_TASKS_PER_DATABOX = 6;

  /* Default bandwidth if none specified */
  public static final int DEFAULT_BANDWIDTH_MB = 100;

  /**
   *  Constants mapping to command line switches/input options
   */
  public static final String CONF_LABEL_LOG_PATH = "adlsmigrator.log.path";
  public static final String CONF_LABEL_VERBOSE_LOG = "adlsmigrator.verbose.log";
  public static final String CONF_LABEL_IGNORE_FAILURES = "adlsmigrator.ignore.failures";
  public static final String CONF_LABEL_PRESERVE_STATUS = "adlsmigrator.preserve.status";
  public static final String CONF_LABEL_PRESERVE_RAWXATTRS = "adlsmigrator.preserve.rawxattrs";
  public static final String CONF_LABEL_SYNC_FOLDERS = "adlsmigrator.sync.folders";
  public static final String CONF_LABEL_SSL_CONF = "adlsmigrator.keystore.resource";
  public static final String CONF_LABEL_LISTSTATUS_THREADS = "adlsmigrator.liststatus.threads";
  public static final String CONF_LABEL_MAX_MAPS = "adlsmigrator.max.maps";
  public static final String CONF_LABEL_SOURCE_LISTING = "adlsmigrator.source.listing";
  public static final String CONF_LABEL_DATABOX_LISTING = "adlsmigrator.databox.listing";
  public static final String CONF_LABEL_DATABOX_CONFIG = "adlsmigrator.databox.configuration";
  public static final String CONF_LABEL_TARGET_CONTAINER = "adlsmigrator.target.container";
  public static final String CONF_LABEL_NUM_TASKS_PER_DATABOX = "adlsmigrator.tasks.per.databox";
  public static final String CONF_LABEL_SKIP_CRC = "adlsmigrator.skip.crc";
  public static final String CONF_LABEL_OVERWRITE = "adlsmigrator.copy.overwrite";
  public static final String CONF_LABEL_APPEND = "adlsmigrator.copy.append";
  public static final String CONF_LABEL_DIFF = "adlsmigrator.copy.diff";
  public static final String CONF_LABEL_BANDWIDTH_MB = "adlsmigrator.map.bandwidth.mb";
  public static final String CONF_LABEL_SIMPLE_LISTING_FILESTATUS_SIZE = "adlsmigrator.simplelisting.file.status.size";
  public static final String CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES = "adlsmigrator.simplelisting.randomize.files";
  public static final String CONF_LABEL_FILTERS_FILE = "adlsmigrator.filters.file";
  public static final String CONF_LABEL_SKIPPED_FILES_LOGFILE = "adlsmigrator.skipped.files.logfile";
  public static final String CONF_LABEL_IDENTITIES_MAP_FILE = "adlsmigrator.source.identities.file";
  public static final String CONF_LABEL_MAX_CHUNKS_TOLERABLE = "adlsmigrator.dynamic.max.chunks.tolerable";
  public static final String CONF_LABEL_MAX_CHUNKS_IDEAL = "adlsmigrator.dynamic.max.chunks.ideal";
  public static final String CONF_LABEL_MIN_RECORDS_PER_CHUNK = "adlsmigrator.dynamic.min.records_per_chunk";
  public static final String CONF_LABEL_SPLIT_RATIO = "adlsmigrator.dynamic.split.ratio";
  
  /* Total bytes to be copied. Updated by copylisting. Unfiltered count */
  public static final String CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED = "mapred.total.bytes.expected";

  /* Total number of paths to copy, includes directories. Unfiltered count */
  public static final String CONF_LABEL_TOTAL_NUMBER_OF_RECORDS = "mapred.number.of.records";

  /* SSL keystore resource */
  public static final String CONF_LABEL_SSL_KEYSTORE = "dfs.https.client.keystore.resource";

  /* If input is based -f <<source listing>>, file containing the src paths */
  public static final String CONF_LABEL_LISTING_FILE_PATH = "adlsmigrator.listing.file.path";

  /* Boolean to indicate whether the target of adlsmigrator exists. */
  public static final String CONF_LABEL_TARGET_PATH_EXISTS = "adlsmigrator.target.path.exists";
  
  /**
   * AdlsMigrator job id for consumers of the Disctp 
   */
  public static final String CONF_LABEL_ADLSMIGRATOR_JOB_ID = "adlsmigrator.job.id";

  /* Meta folder where the job's intermediate data is kept */
  public static final String CONF_LABEL_META_FOLDER = "adlsmigrator.meta.folder";

  /* AdlsMigrator CopyListing class override param */
  public static final String CONF_LABEL_COPY_LISTING_CLASS = "adlsmigrator.copy.listing.class";

  /* AdlsMigrator Copy Buffer Size */
  public static final String CONF_LABEL_COPY_BUFFER_SIZE = "adlsmigrator.copy.buffer.size";

  /**
   * Conf label for SSL Trust-store location.
   */
  public static final String CONF_LABEL_SSL_TRUST_STORE_LOCATION = "ssl.client.truststore.location";

  /**
   * Conf label for SSL Key-store location.
   */
  public static final String CONF_LABEL_SSL_KEY_STORE_LOCATION = "ssl.client.keystore.location";

  /**
   * Constants for AdlsMigrator return code to shell / consumer of ToolRunner's run
   */
  public static final int SUCCESS = 0;
  public static final int INVALID_ARGUMENT = -1;
  public static final int DUPLICATE_INPUT = -2;
  public static final int ACLS_NOT_SUPPORTED = -3;
  public static final int XATTRS_NOT_SUPPORTED = -4;
  public static final int UNKNOWN_ERROR = -999;
  
  /**
   * Constants for AdlsMigrator default values of configurable values
   */
  public static final int MAX_CHUNKS_TOLERABLE_DEFAULT = 400;
  public static final int MAX_CHUNKS_IDEAL_DEFAULT     = 100;
  public static final int MIN_RECORDS_PER_CHUNK_DEFAULT = 5;
  public static final int SPLIT_RATIO_DEFAULT  = 2;

  /**
   * Constants for NONE file deletion
   */
  public static final String NONE_PATH_NAME = "/NONE";
  public static final Path NONE_PATH = new Path(NONE_PATH_NAME);
  public static final Path RAW_NONE_PATH = new Path(
      AdlsMigratorConstants.HDFS_RESERVED_RAW_DIRECTORY_NAME + NONE_PATH_NAME);

  /**
   * Value of reserved raw HDFS directory when copying raw.* xattrs.
   */
  public static final String HDFS_RESERVED_RAW_DIRECTORY_NAME = "/.reserved/raw";

  static final String HDFS_ADLSMIGRATOR_DIFF_DIRECTORY_NAME = ".adlsmigrator.diff.tmp";

  public static final int COPY_BUFFER_SIZE_DEFAULT = 8 * 1024;
}
