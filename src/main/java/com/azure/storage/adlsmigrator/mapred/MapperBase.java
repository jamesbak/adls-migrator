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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import com.azure.storage.adlsmigrator.CopyListingFileStatus;
import com.azure.storage.adlsmigrator.AdlsMigratorConstants;
import com.azure.storage.adlsmigrator.AdlsMigratorOptionSwitch;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions.FileAttribute;
import com.azure.storage.adlsmigrator.mapred.RetriableFileCopyCommand.CopyReadException;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;
import com.azure.storage.adlsmigrator.mapred.lib.DataBoxSplit;
import org.apache.hadoop.util.StringUtils;

public abstract class MapperBase extends Mapper<Text, CopyListingFileStatus, Text, Text> {

  /**
   * Hadoop counters for the AdlsMigrator CopyMapper.
   * (These have been kept identical to the old AdlsMigrator,
   * for backward compatibility.)
   */
  public static enum Counter {
    COPY,         // Number of files received by the mapper for copy.
    DIR_COPY,     // Number of directories received by the mapper for copy.
    SKIP,         // Number of files skipped.
    FAIL,         // Number of files that failed to be copied.
    BYTESCOPIED,  // Number of bytes actually copied by the copy-mapper, total.
    BYTESEXPECTED,// Number of bytes expected to be copied.
    BYTESFAILED,  // Number of bytes that failed to be copied.
    BYTESSKIPPED, // Number of bytes that were skipped from copy.
  }

  private static Log LOG = LogFactory.getLog(MapperBase.class);

  protected Configuration conf;

  protected boolean ignoreFailures = false;
  protected boolean verboseLog = false;

  /**
   * Implementation of the Mapper::setup() method. This extracts the AdlsMigrator-
   * options specified in the Job's configuration, to set up the Job.
   * @param context Mapper's context.
   * @throws IOException On IO failure.
   * @throws InterruptedException If the job is interrupted.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    conf = context.getConfiguration();

    ignoreFailures = conf.getBoolean(AdlsMigratorOptionSwitch.IGNORE_FAILURES.getConfigLabel(), false);
    verboseLog = conf.getBoolean(AdlsMigratorOptionSwitch.VERBOSE_LOG.getConfigLabel(), false);

    if (conf.get(AdlsMigratorConstants.CONF_LABEL_SSL_CONF) != null) {
      initializeSSLConf(context);
    }
  }

  /**
   * Initialize SSL Config if same is set in conf
   *
   * @throws IOException - If any
   */
  private void initializeSSLConf(Context context) throws IOException {
    LOG.info("Initializing SSL configuration");

    String workDir = conf.get(JobContext.JOB_LOCAL_DIR) + "/work";
    Path[] cacheFiles = context.getLocalCacheFiles();

    Configuration sslConfig = new Configuration(false);
    String sslConfFileName = conf.get(AdlsMigratorConstants.CONF_LABEL_SSL_CONF);
    Path sslClient = findCacheFile(cacheFiles, sslConfFileName);
    if (sslClient == null) {
      LOG.warn("SSL Client config file not found. Was looking for " + sslConfFileName +
          " in " + Arrays.toString(cacheFiles));
      return;
    }
    sslConfig.addResource(sslClient);

    String trustStoreFile = conf.get("ssl.client.truststore.location");
    Path trustStorePath = findCacheFile(cacheFiles, trustStoreFile);
    sslConfig.set("ssl.client.truststore.location", trustStorePath.toString());

    String keyStoreFile = conf.get("ssl.client.keystore.location");
    Path keyStorePath = findCacheFile(cacheFiles, keyStoreFile);
    sslConfig.set("ssl.client.keystore.location", keyStorePath.toString());

    try {
      OutputStream out = new FileOutputStream(workDir + "/" + sslConfFileName);
      try {
        sslConfig.writeXml(out);
      } finally {
        out.close();
      }
      conf.set(AdlsMigratorConstants.CONF_LABEL_SSL_KEYSTORE, sslConfFileName);
    } catch (IOException e) {
      LOG.warn("Unable to write out the ssl configuration. " +
          "Will fall back to default ssl-client.xml in class path, if there is one", e);
    }
  }

  /**
   * Find entry from distributed cache
   *
   * @param cacheFiles - All localized cache files
   * @param fileName - fileName to search
   * @return Path of the filename if found, else null
   */
  private Path findCacheFile(Path[] cacheFiles, String fileName) {
    if (cacheFiles != null && cacheFiles.length > 0) {
      for (Path file : cacheFiles) {
        if (file.getName().equals(fileName)) {
          return file;
        }
      }
    }
    return null;
  }

  protected String getFileType(CopyListingFileStatus fileStatus) {
    if (null == fileStatus) {
      return "N/A";
    }
    return fileStatus.isDirectory() ? "dir" : "file";
  }

  protected String getFileType(FileStatus fileStatus) {
    if (null == fileStatus) {
      return "N/A";
    }
    return fileStatus.isDirectory() ? "dir" : "file";
  }

  protected void handleFailures(IOException exception,
      CopyListingFileStatus sourceFileStatus, Path target, Context context, boolean handleAllIOExceptions)
      throws IOException, InterruptedException {

    LOG.error("Failure in copying " + sourceFileStatus.getPath() +
        (sourceFileStatus.isSplit()? ","
            + " offset=" + sourceFileStatus.getChunkOffset()
            + " chunkLength=" + sourceFileStatus.getChunkLength()
            : "") +
        " to " + target, exception);
    AdlsMigratorUtils.appendSkippedFile(context.getConfiguration(), sourceFileStatus.getPath().toString(), exception);
    
    if (ignoreFailures &&
        (handleAllIOExceptions ?
          ExceptionUtils.indexOfType(exception, IOException.class) != -1 :
          ExceptionUtils.indexOfType(exception, CopyReadException.class) != -1)) {
      incrementCounter(context, Counter.FAIL, 1);
      incrementCounter(context, Counter.BYTESFAILED, sourceFileStatus.getLen());
      context.write(null, new Text("FAIL: " + sourceFileStatus.getPath() + " - " +
          StringUtils.stringifyException(exception)));
    }
    else
      throw exception;
  }

  protected static void incrementCounter(Context context, Counter counter, long value) {
    context.getCounter(counter).increment(value);
  }
}
