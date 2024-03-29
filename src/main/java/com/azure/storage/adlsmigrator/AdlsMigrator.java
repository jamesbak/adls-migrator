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

import java.io.IOException;
import java.net.URL;
import java.util.Random;

import org.apache.hadoop.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions.FileAttribute;
import com.azure.storage.adlsmigrator.CopyListing.*;
import com.azure.storage.adlsmigrator.mapred.CopyAclsMapper;
import com.azure.storage.adlsmigrator.mapred.CopyMapper;
import com.azure.storage.adlsmigrator.mapred.CopyOutputFormat;
import com.azure.storage.adlsmigrator.mapred.UniformSizeInputFormat;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.annotations.VisibleForTesting;

/**
 * AdlsMigrator is the main driver-class for AdlsMigratorV2.
 * For command-line use, AdlsMigrator::main() orchestrates the parsing of command-line
 * parameters and the launch of the AdlsMigrator job.
 * For programmatic use, a AdlsMigrator object can be constructed by specifying
 * options (in a AdlsMigratorOptions object), and AdlsMigrator::execute() may be used to
 * launch the copy-job. AdlsMigrator may alternatively be sub-classed to fine-tune
 * behaviour.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AdlsMigrator extends Configured implements Tool {

  /**
   * Priority of the shutdown hook.
   */
  static final int SHUTDOWN_HOOK_PRIORITY = 30;

  static final Log LOG = LogFactory.getLog(AdlsMigrator.class);

  private AdlsMigratorOptions inputOptions;
  private Path metaFolder;

  private static final String PREFIX = "_AdlsMigrator";
  private static final String AdlsMigrator_DEFAULT_XML = "adlsmigrator-default.xml";
  static final Random rand = new Random();

  private boolean submitted;
  private FileSystem jobFS;

  private void prepareFileListing(Job job) throws Exception {
    if (inputOptions.shouldUseSnapshotDiff()) {
      // When "-diff" or "-rdiff" is passed, do sync() first, then
      // create copyListing based on snapshot diff.
      /*AdlsMigratorSync AdlsMigratorSync = new AdlsMigratorSync(inputOptions, getConf());
      if (AdlsMigratorSync.sync()) {
        createInputFileListingWithDiff(job, AdlsMigratorSync);
      } else {
        throw new Exception("AdlsMigrator sync failed, input options: "
            + inputOptions);
      }*/
      throw new InvalidInputException("Snapshot diff support (-diff) is currently unsupported");
    } else {
      // When no "-diff" or "-rdiff" is passed, create copyListing
      // in regular way.
      createInputFileListing(job);
    }
  }

  /**
   * Public Constructor. Creates AdlsMigrator object with specified input-parameters.
   * (E.g. source-paths, target-location, etc.)
   * @param inputOptions Options (indicating source-paths, target-location.)
   * @param configuration The Hadoop configuration against which the Copy-mapper must run.
   * @throws Exception
   */
  public AdlsMigrator(Configuration configuration, AdlsMigratorOptions inputOptions) throws Exception {
    Configuration config = new Configuration(configuration);
    config.addResource(AdlsMigrator_DEFAULT_XML);
    setConf(config);
    this.inputOptions = inputOptions;
    this.metaFolder   = createMetaFolderPath();
  }

  /**
   * To be used with the ToolRunner. Not for public consumption.
   */
  @VisibleForTesting
  AdlsMigrator() {}

  /**
   * Implementation of Tool::run(). Orchestrates the copy of source file(s)
   * to target location, by:
   *  1. Creating a list of files to be copied to target.
   *  2. Launching a Map-only job to copy the files. (Delegates to execute().)
   * @param argv List of arguments passed to AdlsMigrator, from the ToolRunner.
   * @return On success, it returns 0. Else, -1.
   */
  @Override
  public int run(String[] argv) {
    if (argv.length < 1) {
      OptionsParser.usage();
      return AdlsMigratorConstants.INVALID_ARGUMENT;
    }
    
    try {
      inputOptions = (OptionsParser.parse(argv));
      setTargetPathExists();
      LOG.info("Input Options: " + inputOptions);
    } catch (Throwable e) {
      LOG.error("Invalid arguments: " + StringUtils.stringifyException(e) + 
        (e.getCause() != null ? ", Caused by:" + StringUtils.stringifyException(e.getCause()) : ""));
      System.err.println("Invalid arguments: " + e.getMessage());
      OptionsParser.usage();      
      return AdlsMigratorConstants.INVALID_ARGUMENT;
    }
    
    try {
      execute();
    } catch (InvalidInputException e) {
      LOG.error("Invalid input: ", e);
      return AdlsMigratorConstants.INVALID_ARGUMENT;
    } catch (DuplicateFileException e) {
      LOG.error("Duplicate files in input path: ", e);
      return AdlsMigratorConstants.DUPLICATE_INPUT;
    } catch (AclsNotSupportedException e) {
      LOG.error("ACLs not supported on at least one file system: ", e);
      return AdlsMigratorConstants.ACLS_NOT_SUPPORTED;
    } catch (XAttrsNotSupportedException e) {
      LOG.error("XAttrs not supported on at least one file system: ", e);
      return AdlsMigratorConstants.XATTRS_NOT_SUPPORTED;
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      return AdlsMigratorConstants.UNKNOWN_ERROR;
    }
    return AdlsMigratorConstants.SUCCESS;
  }

  /**
   * Implements the core-execution. Creates the file-list for copy,
   * and launches the Hadoop-job, to do the copy.
   * @return Job handle
   * @throws Exception
   */
  public Job execute() throws Exception {
    Job job = createAndSubmitJob();

    if (inputOptions.shouldBlock()) {
      waitForJobCompletion(job);
    }
    return job;
  }

  /**
   * Create and submit the mapreduce job.
   * @return The mapreduce job object that has been submitted
   */
  public Job createAndSubmitJob() throws Exception {
    assert inputOptions != null;
    assert getConf() != null;
    Job job = null;
    try {
      synchronized(this) {
        //Don't cleanup while we are setting up.
        metaFolder = createMetaFolderPath();
        jobFS = metaFolder.getFileSystem(getConf());
        job = createJob();
      }
      prepareFileListing(job);
      job.submit();
      submitted = true;
    } finally {
      if (!submitted) {
        cleanup();
      }
    }

    String jobID = job.getJobID().toString();
    job.getConfiguration().set(AdlsMigratorConstants.CONF_LABEL_ADLSMIGRATOR_JOB_ID, jobID);
    LOG.info("AdlsMigrator job-id: " + jobID);

    return job;
  }

  /**
   * Wait for the given job to complete.
   * @param job the given mapreduce job that has already been submitted
   */
  public void waitForJobCompletion(Job job) throws Exception {
    assert job != null;
    if (!job.waitForCompletion(true)) {
      throw new IOException("AdlsMigrator failure: Job " + job.getJobID()
          + " has failed: " + job.getStatus().getFailureInfo());
    }
  }

  /**
   * Set targetPathExists in both inputOptions and job config,
   * for the benefit of CopyCommitter
   */
  private void setTargetPathExists() throws IOException {
    Configuration conf = getConf();
    boolean allTargetsExist = true;
    if (inputOptions.getTransferAcls()) {
      LOG.info("setTargetPathExists: Path: " + inputOptions.getTargetPath().toString());
      try {
        FileSystem fs = inputOptions.getTargetPath().getFileSystem(conf);
        if (!fs.exists(inputOptions.getTargetPath())) {
          LOG.error("Specified target path: " + inputOptions.getTargetPath() 
                    + " does not exist. The target path must exist before transferring ACLs");
          allTargetsExist = false;
        }
      } catch (IOException ex) {
        LOG.error("setTargetPathExists failure: " + ex.getCause());
        throw ex;
      }
    } else {
      for (AdlsMigratorOptions.DataBoxItem dataBox : inputOptions.getDataBoxes()) {
        Path dataBoxPath = dataBox.getTargetPath();
        LOG.info("setTargetPathExists: Path: " + dataBoxPath.toString());
        if (!dataBox.isDnsFullUri()) {
          conf.set("fs.azure.account.key." + dataBox.getDataBoxDns(), dataBox.getAccountKey());
        }
        FileSystem targetFS = dataBoxPath.getFileSystem(conf);
        if (!targetFS.exists(dataBoxPath)) {
          LOG.error("Data Box:Container (" + dataBoxPath + ") does not exist");
          allTargetsExist = false;
        }
      }
    }
    conf.setBoolean(AdlsMigratorConstants.CONF_LABEL_TARGET_PATH_EXISTS, allTargetsExist);
  }

  /**
   * Create Job object for submitting it, with all the configuration
   *
   * @return Reference to job object.
   * @throws IOException - Exception if any
   */
  private Job createJob() throws IOException {
    String jobName = "AdlsMigrator";
    String userChosenName = getConf().get(JobContext.JOB_NAME);
    if (userChosenName != null)
      jobName += ": " + userChosenName;
    Job job = Job.getInstance(getConf());
    job.setJobName(jobName);
    if (inputOptions.getTransferAcls()) {
      // When setting ACLs, we just distribute the files evenly across all map tasks
      job.setInputFormatClass(UniformSizeInputFormat.class);
      job.setMapperClass(CopyAclsMapper.class);
    } else {
      job.setInputFormatClass(AdlsMigratorUtils.getStrategy(getConf(), inputOptions));
      job.setMapperClass(CopyMapper.class);
    }
    job.setJarByClass(CopyMapper.class);
    configureOutputFormat(job);

    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(CopyOutputFormat.class);
    job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
    job.getConfiguration().set(JobContext.NUM_MAPS, String.valueOf(inputOptions.getMaxMaps()));
    ((JobConf)job.getConfiguration()).setNumMapTasks(inputOptions.getMaxMaps());

    if (inputOptions.getSslConfigurationFile() != null) {
      setupSSLConfig(job);
    }

    inputOptions.appendToConf(job.getConfiguration());
    return job;
  }

  /**
   * Setup ssl configuration on the job configuration to enable hsftp access
   * from map job. Also copy the ssl configuration file to Distributed cache
   *
   * @param job - Reference to job's handle
   * @throws java.io.IOException - Exception if unable to locate ssl config file
   */
  private void setupSSLConfig(Job job) throws IOException  {
    Configuration configuration = job.getConfiguration();
    URL sslFileUrl = configuration.getResource(inputOptions
        .getSslConfigurationFile());
    if (sslFileUrl == null) {
      throw new IOException(
          "Given ssl configuration file doesn't exist in class path : "
              + inputOptions.getSslConfigurationFile());
    }
    Path sslConfigPath = new Path(sslFileUrl.toString());

    addSSLFilesToDistCache(job, sslConfigPath);
    configuration.set(AdlsMigratorConstants.CONF_LABEL_SSL_CONF, sslConfigPath.getName());
    configuration.set(AdlsMigratorConstants.CONF_LABEL_SSL_KEYSTORE, sslConfigPath.getName());
  }

  /**
   * Add SSL files to distributed cache. Trust store, key store and ssl config xml
   *
   * @param job - Job handle
   * @param sslConfigPath - ssl Configuration file specified through options
   * @throws IOException - If any
   */
  private void addSSLFilesToDistCache(Job job,
                                      Path sslConfigPath) throws IOException {
    Configuration configuration = job.getConfiguration();
    FileSystem localFS = FileSystem.getLocal(configuration);

    Configuration sslConf = new Configuration(false);
    sslConf.addResource(sslConfigPath);

    Path localStorePath = getLocalStorePath(sslConf,
                            AdlsMigratorConstants.CONF_LABEL_SSL_TRUST_STORE_LOCATION);
    job.addCacheFile(localStorePath.makeQualified(localFS.getUri(),
                                      localFS.getWorkingDirectory()).toUri());
    configuration.set(AdlsMigratorConstants.CONF_LABEL_SSL_TRUST_STORE_LOCATION,
                      localStorePath.getName());

    localStorePath = getLocalStorePath(sslConf,
                             AdlsMigratorConstants.CONF_LABEL_SSL_KEY_STORE_LOCATION);
    job.addCacheFile(localStorePath.makeQualified(localFS.getUri(),
                                      localFS.getWorkingDirectory()).toUri());
    configuration.set(AdlsMigratorConstants.CONF_LABEL_SSL_KEY_STORE_LOCATION,
                                      localStorePath.getName());

    job.addCacheFile(sslConfigPath.makeQualified(localFS.getUri(),
                                      localFS.getWorkingDirectory()).toUri());

  }

  /**
   * Get Local Trust store/key store path
   *
   * @param sslConf - Config from SSL Client xml
   * @param storeKey - Key for either trust store or key store
   * @return - Path where the store is present
   * @throws IOException -If any
   */
  private Path getLocalStorePath(Configuration sslConf, String storeKey) throws IOException {
    if (sslConf.get(storeKey) != null) {
      return new Path(sslConf.get(storeKey));
    } else {
      throw new IOException("Store for " + storeKey + " is not set in " +
          inputOptions.getSslConfigurationFile());
    }
  }

  /**
   * Setup output format appropriately
   *
   * @param job - Job handle
   * @throws IOException - Exception if any
   */
  private void configureOutputFormat(Job job) throws IOException {
    final Configuration configuration = job.getConfiguration();
    Path targetPath = inputOptions.getTransferAcls() ?
                        inputOptions.getTargetPath() : 
                        inputOptions.getDataBoxes()[0].getTargetPath();
    FileSystem targetFS = targetPath.getFileSystem(configuration);
                                          
    if (inputOptions.shouldPreserve(AdlsMigratorOptions.FileAttribute.ACL)) {
      AdlsMigratorUtils.checkFileSystemAclSupport(targetFS);
    }
    if (inputOptions.shouldPreserve(AdlsMigratorOptions.FileAttribute.XATTR)) {
      AdlsMigratorUtils.checkFileSystemXAttrSupport(targetFS);
    }
    CopyOutputFormat.setCommitDirectory(job, targetPath);

    Path logPath = inputOptions.getLogPath();
    if (logPath == null) {
      logPath = new Path(metaFolder, "_logs");
    } else {
      LOG.info("AdlsMigrator job log path: " + logPath);
    }
    CopyOutputFormat.setOutputPath(job, logPath);
  }

  /**
   * Create input listing by invoking an appropriate copy listing
   * implementation. Also add delegation tokens for each path
   * to job's credential store
   *
   * @param job - Handle to job
   * @return Returns the path where the copy listing is created
   * @throws IOException - If any
   */
  protected Path createInputFileListing(Job job) throws IOException {
    Path fileListingPath = getFileListingPath();
    CopyListing copyListing = CopyListing.getCopyListing(job.getConfiguration(),
        job.getCredentials(), inputOptions);
    copyListing.buildListing(fileListingPath, inputOptions);
    return fileListingPath;
  }

  /**
   * Create input listing based on snapshot diff report.
   * @param job - Handle to job
   * @param AdlsMigratorSync the class wraps the snapshot diff report
   * @return Returns the path where the copy listing is created
   * @throws IOException - If any
   */
  /*private Path createInputFileListingWithDiff(Job job, AdlsMigratorSync AdlsMigratorSync)
      throws IOException {
    Path fileListingPath = getFileListingPath();
    CopyListing copyListing = new SimpleCopyListing(job.getConfiguration(),
        job.getCredentials(), AdlsMigratorSync);
    copyListing.buildListing(fileListingPath, inputOptions);
    return fileListingPath;
  }*/

  /**
   * Get default name of the copy listing file. Use the meta folder
   * to create the copy listing file
   *
   * @return - Path where the copy listing file has to be saved
   * @throws IOException - Exception if any
   */
  protected Path getFileListingPath() throws IOException {
    String fileListPathStr = metaFolder + "/fileList.seq";
    Path path = new Path(fileListPathStr);
    return new Path(path.toUri().normalize().toString());
  }

  /**
   * Create a default working folder for the job, under the
   * job staging directory
   *
   * @return Returns the working folder information
   * @throws Exception - Exception if any
   */
  private Path createMetaFolderPath() throws Exception {
    try {
      Configuration configuration = getConf();
      Path stagingDir = JobSubmissionFiles.getStagingDir(
              new Cluster(configuration), configuration);
      Path metaFolderPath = new Path(stagingDir, PREFIX + String.valueOf(rand.nextInt()));
      if (LOG.isDebugEnabled())
        LOG.debug("Meta folder location: " + metaFolderPath);
      configuration.set(AdlsMigratorConstants.CONF_LABEL_META_FOLDER, metaFolderPath.toString());    
      return metaFolderPath;
    } catch (Exception ex) {
      LOG.error("Failed to create meta folder: ", ex);
      throw ex;
    }
  }

  /**
   * Main function of the AdlsMigrator program. Parses the input arguments (via OptionsParser),
   * and invokes the AdlsMigrator::run() method, via the ToolRunner.
   * @param argv Command-line arguments sent to AdlsMigrator.
   */
  public static void main(String argv[]) {
    int exitCode;
    try {
      AdlsMigrator AdlsMigrator = new AdlsMigrator();
      Cleanup cleanup = new Cleanup(AdlsMigrator);

      ShutdownHookManager.get().addShutdownHook(cleanup, SHUTDOWN_HOOK_PRIORITY);
      exitCode = ToolRunner.run(getDefaultConf(), AdlsMigrator, argv);
    }
    catch (Exception e) {
      LOG.error("Couldn't complete AdlsMigrator operation: ", e);
      exitCode = AdlsMigratorConstants.UNKNOWN_ERROR;
    }
    System.exit(exitCode);
  }

  /**
   * Loads properties from AdlsMigrator-default.xml into configuration
   * object
   * @return Configuration which includes properties from AdlsMigrator-default.xml
   */
  private static Configuration getDefaultConf() {
    Configuration config = new Configuration();
    config.addResource(AdlsMigrator_DEFAULT_XML);
    return config;
  }

  private synchronized void cleanup() {
    try {
      if (metaFolder != null) {
        if (jobFS != null) {
          jobFS.delete(metaFolder, true);
        }
        metaFolder = null;
      }
    } catch (IOException e) {
      LOG.error("Unable to cleanup meta folder: " + metaFolder, e);
    }
  }

  private boolean isSubmitted() {
    return submitted;
  }

  private static class Cleanup implements Runnable {
    private final AdlsMigrator AdlsMigrator;

    Cleanup(AdlsMigrator AdlsMigrator) {
      this.AdlsMigrator = AdlsMigrator;
    }

    @Override
    public void run() {
      if (AdlsMigrator.isSubmitted()) return;

      AdlsMigrator.cleanup();
    }
  }
}
