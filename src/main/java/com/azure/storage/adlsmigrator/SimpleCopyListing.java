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

import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions.FileAttribute;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;
import com.azure.storage.adlsmigrator.util.ProducerConsumer;
import com.azure.storage.adlsmigrator.util.WorkReport;
import com.azure.storage.adlsmigrator.util.WorkRequest;
import com.azure.storage.adlsmigrator.util.WorkRequestProcessor;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;

import com.google.common.annotations.VisibleForTesting;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.LinkedList;

import static com.azure.storage.adlsmigrator.AdlsMigratorConstants
        .HDFS_RESERVED_RAW_DIRECTORY_NAME;

/**
 * The SimpleCopyListing is responsible for making the exhaustive list of
 * all files/directories under its specified list of input-paths.
 * These are written into the specified copy-listing file.
 * Note: The SimpleCopyListing doesn't handle wild-cards in the input-paths.
 */
public class SimpleCopyListing extends CopyListing {
  private static final Log LOG = LogFactory.getLog(SimpleCopyListing.class);

  public static final int DEFAULT_FILE_STATUS_SIZE = 1000;
  public static final boolean DEFAULT_RANDOMIZE_FILE_LISTING = true;

  private long totalPaths = 0;
  private long totalDirs = 0;
  private long totalBytesToCopy = 0;
  private int numListstatusThreads = 1;
  private final int fileStatusLimit;
  private final boolean randomizeFileListing;
  private final int maxRetries = 3;
  private CopyFilter copyFilter;
  //private AdlsMigratorSync AdlsMigratorSync;
  private final Random rnd = new Random();

  /**
   * Protected constructor, to initialize configuration.
   *
   * @param configuration The input configuration, with which the source/target FileSystems may be accessed.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  protected SimpleCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
    numListstatusThreads = getConf().getInt(
        AdlsMigratorConstants.CONF_LABEL_LISTSTATUS_THREADS,
        AdlsMigratorConstants.DEFAULT_LISTSTATUS_THREADS);
    fileStatusLimit = Math.max(1, getConf()
        .getInt(AdlsMigratorConstants.CONF_LABEL_SIMPLE_LISTING_FILESTATUS_SIZE,
        DEFAULT_FILE_STATUS_SIZE));
    randomizeFileListing = getConf().getBoolean(
        AdlsMigratorConstants.CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES,
        DEFAULT_RANDOMIZE_FILE_LISTING);
    if (LOG.isDebugEnabled()) {
      LOG.debug("numListstatusThreads=" + numListstatusThreads
          + ", fileStatusLimit=" + fileStatusLimit
          + ", randomizeFileListing=" + randomizeFileListing);
    }
    copyFilter = CopyFilter.getCopyFilter(getConf());
    copyFilter.initialize();
  }

  @VisibleForTesting
  protected SimpleCopyListing(Configuration configuration,
                              Credentials credentials,
                              int numListstatusThreads,
                              int fileStatusLimit,
                              boolean randomizeFileListing) {
    super(configuration, credentials);
    this.numListstatusThreads = numListstatusThreads;
    this.fileStatusLimit = Math.max(1, fileStatusLimit);
    this.randomizeFileListing = randomizeFileListing;
  }

  @Override
  protected void validatePaths(AdlsMigratorOptions options)
      throws IOException, InvalidInputException {

    for (Path path: options.getSourcePaths()) {
      FileSystem fs = path.getFileSystem(getConf());
      if (!fs.exists(path)) {
        throw new InvalidInputException(path + " doesn't exist");
      }
    }

    /* This is requires to allow map tasks to access each of the source
       clusters. This would retrieve the delegation token for each unique
       file system and add them to job's private credential store
     */
    Credentials credentials = getCredentials();
    if (credentials != null) {
      Path[] inputPaths = options.getSourcePaths().toArray(new Path[1]);
      TokenCache.obtainTokensForNamenodes(credentials, inputPaths, getConf());
    }
  }

  @Override
  protected void doBuildListing(Path pathToListingFile,
                                AdlsMigratorOptions options) throws IOException {
    if(options.shouldUseSnapshotDiff()) {
      //doBuildListingWithSnapshotDiff(getWriter(pathToListingFile), options);
    }else {
      doBuildListing(getWriter(pathToListingFile), options);
    }
  }

  /**
   * Get a path with its scheme and authority.
   */
  /*private Path getPathWithSchemeAndAuthority(Path path) throws IOException {
    FileSystem fs= path.getFileSystem(getConf());
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      scheme = fs.getUri().getScheme();
    }

    String authority = path.toUri().getAuthority();
    if (authority == null) {
      authority = fs.getUri().getAuthority();
    }

    return new Path(scheme, authority, makeQualified(path).toUri().getPath());
  }*/

  /**
   * Write a single file/directory to the sequence file.
   * @throws IOException
   */
  /*private void addToFileListing(SequenceFile.Writer fileListWriter,
      Path sourceRoot, Path path, AdlsMigratorOptions options) throws IOException {
    sourceRoot = getPathWithSchemeAndAuthority(sourceRoot);
    path = getPathWithSchemeAndAuthority(path);
    path = makeQualified(path);

    FileSystem sourceFS = sourceRoot.getFileSystem(getConf());
    FileStatus fileStatus = sourceFS.getFileStatus(path);
    final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
    final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
    final boolean preserveRawXAttrs = options.shouldPreserveRawXattrs();
    LinkedList<CopyListingFileStatus> fileCopyListingStatus =
        AdlsMigratorUtils.toCopyListingFileStatus(sourceFS, fileStatus,
            preserveAcls, preserveXAttrs, preserveRawXAttrs,
            options.getBlocksPerChunk());
    writeToFileListingRoot(fileListWriter, fileCopyListingStatus,
        sourceRoot, options);
  }*/

  /**
   * Build a copy list based on the snapshot diff report.
   *
   * Any file/directory changed or created will be in the list. Deleted
   * files/directories will not be in the list, since they are handled by
   * {@link com.azure.storage.adlsmigrator.AdlsMigratorSync#sync}. An item can be
   * created/modified and renamed, in which case, the target path is put
   * into the list.
   * @throws IOException
   */
  /*@VisibleForTesting
  protected void doBuildListingWithSnapshotDiff(
      SequenceFile.Writer fileListWriter, AdlsMigratorOptions options)
      throws IOException {
    ArrayList<DiffInfo> diffList = AdlsMigratorSync.prepareDiffListForCopyListing();
    Path sourceRoot = options.getSourcePaths().get(0);
    FileSystem sourceFS = sourceRoot.getFileSystem(getConf());

    try {
      List<FileStatusInfo> fileStatuses = Lists.newArrayList();
      for (DiffInfo diff : diffList) {
        // add snapshot paths prefix
        diff.setTarget(
            new Path(options.getSourcePaths().get(0), diff.getTarget()));
        if (diff.getType() == SnapshotDiffReport.DiffType.MODIFY) {
          addToFileListing(fileListWriter,
              sourceRoot, diff.getTarget(), options);
        } else if (diff.getType() == SnapshotDiffReport.DiffType.CREATE) {
          addToFileListing(fileListWriter,
              sourceRoot, diff.getTarget(), options);

          FileStatus sourceStatus = sourceFS.getFileStatus(diff.getTarget());
          if (sourceStatus.isDirectory()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding source dir for traverse: " +
                  sourceStatus.getPath());
            }

            HashSet<String> excludeList =
                AdlsMigratorSync.getTraverseExcludeList(diff.getSource(),
                    options.getSourcePaths().get(0));

            ArrayList<FileStatus> sourceDirs = new ArrayList<>();
            sourceDirs.add(sourceStatus);

            traverseDirectory(fileListWriter, sourceFS, sourceDirs,
                sourceRoot, options, excludeList, fileStatuses);
          }
        }
      }
      if (randomizeFileListing) {
        writeToFileListing(fileStatuses, fileListWriter);
      }
      fileListWriter.close();
      fileListWriter = null;
    } finally {
      IOUtils.cleanup(LOG, fileListWriter);
    }
  }*/

  /**
   * Collect the list of 
   *   {@literal <sourceRelativePath, sourceFileStatus>}
   * to be copied and write to the sequence file. In essence, any file or
   * directory that need to be copied or sync-ed is written as an entry to the
   * sequence file, with the possible exception of the source root:
   *     when either -update (sync) or -overwrite switch is specified, and if
   *     the the source root is a directory, then the source root entry is not 
   *     written to the sequence file, because only the contents of the source
   *     directory need to be copied in this case.
   * See {@link com.azure.storage.adlsmigrator.util.AdlsMigratorUtils#getRelativePath} for
   *     how relative path is computed.
   * See computeSourceRootPath method for how the root path of the source is
   *     computed.
   * @param fileListWriter
   * @param options
   * @throws IOException
   */
  @VisibleForTesting
  protected void doBuildListing(SequenceFile.Writer fileListWriter,
      AdlsMigratorOptions options) throws IOException {
    if (options.getNumListstatusThreads() > 0) {
      numListstatusThreads = options.getNumListstatusThreads();
    }

    try {
      List<FileStatusInfo> statusList = Lists.newArrayList();
      for (Path path: options.getSourcePaths()) {
        FileSystem sourceFS = path.getFileSystem(getConf());
        final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
        final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
        final boolean preserveRawXAttrs = options.shouldPreserveRawXattrs();
        path = makeQualified(path);

        FileStatus rootStatus = sourceFS.getFileStatus(path);
        Path sourcePathRoot = computeSourceRootPath(rootStatus, options);

        FileStatus[] sourceFiles = sourceFS.listStatus(path);
        boolean explore = (sourceFiles != null && sourceFiles.length > 0);
        if (!explore || rootStatus.isDirectory()) {
          LinkedList<CopyListingFileStatus> rootCopyListingStatus =
              AdlsMigratorUtils.toCopyListingFileStatus(sourceFS, rootStatus,
                  preserveAcls, preserveXAttrs, preserveRawXAttrs);
          writeToFileListingRoot(fileListWriter, rootCopyListingStatus,
              sourcePathRoot, options);
        }
        if (explore) {
          ArrayList<FileStatus> sourceDirs = new ArrayList<FileStatus>();
          for (FileStatus sourceStatus: sourceFiles) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Recording source-path: " + sourceStatus.getPath() + " for copy.");
            }
            LinkedList<CopyListingFileStatus> sourceCopyListingStatus =
                AdlsMigratorUtils.toCopyListingFileStatus(sourceFS, sourceStatus,
                    preserveAcls && sourceStatus.isDirectory(),
                    preserveXAttrs && sourceStatus.isDirectory(),
                    preserveRawXAttrs && sourceStatus.isDirectory());
            for (CopyListingFileStatus fs : sourceCopyListingStatus) {
              if (randomizeFileListing) {
                addToFileListing(statusList,
                    new FileStatusInfo(fs, sourcePathRoot), fileListWriter);
              } else {
                writeToFileListing(fileListWriter, fs, sourcePathRoot);
              }
            }
            if (sourceStatus.isDirectory()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Adding source dir for traverse: " + sourceStatus.getPath());
              }
              sourceDirs.add(sourceStatus);
            }
          }
          traverseDirectory(fileListWriter, sourceFS, sourceDirs,
              sourcePathRoot, options, null, statusList);
        }
      }
      if (randomizeFileListing) {
        writeToFileListing(statusList, fileListWriter);
      }
      fileListWriter.close();
      printStats();
      LOG.info("Build file listing completed.");
      fileListWriter = null;
    } finally {
      IOUtils.cleanup(LOG, fileListWriter);
    }
  }

  private void addToFileListing(List<FileStatusInfo> fileStatusInfoList,
      FileStatusInfo statusInfo, SequenceFile.Writer fileListWriter)
      throws IOException {
    fileStatusInfoList.add(statusInfo);
    if (fileStatusInfoList.size() > fileStatusLimit) {
      writeToFileListing(fileStatusInfoList, fileListWriter);
    }
  }

  @VisibleForTesting
  void setSeedForRandomListing(long seed) {
    this.rnd.setSeed(seed);
  }

  private void writeToFileListing(List<FileStatusInfo> fileStatusInfoList,
      SequenceFile.Writer fileListWriter) throws IOException {
    /**
     * In cloud storage systems, it is possible to get region hotspot.
     * Shuffling paths can avoid such cases and also ensure that
     * some mappers do not get lots of similar paths.
     */
    Collections.shuffle(fileStatusInfoList, rnd);
    for (FileStatusInfo fileStatusInfo : fileStatusInfoList) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding " + fileStatusInfo.fileStatus.getPath());
      }
      writeToFileListing(fileListWriter, fileStatusInfo.fileStatus,
          fileStatusInfo.sourceRootPath);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Number of paths written to fileListing="
          + fileStatusInfoList.size());
    }
    fileStatusInfoList.clear();
  }

  private static class FileStatusInfo {
    private CopyListingFileStatus fileStatus;
    private Path sourceRootPath;

    FileStatusInfo(CopyListingFileStatus fileStatus, Path sourceRootPath) {
      this.fileStatus = fileStatus;
      this.sourceRootPath = sourceRootPath;
    }
  }

  private Path computeSourceRootPath(FileStatus sourceStatus,
                                     AdlsMigratorOptions options) throws IOException {

    boolean solitaryFile = options.getSourcePaths().size() == 1
                                                && !sourceStatus.isDirectory();

    if (solitaryFile) {
      return sourceStatus.getPath();
    } else {
      boolean specialHandling = options.getSourcePaths().size() == 1 ||
          options.shouldSyncFolder() || options.shouldOverwrite();

      if ((specialHandling && sourceStatus.isDirectory()) ||
          sourceStatus.getPath().isRoot()) {
        return sourceStatus.getPath();
      } else {
        return sourceStatus.getPath().getParent();
      }
    }
  }

  /**
   * Provide an option to skip copy of a path, Allows for exclusion
   * of files such as {@link org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#SUCCEEDED_FILE_NAME}
   * @param path - Path being considered for copy while building the file listing
   * @return - True if the path should be considered for copy, false otherwise
   */
  protected boolean shouldCopy(Path path) {
    return copyFilter.shouldCopy(path);
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return totalBytesToCopy;
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return totalPaths;
  }

  private Path makeQualified(Path path) throws IOException {
    final FileSystem fs = path.getFileSystem(getConf());
    return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  private SequenceFile.Writer getWriter(Path pathToListFile) throws IOException {
    FileSystem fs = pathToListFile.getFileSystem(getConf());
    fs.delete(pathToListFile, false);
    return SequenceFile.createWriter(getConf(),
            SequenceFile.Writer.file(pathToListFile),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
            SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
  }

  /*
   *  Private class to implement WorkRequestProcessor interface. It processes
   *  each directory (represented by FileStatus item) and returns a list of all
   *  file-system objects in that directory (files and directories). In case of
   *  retriable exceptions it increments retry counter and returns the same
   *  directory for later retry.
   */
  private static class FileStatusProcessor
      implements WorkRequestProcessor<FileStatus, FileStatus[]> {
    private FileSystem fileSystem;
    private HashSet<String> excludeList;

    public FileStatusProcessor(FileSystem fileSystem,
                               HashSet<String> excludeList) {
      this.fileSystem = fileSystem;
      this.excludeList = excludeList;
    }

    /**
     * Get FileStatuses for a given path.
     * Exclude the some renamed FileStatuses since they are already handled by
     * {@link com.azure.storage.adlsmigrator.AdlsMigratorSync#sync}.
     * @return an array of file status
     */
    private FileStatus[] getFileStatus(Path path) throws IOException {
      FileStatus[] fileStatuses = fileSystem.listStatus(path);
      if (excludeList != null && excludeList.size() > 0) {
        ArrayList<FileStatus> fileStatusList = new ArrayList<>();
        for(FileStatus status : fileStatuses) {
          if (!excludeList.contains(status.getPath().toUri().getPath())) {
            fileStatusList.add(status);
          }
        }
        fileStatuses = fileStatusList.toArray(
                new FileStatus[fileStatusList.size()]);
      }
      return fileStatuses;
    }

    /*
     *  Processor for FileSystem.listStatus().
     *
     *  @param workRequest  Input work item that contains FileStatus item which
     *                      is a parent directory we want to list.
     *  @return Outputs WorkReport<FileStatus[]> with a list of objects in the
     *          directory (array of objects, empty if parent directory is
     *          empty). In case of intermittent exception we increment retry
     *          counter and return the list containing the parent directory).
     */
    public WorkReport<FileStatus[]> processItem(
        WorkRequest<FileStatus> workRequest) {
      FileStatus parent = workRequest.getItem();
      int retry = workRequest.getRetry();
      WorkReport<FileStatus[]> result = null;
      try {
        if (retry > 0) {
          int sleepSeconds = 2;
          for (int i = 1; i < retry; i++) {
            sleepSeconds *= 2;
          }
          try {
            Thread.sleep(1000 * sleepSeconds);
          } catch (InterruptedException ie) {
            LOG.debug("Interrupted while sleeping in exponential backoff.");
          }
        }
        result = new WorkReport<FileStatus[]>(getFileStatus(parent.getPath()),
                retry, true);
      } catch (FileNotFoundException fnf) {
        LOG.error("FileNotFoundException exception in listStatus: " +
                  fnf.getMessage());
        result = new WorkReport<FileStatus[]>(new FileStatus[0], retry, true,
                                              fnf);
      } catch (Exception e) {
        LOG.error("Exception in listStatus. Will send for retry.");
        FileStatus[] parentList = new FileStatus[1];
        parentList[0] = parent;
        result = new WorkReport<FileStatus[]>(parentList, retry + 1, false, e);
      }
      return result;
    }
  }

  private void printStats() {
    LOG.info("Paths (files+dirs) cnt = " + totalPaths +
             "; dirCnt = " + totalDirs);
  }

  private void maybePrintStats() {
    if (totalPaths % 100000 == 0) {
      printStats();
    }
  }

  private void traverseDirectory(SequenceFile.Writer fileListWriter,
                                 FileSystem sourceFS,
                                 ArrayList<FileStatus> sourceDirs,
                                 Path sourcePathRoot,
                                 AdlsMigratorOptions options,
                                 HashSet<String> excludeList,
                                 List<FileStatusInfo> fileStatuses)
                                 throws IOException {
    final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
    final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
    final boolean preserveRawXattrs = options.shouldPreserveRawXattrs();

    assert numListstatusThreads > 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting thread pool of " + numListstatusThreads +
          " listStatus workers.");
    }
    ProducerConsumer<FileStatus, FileStatus[]> workers =
        new ProducerConsumer<FileStatus, FileStatus[]>(numListstatusThreads);
    for (int i = 0; i < numListstatusThreads; i++) {
      workers.addWorker(
          new FileStatusProcessor(sourcePathRoot.getFileSystem(getConf()),
              excludeList));
    }

    for (FileStatus status : sourceDirs) {
      workers.put(new WorkRequest<FileStatus>(status, 0));
    }

    while (workers.hasWork()) {
      try {
        WorkReport<FileStatus[]> workResult = workers.take();
        int retry = workResult.getRetry();
        for (FileStatus child: workResult.getItem()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Recording source-path: " + child.getPath() + " for copy.");
          }
          if (workResult.getSuccess()) {
            LinkedList<CopyListingFileStatus> childCopyListingStatus =
              AdlsMigratorUtils.toCopyListingFileStatus(sourceFS, child,
                preserveAcls && child.isDirectory(),
                preserveXAttrs && child.isDirectory(),
                preserveRawXattrs && child.isDirectory());

            for (CopyListingFileStatus fs : childCopyListingStatus) {
              if (randomizeFileListing) {
                addToFileListing(fileStatuses,
                    new FileStatusInfo(fs, sourcePathRoot), fileListWriter);
              } else {
                writeToFileListing(fileListWriter, fs, sourcePathRoot);
              }
            }
          }
          if (retry < maxRetries) {
            if (child.isDirectory()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Traversing into source dir: " + child.getPath());
              }
              workers.put(new WorkRequest<FileStatus>(child, retry));
            }
          } else {
            LOG.error("Giving up on " + child.getPath() +
                      " after " + retry + " retries.");
          }
        }
      } catch (InterruptedException ie) {
        LOG.error("Could not get item from childQueue. Retrying...");
      }
    }
    workers.shutdown();
  }

  private void writeToFileListingRoot(SequenceFile.Writer fileListWriter,
      LinkedList<CopyListingFileStatus> fileStatus, Path sourcePathRoot,
      AdlsMigratorOptions options) throws IOException {
    boolean syncOrOverwrite = options.shouldSyncFolder() ||
        options.shouldOverwrite();
    for (CopyListingFileStatus fs : fileStatus) {
      if (fs.getPath().equals(sourcePathRoot) &&
          fs.isDirectory() && syncOrOverwrite) {
        // Skip the root-paths when syncOrOverwrite
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip " + fs.getPath());
        }
        return;
      }
      writeToFileListing(fileListWriter, fs, sourcePathRoot);
    }
  }

  private void writeToFileListing(SequenceFile.Writer fileListWriter,
                                  CopyListingFileStatus fileStatus,
                                  Path sourcePathRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("REL PATH: " + AdlsMigratorUtils.getRelativePath(sourcePathRoot,
        fileStatus.getPath()) + ", FULL PATH: " + fileStatus.getPath());
    }

    if (!shouldCopy(fileStatus.getPath())) {
      return;
    }

    fileListWriter.append(new Text(AdlsMigratorUtils.getRelativePath(sourcePathRoot,
        fileStatus.getPath())), fileStatus);
    fileListWriter.sync();

    if (!fileStatus.isDirectory()) {
      totalBytesToCopy += fileStatus.getSizeToCopy();
    } else {
      totalDirs++;
    }
    totalPaths++;
    maybePrintStats();
  }
}
