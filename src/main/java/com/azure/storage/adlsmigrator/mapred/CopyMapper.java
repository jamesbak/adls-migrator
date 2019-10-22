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

/**
 * Mapper class that executes the AdlsMigrator copy operation.
 * Implements the o.a.h.mapreduce.Mapper interface.
 */
public class CopyMapper extends MapperBase {

  /**
   * Indicate the action for each file
   */
  static enum FileAction {
    SKIP,         // Skip copying the file since it's already in the target FS
    APPEND,       // Only need to append new data to the file in the target FS 
    OVERWRITE,    // Overwrite the whole file
  }

  private static Log LOG = LogFactory.getLog(CopyMapper.class);

  private boolean syncFolders = false;
  private boolean skipCrc = false;
  private boolean overWrite = false;
  private boolean append = false;
  private EnumSet<FileAttribute> preserve = EnumSet.noneOf(FileAttribute.class);

  private FileSystem targetFS = null;
  private Path targetDataBoxPath;

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

    syncFolders = conf.getBoolean(AdlsMigratorOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false);
    skipCrc = conf.getBoolean(AdlsMigratorOptionSwitch.SKIP_CRC.getConfigLabel(), false);
    overWrite = conf.getBoolean(AdlsMigratorOptionSwitch.OVERWRITE.getConfigLabel(), false);
    append = conf.getBoolean(AdlsMigratorOptionSwitch.APPEND.getConfigLabel(), false);
    preserve = AdlsMigratorUtils.unpackAttributes(conf.get(AdlsMigratorOptionSwitch.
        PRESERVE_STATUS.getConfigLabel()));

    targetDataBoxPath = ((DataBoxSplit.TaskSplit)context.getInputSplit()).getDataBoxBaseUri();
    targetFS = targetDataBoxPath.getFileSystem(conf);
  }

  /**
   * Implementation of the Mapper::map(). Does the copy.
   * @param relPath The target path.
   * @param sourceFileStatus The source path.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void map(Text relPath, CopyListingFileStatus sourceFileStatus,
          Context context) throws IOException, InterruptedException {
    Path sourcePath = sourceFileStatus.getPath();
    
    if (LOG.isDebugEnabled())
      LOG.debug("CopyMapper::map(): Received " + sourcePath + ", " + relPath);

    Path target = new Path(targetDataBoxPath, relPath.toString());

    EnumSet<AdlsMigratorOptions.FileAttribute> fileAttributes
            = getFileAttributeSettings(context);
    final boolean preserveRawXattrs = context.getConfiguration().getBoolean(
        AdlsMigratorConstants.CONF_LABEL_PRESERVE_RAWXATTRS, false);

    final String description = "Copying " + sourcePath + " to " + target;
    context.setStatus(description);

    LOG.info(description);

    try {
      CopyListingFileStatus sourceCurrStatus;
      FileSystem sourceFS;
      try {
        sourceFS = sourcePath.getFileSystem(conf);
        final boolean preserveXAttrs =
            fileAttributes.contains(FileAttribute.XATTR);
        sourceCurrStatus = AdlsMigratorUtils.toCopyListingFileStatusHelper(sourceFS,
            sourceFS.getFileStatus(sourcePath),
            fileAttributes.contains(FileAttribute.ACL),
            preserveXAttrs, preserveRawXattrs,
            sourceFileStatus.getChunkOffset(),
            sourceFileStatus.getChunkLength());
      } catch (FileNotFoundException e) {
        throw new IOException(new RetriableFileCopyCommand.CopyReadException(e));
      }

      FileStatus targetStatus = null;

      try {
        targetStatus = targetFS.getFileStatus(target);
      } catch (FileNotFoundException ignore) {
        if (LOG.isDebugEnabled())
          LOG.debug("Path could not be found: " + target, ignore);
      }

      if (targetStatus != null &&
          (targetStatus.isDirectory() != sourceCurrStatus.isDirectory())) {
        throw new IOException("Can't replace " + target + ". Target is " +
            getFileType(targetStatus) + ", Source is " + getFileType(sourceCurrStatus));
      }

      if (sourceCurrStatus.isDirectory()) {
        createTargetDirsWithRetry(description, target, context);
      } else {
        FileAction action = checkUpdate(sourceFS, sourceCurrStatus, target, targetStatus);

        if (action == FileAction.SKIP) {
          LOG.info("Skipping copy of " + sourceCurrStatus.getPath()
                  + " to " + target);
          updateSkipCounters(context, sourceCurrStatus);
          context.write(null, new Text("SKIP: " + sourceCurrStatus.getPath()));

          if (verboseLog) {
            context.write(null,
                new Text("FILE_SKIPPED: source=" + sourceFileStatus.getPath()
                + ", size=" + sourceFileStatus.getLen() + " --> "
                + "target=" + target + ", size=" + (targetStatus == null ?
                    0 : targetStatus.getLen())));
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("copying " + sourceCurrStatus + " " + target);
          }
          copyFileWithRetry(description, sourceCurrStatus, target,
              targetStatus, context, action, fileAttributes);
        }
      }
      AdlsMigratorUtils.preserve(
        target.getFileSystem(conf), 
        target,
        sourceCurrStatus, 
        fileAttributes, 
        preserveRawXattrs,
        null);
    } catch (IOException exception) {
      handleFailures(exception, sourceFileStatus, target, context, false);
    }
  }

  private static EnumSet<AdlsMigratorOptions.FileAttribute>
          getFileAttributeSettings(Mapper.Context context) {
    String attributeString = context.getConfiguration().get(
            AdlsMigratorOptionSwitch.PRESERVE_STATUS.getConfigLabel());
    return AdlsMigratorUtils.unpackAttributes(attributeString);
  }

  private void copyFileWithRetry(String description,
      CopyListingFileStatus sourceFileStatus, Path target,
      FileStatus targrtFileStatus, Context context, FileAction action,
      EnumSet<AdlsMigratorOptions.FileAttribute> fileAttributes)
      throws IOException, InterruptedException {
    long bytesCopied;
    try {
      bytesCopied = (Long) new RetriableFileCopyCommand(skipCrc, description,
          action).execute(sourceFileStatus, target, context, fileAttributes);
    } catch (Exception e) {
      context.setStatus("Copy Failure: " + sourceFileStatus.getPath());
      throw new IOException("File copy failed: " + sourceFileStatus.getPath() +
          " --> " + target, e);
    }
    incrementCounter(context, Counter.BYTESEXPECTED, sourceFileStatus.getLen());
    incrementCounter(context, Counter.BYTESCOPIED, bytesCopied);
    incrementCounter(context, Counter.COPY, 1);

    if (verboseLog) {
      context.write(null,
          new Text("FILE_COPIED: source=" + sourceFileStatus.getPath() + ","
          + " size=" + sourceFileStatus.getLen() + " --> " + "target="
          + target + ", size=" + (targrtFileStatus == null ?
              0 : targrtFileStatus.getLen())));
    }
  }

  private void createTargetDirsWithRetry(String description,
                   Path target, Context context) throws IOException {
    try {
      new RetriableDirectoryCreateCommand(description).execute(target, context);
    } catch (Exception e) {
      throw new IOException("mkdir failed for " + target, e);
    }
    incrementCounter(context, Counter.DIR_COPY, 1);
  }

  private static void updateSkipCounters(Context context,
      CopyListingFileStatus sourceFile) {
    incrementCounter(context, Counter.SKIP, 1);
    incrementCounter(context, Counter.BYTESSKIPPED, sourceFile.getLen());

  }

  private FileAction checkUpdate(FileSystem sourceFS,
      CopyListingFileStatus source, Path target, FileStatus targetFileStatus)
      throws IOException {
    if (targetFileStatus != null && !overWrite) {
      if (canSkip(sourceFS, source, targetFileStatus)) {
        return FileAction.SKIP;
      } else if (append) {
        long targetLen = targetFileStatus.getLen();
        if (targetLen < source.getLen()) {
          FileChecksum sourceChecksum = sourceFS.getFileChecksum(
              source.getPath(), targetLen);
          if (sourceChecksum != null
              && sourceChecksum.equals(targetFS.getFileChecksum(target))) {
            // We require that the checksum is not null. Thus currently only
            // DistributedFileSystem is supported
            return FileAction.APPEND;
          }
        }
      }
    }
    return FileAction.OVERWRITE;
  }

  private boolean canSkip(FileSystem sourceFS, CopyListingFileStatus source,
      FileStatus target) throws IOException {
    
    if (target == null) {
      return false;
    }
    if (!syncFolders) {
      return true;
    }
    boolean sameLength = target.getLen() == source.getLen();
    boolean sameBlockSize = source.getBlockSize() == target.getBlockSize()
        || !preserve.contains(FileAttribute.BLOCKSIZE);
    if (sameLength && sameBlockSize) {
      return skipCrc ||
          AdlsMigratorUtils.checksumsAreEqual(sourceFS, source.getPath(), null,
              targetFS, target.getPath());
    } else {
      return false;
    }
  }
}
