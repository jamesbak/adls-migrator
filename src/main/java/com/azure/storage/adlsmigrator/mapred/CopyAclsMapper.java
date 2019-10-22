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
import java.util.HashSet;
import java.util.Map;

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
import com.azure.storage.adlsmigrator.IdentityMap;
import com.azure.storage.adlsmigrator.AdlsMigratorConstants;
import com.azure.storage.adlsmigrator.AdlsMigratorOptionSwitch;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions.FileAttribute;
import com.azure.storage.adlsmigrator.mapred.RetriableFileCopyCommand.CopyReadException;
import com.azure.storage.adlsmigrator.util.AdlsMigratorUtils;
import com.azure.storage.adlsmigrator.mapred.lib.DataBoxSplit;
import org.apache.hadoop.util.StringUtils;

/**
 * Mapper class that executes the AdlsMigrator copy ACLs operation.
 * Implements the o.a.h.mapreduce.Mapper interface.
 */
public class CopyAclsMapper extends MapperBase {

  private static Log LOG = LogFactory.getLog(CopyAclsMapper.class);

  private FileSystem targetFS = null;
  private Path targetBasePath;
  private Map<String, IdentityMap> identities;

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

    targetBasePath = new Path(conf.get(AdlsMigratorConstants.CONF_LABEL_TARGET_FINAL_PATH));
    targetFS = targetBasePath.getFileSystem(conf);
    // Read in the identities map, which we will use to translate kerberos identities to AAD
    identities = IdentityMap.readFromJsonFile(
      IdentityMap.getIdentitiesMapFile(conf), 
      conf);
  }

  /**
   * Implementation of the Mapper::map(). Does the copy.
   * @param relPath The target path.
   * @param sourceFileStatus The source path.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void map(Text relPath, CopyListingFileStatus sourceFileStatus, Context context) 
                  throws IOException, InterruptedException {
    Path sourcePath = sourceFileStatus.getPath();
    
    if (LOG.isDebugEnabled())
      LOG.debug("CopyAclsMapper::map(): Received " + sourcePath + ", " + relPath);

    Path target = new Path(targetBasePath + relPath.toString());

    final String description = "Copying ACLS: " + sourcePath + " to " + target;
    context.setStatus(description);
    LOG.info(description);

    try {
      CopyListingFileStatus sourceCurrStatus;
      FileSystem sourceFS;
      try {
        sourceFS = sourcePath.getFileSystem(conf);
        sourceCurrStatus = AdlsMigratorUtils.toCopyListingFileStatusHelper(sourceFS,
            sourceFS.getFileStatus(sourcePath),
            true,
            false, false,
            sourceFileStatus.getChunkOffset(),
            sourceFileStatus.getChunkLength());
      } catch (FileNotFoundException e) {
        throw new IOException(new RetriableFileCopyCommand.CopyReadException(e));
      }

      FileStatus targetStatus = null;

      try {
        targetStatus = targetFS.getFileStatus(target);
      } catch (FileNotFoundException ex) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Path could not be found: " + target, ex);
        }
        throw new IOException(new RetriableFileCopyCommand.CopyReadException(ex));
      }

      if (targetStatus.isDirectory() != sourceCurrStatus.isDirectory()) {
        throw new IOException("Can't copy ACLs: " + target + ". Target is " +
            getFileType(targetStatus) + ", Source is " + getFileType(sourceCurrStatus));
      }

      AdlsMigratorUtils.preserve(
        target.getFileSystem(conf), 
        target,
        sourceCurrStatus, 
        EnumSet.of(FileAttribute.USER, FileAttribute.GROUP, FileAttribute.ACL), 
        false,
        identities);
      incrementCounter(context, Counter.COPY, 1);
    } catch (IOException exception) {
      handleFailures(exception, sourceFileStatus, target, context, true);
    }
  }
}
