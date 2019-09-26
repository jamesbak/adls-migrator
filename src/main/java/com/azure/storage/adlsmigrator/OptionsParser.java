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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import com.azure.storage.adlsmigrator.AdlsMigratorOptions.FileAttribute;

import com.google.common.base.Preconditions;

/**
 * The OptionsParser parses out the command-line options passed to AdlsMigrator,
 * and interprets those specific to AdlsMigrator, to create an Options object.
 */
public class OptionsParser {

  static final Log LOG = LogFactory.getLog(OptionsParser.class);

  private static final Options cliOptions = new Options();

  static {
    for (AdlsMigratorOptionSwitch option : AdlsMigratorOptionSwitch.values()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding option " + option.getOption());
      }
      cliOptions.addOption(option.getOption());
    }
  }

  private static class CustomParser extends GnuParser {
    @Override
    protected String[] flatten(Options options, String[] arguments, boolean stopAtNonOption) {
      for (int index = 0; index < arguments.length; index++) {
        if (arguments[index].equals("-" + AdlsMigratorOptionSwitch.PRESERVE_STATUS.getSwitch())) {
          arguments[index] = AdlsMigratorOptionSwitch.PRESERVE_STATUS_DEFAULT;
        }
      }
      return super.flatten(options, arguments, stopAtNonOption);
    }
  }

  private static void checkSnapshotsArgs(final String[] snapshots) {
    Preconditions.checkArgument(snapshots != null && snapshots.length == 2
        && !StringUtils.isBlank(snapshots[0])
        && !StringUtils.isBlank(snapshots[1]),
        "Must provide both the starting and ending snapshot names");
  }

  /**
   * The parse method parses the command-line options, and creates
   * a corresponding Options object.
   * @param args Command-line arguments (excluding the options consumed
   *              by the GenericOptionsParser).
   * @return The Options object, corresponding to the specified command-line.
   * @throws IllegalArgumentException Thrown if the parse fails.
   */
  public static AdlsMigratorOptions parse(String[] args)
      throws IllegalArgumentException {

    CommandLineParser parser = new CustomParser();

    CommandLine command;
    try {
      command = parser.parse(cliOptions, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Unable to parse arguments. " +
        Arrays.toString(args), e);
    }

    AdlsMigratorOptions option = parseSourceAndTargetPaths(command);

    //Process all the other option switches and set options appropriately
    if (command.hasOption(AdlsMigratorOptionSwitch.IGNORE_FAILURES.getSwitch())) {
      option.setIgnoreFailures(true);
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.ATOMIC_COMMIT.getSwitch())) {
      option.setAtomicCommit(true);
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.WORK_PATH.getSwitch()) &&
        option.shouldAtomicCommit()) {
      String workPath = getVal(command, AdlsMigratorOptionSwitch.WORK_PATH.getSwitch());
      if (workPath != null && !workPath.isEmpty()) {
        option.setAtomicWorkPath(new Path(workPath));
      }
    } else if (command.hasOption(AdlsMigratorOptionSwitch.WORK_PATH.getSwitch())) {
      throw new IllegalArgumentException("-tmp work-path can only be specified along with -atomic");
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.LOG_PATH.getSwitch())) {
      option.setLogPath(new Path(getVal(command, AdlsMigratorOptionSwitch.LOG_PATH.getSwitch())));
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.SYNC_FOLDERS.getSwitch())) {
      option.setSyncFolder(true);
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.OVERWRITE.getSwitch())) {
      option.setOverwrite(true);
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.APPEND.getSwitch())) {
      option.setAppend(true);
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.DELETE_MISSING.getSwitch())) {
      option.setDeleteMissing(true);
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.SKIP_CRC.getSwitch())) {
      option.setSkipCRC(true);
    }

    if (command.hasOption(AdlsMigratorOptionSwitch.BLOCKING.getSwitch())) {
      option.setBlocking(false);
    }

    parseBandwidth(command, option);

    if (command.hasOption(AdlsMigratorOptionSwitch.SSL_CONF.getSwitch())) {
      option.setSslConfigurationFile(command.
          getOptionValue(AdlsMigratorOptionSwitch.SSL_CONF.getSwitch()));
    }

    parseNumListStatusThreads(command, option);

    parseMaxMaps(command, option);

    if (command.hasOption(AdlsMigratorOptionSwitch.COPY_STRATEGY.getSwitch())) {
      option.setCopyStrategy(
            getVal(command, AdlsMigratorOptionSwitch.COPY_STRATEGY.getSwitch()));
    }

    parsePreserveStatus(command, option);

    if (command.hasOption(AdlsMigratorOptionSwitch.DIFF.getSwitch())) {
      String[] snapshots = getVals(command,
          AdlsMigratorOptionSwitch.DIFF.getSwitch());
      checkSnapshotsArgs(snapshots);
      option.setUseDiff(snapshots[0], snapshots[1]);
    }
    if (command.hasOption(AdlsMigratorOptionSwitch.RDIFF.getSwitch())) {
      String[] snapshots = getVals(command,
          AdlsMigratorOptionSwitch.RDIFF.getSwitch());
      checkSnapshotsArgs(snapshots);
      option.setUseRdiff(snapshots[0], snapshots[1]);
    }

    parseFileLimit(command);

    parseSizeLimit(command);

    if (command.hasOption(AdlsMigratorOptionSwitch.FILTERS.getSwitch())) {
      option.setFiltersFile(getVal(command,
          AdlsMigratorOptionSwitch.FILTERS.getSwitch()));
    }

    parseBlocksPerChunk(command, option);

    parseCopyBufferSize(command, option);

    if (command.hasOption(AdlsMigratorOptionSwitch.VERBOSE_LOG.getSwitch())) {
      option.setVerboseLog(true);
    }

    return option;
  }


  /**
   * A helper method to parse chunk size in number of blocks.
   * Used when breaking large file into chunks to copy in parallel.
   *
   * @param command command line arguments
   */
  private static void parseBlocksPerChunk(CommandLine command,
      AdlsMigratorOptions option) {
    boolean hasOption =
        command.hasOption(AdlsMigratorOptionSwitch.BLOCKS_PER_CHUNK.getSwitch());
    LOG.info("parseChunkSize: " +
        AdlsMigratorOptionSwitch.BLOCKS_PER_CHUNK.getSwitch() + " " + hasOption);
    if (hasOption) {
      String chunkSizeString = getVal(command,
          AdlsMigratorOptionSwitch.BLOCKS_PER_CHUNK.getSwitch().trim());
      try {
        int csize = Integer.parseInt(chunkSizeString);
        if (csize < 0) {
          csize = 0;
        }
        LOG.info("Set adlsmigrator blocksPerChunk to " + csize);
        option.setBlocksPerChunk(csize);
      }
      catch (NumberFormatException e) {
        throw new IllegalArgumentException("blocksPerChunk is invalid: "
            + chunkSizeString, e);
      }
    }
  }

  /**
   * A helper method to parse copyBufferSize.
   *
   * @param command command line arguments
   */
  private static void parseCopyBufferSize(CommandLine command,
      AdlsMigratorOptions option) {
    if (command.hasOption(AdlsMigratorOptionSwitch.COPY_BUFFER_SIZE.getSwitch())) {
      String copyBufferSizeStr =
          getVal(command, AdlsMigratorOptionSwitch.COPY_BUFFER_SIZE.getSwitch()
              .trim());
      try {
        int copyBufferSize = Integer.parseInt(copyBufferSizeStr);
        option.setCopyBufferSize(copyBufferSize);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("copyBufferSize is invalid: "
            + copyBufferSizeStr, e);
      }
    }
  }

  /**
   * parseSizeLimit is a helper method for parsing the deprecated argument
   * SIZE_LIMIT.
   *
   * @param command command line arguments
   */
  private static void parseSizeLimit(CommandLine command) {
    if (command.hasOption(AdlsMigratorOptionSwitch.SIZE_LIMIT.getSwitch())) {
      String sizeLimitString = getVal(command,
                              AdlsMigratorOptionSwitch.SIZE_LIMIT.getSwitch().trim());
      try {
        Long.parseLong(sizeLimitString);
      }
      catch (NumberFormatException e) {
        throw new IllegalArgumentException("Size-limit is invalid: "
                                            + sizeLimitString, e);
      }
      LOG.warn(AdlsMigratorOptionSwitch.SIZE_LIMIT.getSwitch() + " is a deprecated" +
              " option. Ignoring.");
    }
  }

  /**
   * parseFileLimit is a helper method for parsing the deprecated
   * argument FILE_LIMIT.
   *
   * @param command command line arguments
   */
  private static void parseFileLimit(CommandLine command) {
    if (command.hasOption(AdlsMigratorOptionSwitch.FILE_LIMIT.getSwitch())) {
      String fileLimitString = getVal(command,
                              AdlsMigratorOptionSwitch.FILE_LIMIT.getSwitch().trim());
      try {
        Integer.parseInt(fileLimitString);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("File-limit is invalid: "
                                            + fileLimitString, e);
      }
      LOG.warn(AdlsMigratorOptionSwitch.FILE_LIMIT.getSwitch() + " is a deprecated" +
          " option. Ignoring.");
    }
  }

  /**
   * parsePreserveStatus is a helper method for parsing PRESERVE_STATUS.
   *
   * @param command command line arguments
   * @param option  parsed adlsmigrator options
   */
  private static void parsePreserveStatus(CommandLine command,
                                          AdlsMigratorOptions option) {
    if (command.hasOption(AdlsMigratorOptionSwitch.PRESERVE_STATUS.getSwitch())) {
      String attributes =
          getVal(command, AdlsMigratorOptionSwitch.PRESERVE_STATUS.getSwitch());
      if (attributes == null || attributes.isEmpty()) {
        for (FileAttribute attribute : FileAttribute.values()) {
          option.preserve(attribute);
        }
      } else {
        for (int index = 0; index < attributes.length(); index++) {
          option.preserve(FileAttribute.
              getAttribute(attributes.charAt(index)));
        }
      }
    }
  }

  /**
   * parseMaxMaps is a helper method for parsing MAX_MAPS.
   *
   * @param command command line arguments
   * @param option  parsed adlsmigrator options
   */
  private static void parseMaxMaps(CommandLine command,
                                   AdlsMigratorOptions option) {
    if (command.hasOption(AdlsMigratorOptionSwitch.MAX_MAPS.getSwitch())) {
      try {
        Integer maps = Integer.parseInt(
            getVal(command, AdlsMigratorOptionSwitch.MAX_MAPS.getSwitch()).trim());
        option.setMaxMaps(maps);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Number of maps is invalid: " +
            getVal(command, AdlsMigratorOptionSwitch.MAX_MAPS.getSwitch()), e);
      }
    }
  }

  /**
   * parseNumListStatusThreads is a helper method for parsing
   * NUM_LISTSTATUS_THREADS.
   *
   * @param command command line arguments
   * @param option  parsed adlsmigrator options
   */
  private static void parseNumListStatusThreads(CommandLine command,
                                                AdlsMigratorOptions option) {
    if (command.hasOption(
        AdlsMigratorOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch())) {
      try {
        Integer numThreads = Integer.parseInt(getVal(command,
              AdlsMigratorOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()).trim());
        option.setNumListstatusThreads(numThreads);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Number of liststatus threads is invalid: " + getVal(command,
                AdlsMigratorOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()), e);
      }
    }
  }

  /**
   * parseBandwidth is a helper method for parsing BANDWIDTH.
   *
   * @param command command line arguments
   * @param option  parsed adlsmigrator options
   */
  private static void parseBandwidth(CommandLine command,
                                     AdlsMigratorOptions option) {
    if (command.hasOption(AdlsMigratorOptionSwitch.BANDWIDTH.getSwitch())) {
      try {
        Integer mapBandwidth = Integer.parseInt(
            getVal(command, AdlsMigratorOptionSwitch.BANDWIDTH.getSwitch()).trim());
        if (mapBandwidth <= 0) {
          throw new IllegalArgumentException("Bandwidth specified is not " +
              "positive: " + mapBandwidth);
        }
        option.setMapBandwidth(mapBandwidth);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bandwidth specified is invalid: " +
            getVal(command, AdlsMigratorOptionSwitch.BANDWIDTH.getSwitch()), e);
      }
    }
  }

  /**
   * parseSourceAndTargetPaths is a helper method for parsing the source
   * and target paths.
   *
   * @param command command line arguments
   * @return        AdlsMigratorOptions
   */
  private static AdlsMigratorOptions parseSourceAndTargetPaths(
      CommandLine command) {
    AdlsMigratorOptions option;
    Path targetPath;
    List<Path> sourcePaths = new ArrayList<Path>();

    String[] leftOverArgs = command.getArgs();
    if (leftOverArgs == null || leftOverArgs.length < 1) {
      throw new IllegalArgumentException("Target path not specified");
    }

    //Last Argument is the target path
    targetPath = new Path(leftOverArgs[leftOverArgs.length - 1].trim());

    //Copy any source paths in the arguments to the list
    for (int index = 0; index < leftOverArgs.length - 1; index++) {
      sourcePaths.add(new Path(leftOverArgs[index].trim()));
    }

    /* If command has source file listing, use it else, fall back on source
       paths in args.  If both are present, throw exception and bail */
    if (command.hasOption(
        AdlsMigratorOptionSwitch.SOURCE_FILE_LISTING.getSwitch())) {
      if (!sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Both source file listing and " +
            "source paths present");
      }
      option = new AdlsMigratorOptions(new Path(getVal(command, AdlsMigratorOptionSwitch.
          SOURCE_FILE_LISTING.getSwitch())), targetPath);
    } else {
      if (sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Neither source file listing nor " +
            "source paths present");
      }
      option = new AdlsMigratorOptions(sourcePaths, targetPath);
    }
    return option;
  }

  private static String getVal(CommandLine command, String swtch) {
    String optionValue = command.getOptionValue(swtch);
    if (optionValue == null) {
      return null;
    } else {
      return optionValue.trim();
    }
  }

  private static String[] getVals(CommandLine command, String option) {
    return command.getOptionValues(option);
  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("adlsmigrator OPTIONS [source_path...] <target_path>\n\nOPTIONS", cliOptions);
  }
}
