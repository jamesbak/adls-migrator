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

import static org.mockito.Mockito.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import com.azure.storage.adlsmigrator.util.TestAdlsMigratorUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@RunWith(value = Parameterized.class)
public class TestCopyListing extends SimpleCopyListing {
  private static final Log LOG = LogFactory.getLog(TestCopyListing.class);

  private static final Credentials CREDENTIALS = new Credentials();

  private static final Configuration config = new Configuration();
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void create() throws IOException {
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).format(true)
                                                .build();
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 1 }, { 2 }, { 10 }, { 20} };
    return Arrays.asList(data);
  }

  public TestCopyListing(int numListstatusThreads) {
    super(config, CREDENTIALS, numListstatusThreads, 0, false);
  }

  protected TestCopyListing(Configuration configuration) {
    super(configuration, CREDENTIALS);
  }

  @Override
  protected long getBytesToCopy() {
    return 0;
  }

  @Override
  protected long getNumberOfPaths() {
    return 0;
  }

  @Test(timeout=10000)
  public void testMultipleSrcToFile() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(new Path("/tmp/in/1"));
      srcPaths.add(new Path("/tmp/in/2"));
      Path target = new Path("/tmp/out/1");
      TestAdlsMigratorUtils.createFile(fs, "/tmp/in/1");
      TestAdlsMigratorUtils.createFile(fs, "/tmp/in/2");
      fs.mkdirs(target);
      AdlsMigratorOptions options = new AdlsMigratorOptions(srcPaths, target);
      validatePaths(options);
      TestAdlsMigratorUtils.delete(fs, "/tmp");
      //No errors

      target = new Path("/tmp/out/1");
      fs.create(target).close();
      options = new AdlsMigratorOptions(srcPaths, target);
      try {
        validatePaths(options);
        Assert.fail("Invalid inputs accepted");
      } catch (InvalidInputException ignore) { }
      TestAdlsMigratorUtils.delete(fs, "/tmp");

      srcPaths.clear();
      srcPaths.add(new Path("/tmp/in/1"));
      fs.mkdirs(new Path("/tmp/in/1"));
      target = new Path("/tmp/out/1");
      fs.create(target).close();
      options = new AdlsMigratorOptions(srcPaths, target);
      try {
        validatePaths(options);
        Assert.fail("Invalid inputs accepted");
      } catch (InvalidInputException ignore) { }
      TestAdlsMigratorUtils.delete(fs, "/tmp");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test input validation failed");
    } finally {
      TestAdlsMigratorUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testDuplicates() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(new Path("/tmp/in/*/*"));
      TestAdlsMigratorUtils.createFile(fs, "/tmp/in/src1/1.txt");
      TestAdlsMigratorUtils.createFile(fs, "/tmp/in/src2/1.txt");
      Path target = new Path("/tmp/out");
      Path listingFile = new Path("/tmp/list");
      AdlsMigratorOptions options = new AdlsMigratorOptions(srcPaths, target);
      CopyListing listing = CopyListing.getCopyListing(getConf(), CREDENTIALS, options);
      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Duplicates not detected");
      } catch (DuplicateFileException ignore) {
      }
    } catch (IOException e) {
      LOG.error("Exception encountered in test", e);
      Assert.fail("Test failed " + e.getMessage());
    } finally {
      TestAdlsMigratorUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testBuildListing() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      Path p1 = new Path("/tmp/in/1");
      Path p2 = new Path("/tmp/in/2");
      Path p3 = new Path("/tmp/in2/2");
      Path target = new Path("/tmp/out/1");
      srcPaths.add(p1.getParent());
      srcPaths.add(p3.getParent());
      TestAdlsMigratorUtils.createFile(fs, "/tmp/in/1");
      TestAdlsMigratorUtils.createFile(fs, "/tmp/in/2");
      TestAdlsMigratorUtils.createFile(fs, "/tmp/in2/2");
      fs.mkdirs(target);
      OutputStream out = fs.create(p1);
      out.write("ABC".getBytes());
      out.close();

      out = fs.create(p2);
      out.write("DEF".getBytes());
      out.close();

      out = fs.create(p3);
      out.write("GHIJ".getBytes());
      out.close();

      Path listingFile = new Path("/tmp/file");

      AdlsMigratorOptions options = new AdlsMigratorOptions(srcPaths, target);
      options.setSyncFolder(true);
      CopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Duplicates not detected");
      } catch (DuplicateFileException ignore) {
      }
      Assert.assertEquals(listing.getBytesToCopy(), 10);
      Assert.assertEquals(listing.getNumberOfPaths(), 3);
      TestAdlsMigratorUtils.delete(fs, "/tmp");

      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Invalid input not detected");
      } catch (InvalidInputException ignore) {
      }
      TestAdlsMigratorUtils.delete(fs, "/tmp");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test build listing failed");
    } finally {
      TestAdlsMigratorUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=60000)
  public void testWithRandomFileListing() throws IOException {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<>();
      List<Path> srcFiles = new ArrayList<>();
      Path target = new Path("/tmp/out/1");
      final int pathCount = 25;
      for (int i = 0; i < pathCount; i++) {
        Path p = new Path("/tmp", String.valueOf(i));
        srcPaths.add(p);
        fs.mkdirs(p);

        Path fileName = new Path(p, i + ".txt");
        srcFiles.add(fileName);
        try (OutputStream out = fs.create(fileName)) {
          out.write(i);
        }
      }

      Path listingFile = new Path("/tmp/file");
      AdlsMigratorOptions options = new AdlsMigratorOptions(srcPaths, target);
      options.setSyncFolder(true);

      // Check without randomizing files
      getConf().setBoolean(
          AdlsMigratorConstants.CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES, false);
      SimpleCopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
      listing.buildListing(listingFile, options);

      Assert.assertEquals(listing.getNumberOfPaths(), pathCount);
      validateFinalListing(listingFile, srcFiles);
      fs.delete(listingFile, true);

      // Check with randomized file listing
      getConf().setBoolean(
          AdlsMigratorConstants.CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES, true);
      listing = new SimpleCopyListing(getConf(), CREDENTIALS);

      // Set the seed for randomness, so that it can be verified later
      long seed = System.nanoTime();
      listing.setSeedForRandomListing(seed);
      listing.buildListing(listingFile, options);
      Assert.assertEquals(listing.getNumberOfPaths(), pathCount);

      // validate randomness
      Collections.shuffle(srcFiles, new Random(seed));
      validateFinalListing(listingFile, srcFiles);
    } finally {
      TestAdlsMigratorUtils.delete(fs, "/tmp");
    }
  }

  private void validateFinalListing(Path pathToListFile, List<Path> srcFiles)
      throws IOException {
    FileSystem fs = pathToListFile.getFileSystem(config);

    try (SequenceFile.Reader reader = new SequenceFile.Reader(
        config, SequenceFile.Reader.file(pathToListFile))) {
      CopyListingFileStatus currentVal = new CopyListingFileStatus();

      Text currentKey = new Text();
      int idx = 0;
      while (reader.next(currentKey)) {
        reader.getCurrentValue(currentVal);
        Assert.assertEquals("srcFiles.size=" + srcFiles.size()
                + ", idx=" + idx, fs.makeQualified(srcFiles.get(idx)),
            currentVal.getPath());
        if (LOG.isDebugEnabled()) {
          LOG.debug("val=" + fs.makeQualified(srcFiles.get(idx)));
        }
        idx++;
      }
    }
  }


  @Test(timeout=10000)
  public void testBuildListingForSingleFile() {
    FileSystem fs = null;
    String testRootString = "/singleFileListing";
    Path testRoot = new Path(testRootString);
    SequenceFile.Reader reader = null;
    try {
      fs = FileSystem.get(getConf());
      if (fs.exists(testRoot))
        TestAdlsMigratorUtils.delete(fs, testRootString);

      Path sourceFile = new Path(testRoot, "/source/foo/bar/source.txt");
      Path decoyFile  = new Path(testRoot, "/target/moo/source.txt");
      Path targetFile = new Path(testRoot, "/target/moo/target.txt");

      TestAdlsMigratorUtils.createFile(fs, sourceFile.toString());
      TestAdlsMigratorUtils.createFile(fs, decoyFile.toString());
      TestAdlsMigratorUtils.createFile(fs, targetFile.toString());

      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(sourceFile);

      AdlsMigratorOptions options = new AdlsMigratorOptions(srcPaths, targetFile);
      CopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);

      final Path listFile = new Path(testRoot, "/tmp/fileList.seq");
      listing.buildListing(listFile, options);

      reader = new SequenceFile.Reader(getConf(), SequenceFile.Reader.file(listFile));

      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertTrue(relativePath.toString().equals(""));
    }
    catch (Exception e) {
      Assert.fail("Unexpected exception encountered.");
      LOG.error("Unexpected exception: ", e);
    }
    finally {
      TestAdlsMigratorUtils.delete(fs, testRootString);
      IOUtils.closeStream(reader);
    }
  }
  
  @Test
  public void testFailOnCloseError() throws IOException {
    File inFile = File.createTempFile("TestCopyListingIn", null);
    inFile.deleteOnExit();
    File outFile = File.createTempFile("TestCopyListingOut", null);
    outFile.deleteOnExit();
    List<Path> srcs = new ArrayList<Path>();
    srcs.add(new Path(inFile.toURI()));
    
    Exception expectedEx = new IOException("boom");
    SequenceFile.Writer writer = mock(SequenceFile.Writer.class);
    doThrow(expectedEx).when(writer).close();
    
    SimpleCopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
    AdlsMigratorOptions options = new AdlsMigratorOptions(srcs, new Path(outFile.toURI()));
    Exception actualEx = null;
    try {
      listing.doBuildListing(writer, options);
    } catch (Exception e) {
      actualEx = e;
    }
    Assert.assertNotNull("close writer didn't fail", actualEx);
    Assert.assertEquals(expectedEx, actualEx);
  }
}
