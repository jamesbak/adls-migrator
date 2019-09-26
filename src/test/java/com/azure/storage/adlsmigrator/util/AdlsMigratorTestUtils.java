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

package com.azure.storage.adlsmigrator.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.azure.storage.adlsmigrator.AdlsMigrator;
import org.apache.hadoop.util.ToolRunner;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for AdlsMigratorTests
 */
public class AdlsMigratorTestUtils {

   /**
    * Asserts the XAttrs returned by getXAttrs for a specific path match an
    * expected set of XAttrs.
    *
    * @param path String path to check
    * @param fs FileSystem to use for the path
    * @param expectedXAttrs XAttr[] expected xAttrs
    * @throws Exception if there is any error
    */
  public static void assertXAttrs(Path path, FileSystem fs,
      Map<String, byte[]> expectedXAttrs)
      throws Exception {
    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    assertEquals(path.toString(), expectedXAttrs.size(), xAttrs.size());
    Iterator<Entry<String, byte[]>> i = expectedXAttrs.entrySet().iterator();
    while (i.hasNext()) {
      Entry<String, byte[]> e = i.next();
      String name = e.getKey();
      byte[] value = e.getValue();
      if (value == null) {
        assertTrue(xAttrs.containsKey(name) && xAttrs.get(name) == null);
      } else {
        assertArrayEquals(value, xAttrs.get(name));
      }
    }
  }

  /**
   * Runs adlsmigrator from src to dst, preserving XAttrs. Asserts the
   * expected exit code.
   *
   * @param exitCode expected exit code
   * @param src adlsmigrator src path
   * @param dst adlsmigrator destination
   * @param options adlsmigrator command line options
   * @param conf Configuration to use
   * @throws Exception if there is any error
   */
  public static void assertrunAdlsMigrator(int exitCode, String src, String dst,
      String options, Configuration conf)
      throws Exception {
    assertrunAdlsMigrator(exitCode, src, dst,
        options == null ? new String[0] : options.trim().split(" "), conf);
  }

  private static void assertrunAdlsMigrator(int exitCode, String src, String dst,
      String[] options, Configuration conf)
      throws Exception {
    AdlsMigrator AdlsMigrator = new AdlsMigrator(conf, null);
    String[] optsArr = new String[options.length + 2];
    System.arraycopy(options, 0, optsArr, 0, options.length);
    optsArr[optsArr.length - 2] = src;
    optsArr[optsArr.length - 1] = dst;

    assertEquals(exitCode,
        ToolRunner.run(conf, AdlsMigrator, optsArr));
  }
}
