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

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonWriteNullProperties;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.Writer;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * The IdentityMap holds a mapping between a local (Kerberos) identity and
 * a cloud-based AAD identity.
 */
@JsonWriteNullProperties
public class IdentityMap {
  private String localIdentity;
  private String cloudIdentity;

  public IdentityMap() {
  }

  public IdentityMap(String localIdentity) {
    this.localIdentity = localIdentity;
  }

  public String getLocalIdentity() {
    return localIdentity;
  }

  public String setLocalIdentity(String localIdentity) {
    return this.localIdentity = localIdentity;
  }

  public String getCloudIdentity() {
    return cloudIdentity;
  }

  public String setCloudIdentity(String cloudIdentity) {
    return this.cloudIdentity = cloudIdentity;
  }

  @Override
  public int hashCode() {
    return localIdentity.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IdentityMap) {
      return localIdentity.equals(((IdentityMap)obj).getLocalIdentity());
    }
    return false;
  }

  public static void saveToJsonFile(HashSet<IdentityMap> identities, String filename) 
                                      throws IOException {
    try {
      try (Writer writer = new OutputStreamWriter(new FileOutputStream(filename))) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
        objectMapper.writeValue(writer, identities);
      }
    } catch (JsonGenerationException ex) {
      throw new IOException("Failure encoding to JSON", ex);
    } 
  }

  public static HashSet<IdentityMap> readFromJsonFile(String filename) 
                                                    throws IOException {
    try {
      try (Reader input = new InputStreamReader(new FileInputStream(filename), "UTF-8")) {
        ObjectMapper objectMapper = new ObjectMapper();
        IdentityMap[] identities = objectMapper.readValue(input, IdentityMap[].class);
        return new HashSet<IdentityMap>(Arrays.asList(identities));
      }
    } catch (JsonMappingException jme) {
      // The old format doesn't have json top-level token to enclose the array.
      // For backward compatibility, try parsing the old format.
      throw new IOException("Failure parsing", jme);
    }
  }
}

