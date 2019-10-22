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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.Map;
import java.util.stream.Collectors;

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
    } else if (obj instanceof String) {
      return localIdentity.equals((String)obj);
    }
    return false;
  }

  public static Path getIdentitiesMapFile(Configuration configuration) {
    String identitiesMapFile = configuration.get(AdlsMigratorConstants.CONF_LABEL_IDENTITIES_MAP_FILE);
    if (StringUtils.isNotBlank(identitiesMapFile)) {
      return new Path(identitiesMapFile);
    }
    return null;
  }

  public static void saveToJsonFile(HashSet<IdentityMap> identities, Path filePath, Configuration configuration) 
                                      throws IOException {
    if (filePath == null) {
      return;
    }
    try {
      FileSystem fs = filePath.getFileSystem(configuration);
      try (Writer writer = new OutputStreamWriter(fs.create(filePath))) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
        objectMapper.writeValue(writer, identities);
      }
    } catch (JsonGenerationException ex) {
      throw new IOException("Failure encoding to JSON", ex);
    } 
  }

  public static Map<String, IdentityMap> readFromJsonFile(Path filePath, Configuration configuration) 
                                                    throws IOException {
    if (filePath == null) {
      throw new IOException("Unable to deserialize from JSON. filePath is null");
    }                                                        
    try {
      FileSystem fs = filePath.getFileSystem(configuration);
      try (Reader input = new InputStreamReader(fs.open(filePath), "UTF-8")) {
        ObjectMapper objectMapper = new ObjectMapper();
        IdentityMap[] identities = objectMapper.readValue(input, IdentityMap[].class);
        return Arrays.asList(identities)
          .stream()
          .collect(Collectors.toMap(i -> i.getLocalIdentity(), i -> i));
      }
    } catch (JsonMappingException jme) {
      // The old format doesn't have json top-level token to enclose the array.
      // For backward compatibility, try parsing the old format.
      throw new IOException("Failure parsing", jme);
    }
  }

  public static String mapIdentity(String srcIdentity, Map<String, IdentityMap> identities) {
    String retval = srcIdentity;
    if (identities != null) {
      IdentityMap item = identities.get(srcIdentity);
      if (item != null) {
        retval = item.getCloudIdentity();
      }
    }
    return retval;
  }
}

