/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.examples;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OldFilesRemover {

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("Usage: java OldFilesRemover <metalake> <fileset>");
      System.exit(1);
    }

    String metalake = args[0];
    String filesetName = args[1];

    // Do the initialization and check
    GravitinoClient gravitinoClient =
        GravitinoClient.builder("http://localhost:8090").withMetalake(metalake).build();

    Policy ttlPolicy = null;
    try {
      ttlPolicy = gravitinoClient.getPolicy("demo-fileset-ttl");
    } catch (NoSuchPolicyException e) {
      System.out.println("Policy demo-fileset-ttl does not exist.");
      System.exit(1);
    }

    String ttlInSec =
        (String)
            (((PolicyContents.CustomContent) ttlPolicy.content())
                .customRules()
                .get("ttl-in-seconds"));
    if (ttlInSec == null) {
      System.out.println("Policy demo-fileset-ttl does not have ttl-in-seconds property.");
      System.exit(1);
    }
    long ttlInMs = Long.parseLong(ttlInSec) * 1000;

    MetadataObject[] objects = ttlPolicy.associatedObjects().objects();
    Optional<MetadataObject> target =
        Arrays.stream(objects).filter(o -> o.fullName().equals(filesetName)).findAny();
    if (target.isEmpty()) {
      System.out.println("Policy demo-fileset-ttl is not associated with fileset " + filesetName);
      System.exit(1);
    }

    String[] nameParts = filesetName.split("\\.");
    String catalogName = nameParts[0];
    String schemaName = nameParts[1];
    String simpleFilesetName = nameParts[2];
    String gvfsPathStr =
        String.format("gvfs://fileset/%s/%s/%s", catalogName, schemaName, simpleFilesetName);

    // Create GVFS and list all the files that are older than the TTL
    Configuration conf = new Configuration();
    conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.gravitino.server.uri", "http://localhost:8090");
    conf.set("fs.gravitino.client.metalake", metalake);

    Path filesetPath = new Path(gvfsPathStr);
    FileSystem gvfs = filesetPath.getFileSystem(conf);

    // Recursively list and delete old files
    long currentTime = System.currentTimeMillis();
    listAndDeleteOldFiles(gvfs, filesetPath, currentTime, ttlInMs);
    gvfs.close();
    gravitinoClient.close();
  }

  private static void listAndDeleteOldFiles(
      FileSystem gvfs, Path path, long currentTime, long ttlInMs) throws IOException {
    FileStatus status = gvfs.getFileStatus(path);

    if (status.isFile()) {
      long accessTime = gvfs.getFileStatus(path).getAccessTime();
      if (currentTime - accessTime > ttlInMs) {
        Instant accessInstant = Instant.ofEpochMilli(accessTime);
        System.out.println(
            "Deleting old file: " + path.toString() + ", last accessed at " + accessInstant);
        gvfs.delete(path, false);
      }
    } else if (status.isDirectory()) {
      for (FileStatus s : gvfs.listStatus(path)) {
        listAndDeleteOldFiles(gvfs, s.getPath(), currentTime, ttlInMs);
      }
    }
  }
}
