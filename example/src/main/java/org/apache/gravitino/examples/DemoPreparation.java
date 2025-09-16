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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;

public class DemoPreparation {
  private static final String METALAKE = "demo_metalake";
  private static final String CATALOG = "demo_catalog";
  private static final String SCHEMA = "demo_schema";
  private static final String FILESET = "demo_fileset";

  private static final int MIN_FILE_COUNT = 5;
  private static final int RANDOM_FILE_COUNT = 5;
  private static final int MAX_DEPTH = 4;
  private static final int MAX_FOLDERS_PER_LEVEL = 6;
  private static final String FILE_PREFIX = "file_";
  private static final String FILE_SUFFIX = ".txt";
  private static final String FOLDER_PREFIX = "dir_";
  private static final int FILE_SIZE_KB = 10; // KB

  private static final Random random = new Random();
  private static final String CHARACTERS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  public static void main(String[] args) {
    GravitinoAdminClient adminClient =
        GravitinoAdminClient.builder("http://localhost:8090").build();

    // Create metalake if not exists
    if (!adminClient.metalakeExists(METALAKE)) {
      adminClient.createMetalake(METALAKE, "Demo Metalake", Collections.emptyMap());
      System.out.println("Created metalake: " + METALAKE);
    } else {
      System.out.println("Metalake already exists: " + METALAKE);
    }

    GravitinoClient client =
        GravitinoClient.builder("http://localhost:8090").withMetalake(METALAKE).build();

    // Create catalog if not exists
    if (!client.catalogExists(CATALOG)) {
      client.createCatalog(CATALOG, Catalog.Type.FILESET, "Demo Catalog", Collections.emptyMap());
      System.out.println("Created catalog: " + CATALOG);
    } else {
      System.out.println("Catalog already exists: " + CATALOG);
    }

    Catalog catalog = client.loadCatalog(CATALOG);

    // Create schema if not exists
    if (!catalog.asSchemas().schemaExists(SCHEMA)) {
      catalog.asSchemas().createSchema(SCHEMA, "Demo Schema", Collections.emptyMap());
      System.out.println("Created schema: " + SCHEMA);
    } else {
      System.out.println("Schema already exists: " + SCHEMA);
    }

    // Create fileset if not exists
    String filesetLocation = "file:///tmp/demo-fileset";
    FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();
    NameIdentifier filesetIdent = NameIdentifier.of(SCHEMA, FILESET);
    if (!filesetCatalog.filesetExists(filesetIdent)) {
      filesetCatalog.createFileset(
          filesetIdent,
          "Demo Fileset",
          Fileset.Type.MANAGED,
          filesetLocation,
          Collections.emptyMap());
      System.out.println("Created fileset: " + FILESET);
    } else {
      System.out.println("Fileset already exists: " + FILESET);
    }

    // Randomly generate some files and directories in the fileset location recursively
    try {
      Path basePath = Path.of("/tmp/demo-fileset");
      generateDirectoryStructure(basePath, 0);
      System.out.println("Generated random files and directories in: " + basePath);
    } catch (IOException e) {
      System.err.println("Error generating files and directories: " + e.getMessage());
    }
  }

  private static void generateDirectoryStructure(Path currentPath, int currentDepth)
      throws IOException {
    Files.createDirectories(currentPath);

    int fileCount = MIN_FILE_COUNT + random.nextInt(RANDOM_FILE_COUNT);
    generateRandomFiles(currentPath, fileCount);

    if (currentDepth < MAX_DEPTH) {
      int folderCount = random.nextInt(MAX_FOLDERS_PER_LEVEL) + 1;

      for (int i = 0; i < folderCount; i++) {
        Path subDirPath = generateRandomFolderName(currentPath);
        generateDirectoryStructure(subDirPath, currentDepth + 1);
      }
    }
  }

  private static void generateRandomFiles(Path directory, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      Path filePath = generateRandomFileName(directory);
      createFileWithContent(filePath, FILE_SIZE_KB);

      LocalDateTime start = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
      LocalDateTime end = LocalDateTime.now();
      long startEpoch = start.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
      long endEpoch = end.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
      long randomEpoch = startEpoch + (long) (random.nextDouble() * (endEpoch - startEpoch));

      Instant randomInstant = Instant.ofEpochMilli(randomEpoch);
      FileTime newAccessTime = FileTime.from(randomInstant);
      Files.setAttribute(filePath, "lastAccessTime", newAccessTime);
    }
  }

  private static void createFileWithContent(Path filePath, int sizeKB) throws IOException {
    try (FileWriter writer = new FileWriter(filePath.toFile(), StandardCharsets.UTF_8)) {
      StringBuilder content = new StringBuilder();
      int targetSize = sizeKB * 1024;

      while (content.length() < targetSize) {
        content.append(generateRandomString(100));
      }

      writer.write(content.toString().substring(0, targetSize));
    }
  }

  private static Path generateRandomFolderName(Path parentPath) {
    String folderName;
    if (random.nextBoolean()) {
      folderName = FOLDER_PREFIX + UUID.randomUUID().toString().substring(0, 8);
    } else {
      folderName = FOLDER_PREFIX + generateRandomString(6);
    }
    return parentPath.resolve(folderName);
  }

  private static Path generateRandomFileName(Path parentPath) {
    String fileName;
    if (random.nextBoolean()) {
      fileName = FILE_PREFIX + UUID.randomUUID().toString().replace("-", "") + FILE_SUFFIX;
    } else {
      fileName = FILE_PREFIX + generateRandomString(8) + FILE_SUFFIX;
    }

    Path filePath = parentPath.resolve(fileName);
    if (Files.exists(filePath)) {
      return generateRandomFileName(parentPath);
    }
    return filePath;
  }

  private static String generateRandomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      int index = random.nextInt(CHARACTERS.length());
      sb.append(CHARACTERS.charAt(index));
    }
    return sb.toString();
  }
}
