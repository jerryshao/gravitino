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
package org.apache.gravitino.job.k8s;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestK8sJobResourceUtils {

  @Test
  public void testIsClusterReachable() {
    for (String uri :
        new String[] {
          "local:///opt/app.jar",
          "https://repo/app.jar",
          "s3a://bucket/app.jar",
          "hdfs://nn/app.jar"
        }) {
      Assertions.assertTrue(K8sJobResourceUtils.isClusterReachable(uri), uri);
    }

    // Paths on the Gravitino server.
    for (String uri : new String[] {"/opt/app.jar", "app.jar", "file:///opt/app.jar", "FILE:/a"}) {
      Assertions.assertFalse(K8sJobResourceUtils.isClusterReachable(uri), uri);
    }

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> K8sJobResourceUtils.isClusterReachable("s3a://bucket/app jar"));
  }
}
