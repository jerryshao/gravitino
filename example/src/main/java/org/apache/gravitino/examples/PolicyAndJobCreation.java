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

import java.util.Collections;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;

public class PolicyAndJobCreation {
  private static final String METALAKE = "demo_metalake";
  private static final String CATALOG_NAME = "demo_catalog";
  private static final String SCHEMA_NAME = "demo_schema";
  private static final String FILESET_NAME = "demo_fileset";

  private static final String POLICY_NAME = "demo-fileset-ttl";
  private static final String JOB_TEMPLATE_NAME = "demo-fileset-ttl-job-template";

  public static void main(String[] args) {
    GravitinoClient client =
        GravitinoClient.builder("http://localhost:8090").withMetalake(METALAKE).build();

    // Create a policy if not exists
    try {
      client.getPolicy(POLICY_NAME);
      System.out.println("Policy " + POLICY_NAME + " already exists.");
    } catch (NoSuchPolicyException e) {
      System.out.println("Creating policy " + POLICY_NAME);

      PolicyContent filesetTtlPolicyContent =
          PolicyContents.custom(
              ImmutableMap.of("ttl-in-seconds", "604800"), // 7 days
              ImmutableSet.of(
                  MetadataObject.Type.CATALOG,
                  MetadataObject.Type.SCHEMA,
                  MetadataObject.Type.FILESET),
              Collections.emptyMap());
      client.createPolicy(
          POLICY_NAME,
          "custom",
          "A demo policy to set TTL for filesets",
          true,
          filesetTtlPolicyContent);
    }

    // Attach the policy to the fileset if not attached
    try {
      Fileset fileset =
          client
              .loadCatalog(CATALOG_NAME)
              .asFilesetCatalog()
              .loadFileset(NameIdentifier.of(SCHEMA_NAME, FILESET_NAME));
      fileset.supportsPolicies().associatePolicies(new String[] {POLICY_NAME}, new String[0]);
    } catch (PolicyAlreadyAssociatedException e) {
      System.out.println("Policy " + POLICY_NAME + " is already associated with the fileset.");
    }

    // Create job template
    try {
      client.getJobTemplate(JOB_TEMPLATE_NAME);
      System.out.println("Job template " + JOB_TEMPLATE_NAME + " already exists.");
    } catch (NoSuchJobTemplateException e) {
      System.out.println("Creating job template " + JOB_TEMPLATE_NAME);

      ShellJobTemplate jobTemplate =
          ShellJobTemplate.builder()
              .withName(JOB_TEMPLATE_NAME)
              .withComment("A demo job template to remove old files in a fileset")
              .withExecutable("/Users/jerryshao/Projects/fileset-ttl-demo/ttl-job.sh")
              .withArguments(Lists.newArrayList("{{metalake_name}}", "{{fileset_name}}"))
              .withScripts(
                  Lists.newArrayList(
                      "/Users/jerryshao/Projects/fileset-ttl-demo/gravitino-example-1.0.1-SNAPSHOT"
                          + ".jar"))
              .build();

      client.registerJobTemplate(jobTemplate);
    }
  }
}
