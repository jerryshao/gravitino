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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.connector.job.JobContext;
import org.apache.gravitino.connector.job.JobExecutionInfo;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.job.k8s.spark.SparkApplicationStatusUtils;
import org.apache.gravitino.job.k8s.spark.SparkApplicationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A job executor that runs jobs on Kubernetes. Spark jobs run as SparkApplication resources of <a
 * href="https://github.com/apache/spark-kubernetes-operator">apache/spark-kubernetes-operator</a>
 * 0.9.x, shell jobs aren't supported yet.
 *
 * <p>All the job state lives in Kubernetes, so every Gravitino server can query and cancel any job.
 * The job execution id is {@code <cluster>/<kind>/<namespace>/<name>}, see {@link
 * K8sJobExecutionId}.
 */
public class K8sJobExecutor implements JobExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(K8sJobExecutor.class);

  private final Clock clock;

  private K8sJobExecutorConfigs configs;

  private KubernetesClient client;

  private NamespacedResourceCache<GenericKubernetesResource> sparkApplicationCache;

  /** Creates the job executor, which Gravitino initializes with {@link #initialize(Map)}. */
  public K8sJobExecutor() {
    this(Clock.systemUTC());
  }

  @VisibleForTesting
  K8sJobExecutor(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void initialize(Map<String, String> configs) {
    K8sJobExecutorConfigs parsed = new K8sJobExecutorConfigs(configs);
    initialize(parsed, K8sClientUtils.createClient(parsed));
  }

  @VisibleForTesting
  void initialize(K8sJobExecutorConfigs configs, KubernetesClient client) {
    this.configs = configs;
    this.client = client;
    this.sparkApplicationCache =
        new NamespacedResourceCache<>(
            configs.statusCacheTtlMs(),
            namespace ->
                sparkApplications(namespace)
                    .withLabel(
                        K8sJobResourceUtils.LABEL_MANAGED_BY,
                        K8sJobResourceUtils.MANAGED_BY_GRAVITINO)
                    .list()
                    .getItems(),
            clock::millis);
    LOG.info(
        "Initialized the k8s job executor for cluster {} at {}, default namespace {}",
        configs.cluster(),
        client.getMasterUrl(),
        configs.namespace());
  }

  @Override
  public String submitJob(JobContext context, JobTemplate jobTemplate) {
    if (!(jobTemplate instanceof SparkJobTemplate)) {
      throw new IllegalArgumentException(
          String.format(
              "The k8s job executor only supports Spark jobs, but job template %s is a %s job",
              jobTemplate.name(), jobTemplate.jobType()));
    }

    GenericKubernetesResource app =
        SparkApplicationUtils.buildSparkApplication(
            context, (SparkJobTemplate) jobTemplate, configs);
    String namespace = app.getMetadata().getNamespace();
    String name = app.getMetadata().getName();
    try {
      sparkApplications(namespace).resource(app).create();
    } catch (KubernetesClientException e) {
      if (e.getCode() != 409) {
        throw e;
      }
      // Created by an earlier attempt to submit the same job. Never adopt a resource of another
      // job, for example one left by another Gravitino deployment sharing the namespace.
      GenericKubernetesResource existing = sparkApplications(namespace).withName(name).get();
      if (existing == null || !K8sJobResourceUtils.isOfJob(existing, context.jobId())) {
        throw new IllegalStateException(
            String.format(
                "SparkApplication %s/%s already exists and doesn't belong to job %s, use a"
                    + " different %s for each Gravitino deployment sharing the namespace",
                namespace, name, context.jobId(), K8sJobExecutorConfigs.NAME_PREFIX),
            e);
      }
    }

    K8sJobExecutionId jobExecutionId =
        K8sJobExecutionId.of(
            configs.cluster(), K8sJobExecutionId.Kind.SPARK_APPLICATION, namespace, name);
    LOG.info("Submitted job {} as {}", context.jobId(), jobExecutionId);
    return jobExecutionId.toString();
  }

  @Override
  public JobExecutionInfo getJobExecutionInfo(String jobId) throws NoSuchJobException {
    K8sJobExecutionId id = parseJobExecutionId(jobId);
    GenericKubernetesResource app =
        sparkApplicationCache
            .find(id.namespace(), id.name())
            .orElseGet(() -> sparkApplications(id.namespace()).withName(id.name()).get());
    if (app == null) {
      throw new NoSuchJobException("SparkApplication of job %s doesn't exist", jobId);
    }

    // Gravitino requested to cancel the job, but the deletion failed, retry it.
    if (K8sJobResourceUtils.isCancelRequested(app)
        && app.getMetadata().getDeletionTimestamp() == null
        && !SparkApplicationStatusUtils.isStopped(app)) {
      LOG.info("Retrying to delete the SparkApplication of cancelled job {}", jobId);
      try {
        delete(id);
      } catch (KubernetesClientException e) {
        // Retried again by the next status query.
        LOG.warn("Failed to delete the SparkApplication of cancelled job {}", jobId, e);
      }
    }

    // The operator observes the driver finishing a while after it does, so a finished job takes
    // its times from the driver pod, which is retained after the job finishes.
    Pod driver = null;
    if (SparkApplicationStatusUtils.isStopped(app)) {
      try {
        driver = driverPod(id).orElse(null);
      } catch (KubernetesClientException e) {
        LOG.warn("Failed to get the driver pod of job {}", jobId, e);
      }
    }
    return SparkApplicationStatusUtils.executionInfoOf(
        app, driver, clock.instant(), configs.noStatusTimeoutMs());
  }

  @Override
  public void cancelJob(String jobId) throws NoSuchJobException {
    K8sJobExecutionId id = parseJobExecutionId(jobId);
    GenericKubernetesResource app = sparkApplications(id.namespace()).withName(id.name()).get();
    if (app == null) {
      throw new NoSuchJobException("SparkApplication of job %s doesn't exist", jobId);
    }
    if (SparkApplicationStatusUtils.isStopped(app)) {
      return;
    }

    // Mark the cancellation before deleting, so that a status query on any server tells a
    // cancelled job from one deleted by someone else.
    try {
      sparkApplications(id.namespace())
          .withName(id.name())
          .edit(
              current -> {
                Map<String, String> annotations = new HashMap<>();
                if (current.getMetadata().getAnnotations() != null) {
                  annotations.putAll(current.getMetadata().getAnnotations());
                }
                annotations.put(K8sJobResourceUtils.ANNOTATION_CANCEL_REQUESTED, "true");
                current.getMetadata().setAnnotations(annotations);
                return current;
              });
    } catch (KubernetesClientException e) {
      if (e.getCode() == 404) {
        throw new NoSuchJobException(e, "SparkApplication of job %s doesn't exist", jobId);
      }
      throw e;
    }
    delete(id);
    LOG.info("Requested to cancel job {}", jobId);
  }

  @Override
  public List<String> getJobStdout(String jobId, int maxLines, int maxBytes) {
    K8sJobExecutionId id;
    try {
      id = parseJobExecutionId(jobId);
    } catch (NoSuchJobException e) {
      return ImmutableList.of();
    }

    try {
      Optional<Pod> driver = driverPod(id);
      if (!driver.isPresent()) {
        return ImmutableList.of();
      }

      // Kubernetes can only limit the bytes from the head of the log, so read the last lines and
      // keep their tail.
      try (InputStream log =
          client
              .pods()
              .inNamespace(id.namespace())
              .withName(driver.get().getMetadata().getName())
              .tailingLines(maxLines)
              .getLogInputStream()) {
        return PodLogUtils.readLastLines(log, maxLines, maxBytes);
      }
    } catch (KubernetesClientException | IOException e) {
      LOG.warn("Failed to read the driver log of job {}", jobId, e);
      return ImmutableList.of();
    }
  }

  /**
   * Returns an empty list: Kubernetes merges the standard error into the standard output of a pod,
   * so it is returned by {@link #getJobStdout(String, int, int)}.
   */
  @Override
  public List<String> getJobStderr(String jobId, int maxLines, int maxBytes) {
    return ImmutableList.of();
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  private K8sJobExecutionId parseJobExecutionId(String jobId) throws NoSuchJobException {
    K8sJobExecutionId id = K8sJobExecutionId.parse(jobId);
    if (!configs.cluster().equals(id.cluster())) {
      throw new NoSuchJobException(
          "Job %s runs in cluster %s, but the k8s job executor is connected to cluster %s",
          jobId, id.cluster(), configs.cluster());
    }
    return id;
  }

  private Optional<Pod> driverPod(K8sJobExecutionId id) {
    // The operator names the driver pod after the Spark app id, which it may hash-truncate, so
    // look it up by labels.
    return client
        .pods()
        .inNamespace(id.namespace())
        .withLabel(SparkApplicationUtils.LABEL_SPARK_APP_NAME, id.name())
        .withLabel(SparkApplicationUtils.LABEL_SPARK_ROLE, SparkApplicationUtils.SPARK_ROLE_DRIVER)
        .list()
        .getItems()
        .stream()
        .max(Comparator.comparing(pod -> pod.getMetadata().getCreationTimestamp()));
  }

  private void delete(K8sJobExecutionId id) {
    // The SparkApplication stays until its pods are deleted, so that a status query sees it being
    // deleted rather than gone.
    sparkApplications(id.namespace())
        .withName(id.name())
        .withPropagationPolicy(DeletionPropagation.FOREGROUND)
        .delete();
    sparkApplicationCache.invalidate(id.namespace());
  }

  private NonNamespaceOperation<
          GenericKubernetesResource,
          GenericKubernetesResourceList,
          Resource<GenericKubernetesResource>>
      sparkApplications(String namespace) {
    return client
        .genericKubernetesResources(SparkApplicationUtils.SPARK_APPLICATION)
        .inNamespace(namespace);
  }
}
