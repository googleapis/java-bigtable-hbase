/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.test.plugins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This plugin helps ensure that a project's dependencies align with another project.
 *
 * <p>This is specifically useful for drop in replacements like bigtable-hbase-*-hadoop where the
 * project's external dependencies should be a strict superset of hbase-client.
 */
@Mojo(
    name = "verify-mirror-deps",
    defaultPhase = LifecyclePhase.VALIDATE,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class VerifyMirrorDependencyVersions extends AbstractMojo {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(VerifyMirrorDependencyVersions.class);

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  /** The entry point to Maven Artifact Resolver, i.e. the component doing all the work. */
  @Component private RepositorySystem repoSystem;

  /** The current repository/network configuration of Maven. */
  @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
  private RepositorySystemSession repoSession;

  /** The project's remote repositories to use for the resolution. */
  @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true)
  private List<RemoteRepository> remoteRepos;

  /** The dependencies that should be reflected in the project's dependencies */
  @Parameter private List<String> targetDependencies;

  @Parameter private List<String> ignoredDependencies;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    // Grab the dependencies specified for the project
    Map<String, String> actualVersionMap = resolveProjectDependencyVersions();

    // Resolve transitive dep versions for the target
    System.out.println("target deps: " + targetDependencies);
    Collection<String> fullTargetDeps = patchProjectVersions(targetDependencies, actualVersionMap);
    System.out.println("full targetdeps:" + fullTargetDeps);
    Map<String, String> targetVersionMap;
    try {
      targetVersionMap = resolveTargetDependencyVersions(fullTargetDeps);
    } catch (IllegalArgumentException | DependencyResolutionException e) {
      throw new MojoFailureException(e.getMessage(), e);
    }

    // Remove ignored deps
    if (ignoredDependencies != null) {
      for (String dep : ignoredDependencies) {
        if (targetVersionMap.remove(dep) == null) {
          LOGGER.warn("stale ignore value: " + dep);
        }
      }
    }

    // Make sure that the overlap between actual and target dependencies align
    List<String> mismatches = new ArrayList<>();

    for (Map.Entry<String, String> entry : actualVersionMap.entrySet()) {
      String actualVersion = entry.getValue();
      String expectedVersion = targetVersionMap.get(entry.getKey());

      if (expectedVersion != null && !actualVersion.equals(expectedVersion)) {
        mismatches.add(
            String.format(
                "%s: expected %s, got %s", entry.getKey(), expectedVersion, actualVersion));
      }
    }

    Collections.sort(mismatches);
    if (!mismatches.isEmpty()) {
      LOGGER.error("Found unexpected dependency versions:");
      for (String mismatch : mismatches) {
        LOGGER.error(mismatch);
      }
      throw new MojoFailureException("Found unexpected dependency versions");
    }
  }

  private Collection<String> patchProjectVersions(
      Collection<String> targetDeps, Map<String, String> projectDeps) throws MojoFailureException {
    Set<String> patched = new HashSet<>();
    for (String s : targetDeps) {
      String[] parts = s.split(":");
      if (parts.length == 2) {
        String version = projectDeps.get(s);
        if (version == null) {
          throw new MojoFailureException("failed to patch version for target dep " + s);
        }
        patched.add(s + ":" + version);
      } else {
        patched.add(s);
      }
    }
    return patched;
  }

  /** Resolve all of the desired transitive dependencies */
  private Map<String, String> resolveTargetDependencyVersions(Collection<String> deps)
      throws DependencyResolutionException {
    CollectRequest collectRequest = new CollectRequest();

    deps.stream()
        .map(DefaultArtifact::new)
        .map((a) -> new Dependency(a, JavaScopes.COMPILE))
        .forEach(collectRequest::addDependency);

    collectRequest.setRepositories(remoteRepos);

    DependencyFilter classpathFlter =
        DependencyFilterUtils.classpathFilter(
            Arrays.asList(JavaScopes.COMPILE, JavaScopes.RUNTIME));

    DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFlter);

    final List<ArtifactResult> artifactResults;
    artifactResults =
        repoSystem.resolveDependencies(repoSession, dependencyRequest).getArtifactResults();

    return artifactResults.stream()
        .map(ArtifactResult::getArtifact)
        .collect(
            Collectors.toMap(
                VerifyMirrorDependencyVersions::extractKey,
                org.eclipse.aether.artifact.Artifact::getVersion));
  }

  /** Resolve all of the project specified transitive dependencies */
  private Map<String, String> resolveProjectDependencyVersions() {
    return project.getArtifacts().stream()
        .collect(
            Collectors.toMap(VerifyMirrorDependencyVersions::extractKey, Artifact::getVersion));
  }

  /** Create a key for a resolved target dependency */
  private static String extractKey(org.eclipse.aether.artifact.Artifact artifact) {
    StringBuilder buffer = new StringBuilder(128);
    buffer.append(artifact.getGroupId());
    buffer.append(':').append(artifact.getArtifactId());
    if (!artifact.getClassifier().isEmpty()) {
      buffer.append(':').append(artifact.getClassifier());
    }
    return buffer.toString();
  }

  /** Create a key for a project specified dependency */
  private static String extractKey(Artifact artifact) {
    StringBuilder buffer = new StringBuilder(128);
    buffer.append(artifact.getGroupId());
    buffer.append(':').append(artifact.getArtifactId());
    if (artifact.getClassifier() != null && !artifact.getClassifier().isEmpty()) {
      buffer.append(':').append(artifact.getClassifier());
    }
    return buffer.toString();
  }
}
