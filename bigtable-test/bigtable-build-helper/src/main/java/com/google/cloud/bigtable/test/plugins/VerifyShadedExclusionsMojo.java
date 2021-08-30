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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.maven.model.Model;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
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

@Mojo(
    name = "verify-shaded-exclusions",
    defaultPhase = LifecyclePhase.VERIFY,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class VerifyShadedExclusionsMojo extends AbstractMojo {
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

  /**
   * Verifies that all of the dependencies that were excluded from shading, are added as external
   * deps in the final dependency-reduced-pom.xml.
   *
   * <p>This doesn't currently consider transitive dependencies, so it expects that
   * dependency-reduced-pom.xml flattens all the unshaded deps.
   *
   * @return true if all of the dependencies are accounted for
   */
  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    Set<String> excludedDeps = getShadingExclusions();
    Set<String> outputDeps;

    try {
      outputDeps = getOutputDependencies();
    } catch (DependencyResolutionException e) {
      throw new MojoFailureException(e.getMessage(), e);
    }

    // Find all of the dependencies that were excluded but not specified in the
    // dependency-reduced-pom.xml
    List<String> missingDeps = new ArrayList<>(excludedDeps);
    missingDeps.removeAll(outputDeps);

    // Log them
    Collections.sort(missingDeps);
    for (String coordinate : missingDeps) {
      LOGGER.error(
          "{} was excluded from the shaded jar, but is not listed in the dependency-reduced-pom.xml",
          coordinate);
    }

    if (!missingDeps.isEmpty()) {
      throw new MojoFailureException("Found unpromoted dependencies");
    }
  }

  /** Extracts the artifact names that we excluded from shading */
  private Set<String> getShadingExclusions() throws MojoFailureException {
    Set<String> allDeps =
        project.getArtifacts().stream()
            .map(a -> String.format("%s:%s", a.getGroupId(), a.getArtifactId()))
            .collect(Collectors.toSet());

    List<Plugin> shadePlugins =
        project.getBuildPlugins().stream()
            .filter(p -> "maven-shade-plugin".equals(p.getArtifactId()))
            .collect(Collectors.toList());
    if (shadePlugins.size() != 1) {
      throw new MojoFailureException("Expected a single maven shade plugin config");
    }
    Plugin shadePlugin = shadePlugins.get(0);
    if (shadePlugin.getExecutions().size() != 1) {
      throw new MojoFailureException("expected only a single execution of maven-shade-plugin");
    }
    Xpp3Dom configuration = (Xpp3Dom) shadePlugin.getExecutions().get(0).getConfiguration();

    Set<String> includes =
        Optional.ofNullable(configuration.getChild("artifactSet"))
            .map(n -> n.getChild("includes"))
            .map(n -> n.getChildren("include"))
            .map(Arrays::stream)
            .orElse(Stream.empty())
            .map(Xpp3Dom::getValue)
            .collect(Collectors.toSet());

    Set<String> staleIncludes = new HashSet<>(includes);
    staleIncludes.removeAll(allDeps);
    if (!staleIncludes.isEmpty()) {
      LOGGER.warn("Found stale includes in shading config:");
      staleIncludes.forEach(LOGGER::warn);
    }

    // Empty includes implies that everything is included
    final Set<String> effectiveIncludes = includes.isEmpty() ? allDeps : includes;

    Set<String> excludes =
        Optional.ofNullable(configuration.getChild("artifactSet"))
            .map(n -> n.getChild("excludes"))
            .map(n -> n.getChildren("exclude"))
            .map(Arrays::stream)
            .orElse(Stream.empty())
            .map(Xpp3Dom::getValue)
            .collect(Collectors.toSet());

    Set<String> staleExcludes = new HashSet<>(excludes);
    staleExcludes.removeAll(allDeps);
    if (!staleExcludes.isEmpty()) {
      LOGGER.warn("Found stale excludes in shading config:");
      staleExcludes.forEach(LOGGER::warn);
    }

    // Compute all the deps that effectively excluded from the shaded jar
    return allDeps.stream()
        .filter(s -> !effectiveIncludes.contains(s) || excludes.contains(s))
        .collect(Collectors.toSet());
  }

  /** Extract the dependencies that were explicitly specified in the dependency-reduced-pom.xml */
  private Set<String> getOutputDependencies()
      throws MojoFailureException, DependencyResolutionException {
    File reducedPom = new File(project.getBasedir(), "dependency-reduced-pom.xml");
    MavenXpp3Reader reader = new MavenXpp3Reader();
    Model reduceMavenModel;
    try (FileInputStream fin = new FileInputStream(reducedPom)) {
      reduceMavenModel = reader.read(fin);
    } catch (XmlPullParserException | IOException e) {
      throw new MojoFailureException("Failed to read " + reducedPom.getName(), e);
    }

    CollectRequest collectRequest = new CollectRequest();

    reduceMavenModel.getDependencies().stream()
        .map(d -> String.format("%s:%s:%s", d.getGroupId(), d.getArtifactId(), d.getVersion()))
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
        .map(a -> String.format("%s:%s", a.getGroupId(), a.getArtifactId()))
        .collect(Collectors.toSet());
  }
}
