/*
 * Copyright 2026 Google LLC
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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the pom Maven is about to deploy still declares the dependencies that consumers
 * need to resolve.
 *
 * <p>This exists because maven-shade-plugin can silently swap the project's pom for a generated
 * dependency-reduced one. That is correct when the shaded jar *is* the main artifact, but wrong
 * when {@code shadedArtifactAttached} is set: there the main artifact is the thin, unshaded jar,
 * and stripping its dependencies leaves downstream consumers with nothing to resolve. MSHADE-419
 * does precisely that in shade >= 3.3.0.
 *
 * <p>Nothing inside the reactor can catch this, because Maven resolves sibling modules from their
 * source poms and never from the generated one, so the damage is only visible after deploy. Hence
 * this check reads {@link MavenProject#getFile()} — whichever pom is actually slated for deploy at
 * {@code verify} time — rather than the source pom on disk.
 */
@Mojo(name = "verify-published-pom-deps", defaultPhase = LifecyclePhase.VERIFY)
public class VerifyPublishedPomDepsMojo extends AbstractMojo {
  private static final Logger LOGGER = LoggerFactory.getLogger(VerifyPublishedPomDepsMojo.class);

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  /**
   * Coordinates, as {@code groupId:artifactId}, that must be declared in the published pom with a
   * scope that propagates to consumers (compile or runtime).
   */
  @Parameter(required = true)
  private List<String> requiredDependencies;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    File publishedPom = project.getFile();
    if (publishedPom == null || !publishedPom.isFile()) {
      // Whatever pom is slated for deploy has to be readable; if it isn't, fail loudly rather
      // than let a build that publishes an unverified pom look like it passed the check.
      throw new MojoFailureException(
          "There is no pom to verify, project.getFile() is " + publishedPom);
    }
    Set<String> declared = readPropagatingDependencies(publishedPom);

    List<String> missing = new ArrayList<>();
    for (String required : requiredDependencies) {
      if (!declared.contains(required)) {
        missing.add(required);
      }
    }

    if (missing.isEmpty()) {
      return;
    }

    Collections.sort(missing);
    for (String coordinate : missing) {
      LOGGER.error(
          "{} is missing from the pom that will be published ({}); consumers resolving this"
              + " artifact from a repository would not get it",
          coordinate,
          publishedPom);
    }
    LOGGER.error(
        "If {} is a generated dependency-reduced pom, set"
            + " <createDependencyReducedPom>false</createDependencyReducedPom> on maven-shade-plugin"
            + " (see MSHADE-419).",
        publishedPom.getName());

    throw new MojoFailureException("Published pom is missing required dependencies");
  }

  /** Reads the raw pom and returns the {@code groupId:artifactId} of its compile/runtime deps. */
  private Set<String> readPropagatingDependencies(File pom) throws MojoFailureException {
    Model model;
    try (FileInputStream fin = new FileInputStream(pom)) {
      model = new MavenXpp3Reader().read(fin);
    } catch (XmlPullParserException | IOException e) {
      throw new MojoFailureException("Failed to read " + pom, e);
    }

    Set<String> declared = new LinkedHashSet<>();
    for (Dependency dependency : model.getDependencies()) {
      String scope = dependency.getScope();
      // An absent scope means compile.
      if (scope != null && !"compile".equals(scope) && !"runtime".equals(scope)) {
        continue;
      }
      String groupId = dependency.getGroupId();
      String artifactId = dependency.getArtifactId();
      // A half-declared coordinate cannot satisfy a requirement anyway, so drop it and let the
      // dependency it was meant to be get reported as missing.
      if (groupId == null || artifactId == null) {
        continue;
      }
      declared.add(resolveGroupId(groupId) + ":" + artifactId);
    }
    return declared;
  }

  /**
   * Maven deploys the raw pom, so a dependency on a sibling module is still written as {@code
   * ${project.groupId}} rather than the resolved value.
   */
  private String resolveGroupId(String groupId) {
    if ("${project.groupId}".equals(groupId) || "${pom.groupId}".equals(groupId)) {
      return project.getGroupId();
    }
    return groupId;
  }
}
