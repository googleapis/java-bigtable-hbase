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
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.apache.maven.model.Model;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Mojo(
    name = "verify-shading",
    defaultPhase = LifecyclePhase.VERIFY,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class VerifyShadingMojo extends AbstractMojo {
  private static final Logger LOGGER = LoggerFactory.getLogger(VerifyMirrorDependencyVersions.class);

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Parameter
  private List<String> allowedJarClassEntries;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    // NOTE: non-short circuit & is intentional to get all the errors
    boolean isOk = verifyShadedJarEntries() & verifyDependencyPromotion();

    if (!isOk) {
      throw new MojoFailureException("Shaded jar is invalid");
    }
  }

  /**
   * Verifies that all of the shaded files live under the specified package names
   *
   * @return true if all of the files are properly namespaced
   */
  private boolean verifyShadedJarEntries() throws MojoFailureException {
    System.out.println("artifact: " + project.getArtifact());
    System.out.println("file: " + project.getArtifact().getFile());

    List<String> allowedClassPrefixes = new ArrayList<>();
    allowedClassPrefixes.add("com/google/cloud/bigtable/");
    allowedClassPrefixes.add("com/google/bigtable/repackaged/");

    for (String allowedClassPrefix : allowedClassPrefixes) {
      System.out.println("allowed prefix: " + allowedClassPrefix);
    }

    JarFile jarFile;
    try {
      jarFile = new JarFile(project.getArtifact().getFile(), false);
    } catch (IOException e) {
      throw new MojoFailureException("Failed to open shaded jar for inspect", e);
    }
    Enumeration<JarEntry> entries = jarFile.entries();

    boolean isOk = true;

    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String name = entry.getName();

      // make sure all of the contents are relocated under bigtable
      if (name.endsWith(".class") && allowedJarClassEntries.stream().noneMatch(name::startsWith)) {
        System.out.println("Forbidden class path: " + name);
        isOk = false;
      }
    }

    return isOk;
  }

  /**
   * Verifies that all of the dependencies that were excluded from shading, are added as external
   * deps in the final dependency-reduced-pom.xml.
   *
   * <p>This doesn't currently consider transitive dependencies, so it expects that
   * dependency-reduced-pom.xml flattens all the unshaded deps.
   *
   * @return true if all of the dependencies are accounted for
   */
  private boolean verifyDependencyPromotion() throws MojoFailureException {
    Set<String> excludedDeps = getShadingExclusions();
    Set<String> outputDeps = getOutputDependencies();

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

    return missingDeps.isEmpty();
  }

  /** Extracts the artifact names that we excluded from shading */
  private Set<String> getShadingExclusions() throws MojoFailureException {
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

    Xpp3Dom[] excludeNodes =
        configuration.getChild("artifactSet").getChild("excludes").getChildren("exclude");

    return Arrays.stream(excludeNodes).map(Xpp3Dom::getValue).collect(Collectors.toSet());
  }

  /** Extract the dependencies that were explicitly specified in the dependency-reduced-pom.xml */
  private Set<String> getOutputDependencies() throws MojoFailureException {
    File reducedPom = new File(project.getBasedir(), "dependency-reduced-pom.xml");
    MavenXpp3Reader reader = new MavenXpp3Reader();
    Model reduceMavenModel;
    try (FileInputStream fin = new FileInputStream(reducedPom)) {
      reduceMavenModel = reader.read(fin);
    } catch (XmlPullParserException | IOException e) {
      throw new MojoFailureException("Failed to read " + reducedPom.getName(), e);
    }

    return reduceMavenModel.getDependencies().stream()
        .map(d -> String.format("%s:%s", d.getGroupId(), d.getArtifactId()))
        .collect(Collectors.toSet());
  }
}
