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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Mojo(
    name = "verify-shaded-jar-entries",
    defaultPhase = LifecyclePhase.VERIFY,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class VerifyShadedJarEntriesMojo extends AbstractMojo {
  private static final Logger LOGGER = LoggerFactory.getLogger(VerifyShadedJarEntriesMojo.class);

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Parameter private List<String> allowedJarClassEntries;

  /** Verifies that all of the shaded files live under the specified package names */
  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    JarFile jarFile;
    try {
      jarFile = new JarFile(project.getArtifact().getFile(), false);
    } catch (IOException e) {
      throw new MojoFailureException("Failed to open shaded jar for inspect", e);
    }

    List<String> forbiddenPaths =
        Collections.list(jarFile.entries()).stream()
            .map(ZipEntry::getName)
            .filter(s -> s.endsWith(".class"))
            .filter(s -> allowedJarClassEntries.stream().noneMatch(s::startsWith))
            .sorted()
            .collect(Collectors.toList());

    if (!forbiddenPaths.isEmpty()) {
      LOGGER.error("Found forbidden entries in the shaded jar:");
      forbiddenPaths.forEach(LOGGER::error);
      throw new MojoFailureException("Found forbidden jar entries");
    }
  }
}
