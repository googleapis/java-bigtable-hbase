/*
 * Copyright 2023 Google LLC
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.resolver.filter.ScopeArtifactFilter;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.DefaultProjectBuildingRequest;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuildingRequest;
import org.apache.maven.shared.dependency.graph.DependencyCollectorBuilder;
import org.apache.maven.shared.dependency.graph.DependencyCollectorBuilderException;
import org.apache.maven.shared.dependency.graph.DependencyNode;
import org.apache.maven.shared.dependency.graph.traversal.DependencyNodeVisitor;
import org.eclipse.aether.util.artifact.JavaScopes;

@Mojo(
    name = "verify-provided-deps",
    defaultPhase = LifecyclePhase.VALIDATE,
    requiresDependencyCollection = ResolutionScope.TEST,
    threadSafe = true)
public class VerifyProvidedDeps extends AbstractMojo {
  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Parameter(defaultValue = "${session}", readonly = true, required = true)
  private MavenSession session;

  @Component(hint = "default")
  private DependencyCollectorBuilder dependencyCollectorBuilder;

  @Override
  public void execute() throws MojoExecutionException {
    ProjectBuildingRequest buildingRequest =
        new DefaultProjectBuildingRequest(session.getProjectBuildingRequest());
    buildingRequest.setProject(project);

    final DependencyNode rootNode;

    try {
      rootNode = dependencyCollectorBuilder.collectDependencyGraph(buildingRequest, new ScopeArtifactFilter(Artifact.SCOPE_COMPILE_PLUS_RUNTIME));
    } catch (DependencyCollectorBuilderException exception) {
      throw new MojoExecutionException("Cannot build project dependency graph", exception);
    }


    ProvidedDepsCollector collector = new ProvidedDepsCollector();
    rootNode.accept(collector);

    ErrorCollector conflictCollector = new ErrorCollector(collector.getResults());
    rootNode.accept(conflictCollector);

    for (String error : new TreeSet<>(conflictCollector.getErrors())) {
      getLog().error(error);
    }
    if (!conflictCollector.getErrors().isEmpty()) {
      throw new MojoExecutionException("Found conflicts in provided deps, see above");
    }
  }

  static class ProvidedDepsCollector implements DependencyNodeVisitor {
    // ArtifactKey -> { scope -> [directArtifact] }
    private Set<String> deps = new HashSet<>();
    private int providedDepth = 0;

    @Override
    public boolean visit(DependencyNode node) {
      Artifact artifact = node.getArtifact();
      String scope = artifact.getScope();

      if (providedDepth > 0 || JavaScopes.PROVIDED.equals(scope)) {
        providedDepth++;
      }

      if (providedDepth > 0) {
        deps.add(artifactKey(node.getArtifact()));
      }
      return true;
    }

    @Override
    public boolean endVisit(DependencyNode node) {
      if (providedDepth > 0) {
        providedDepth--;
      }
      return true;
    }


    Set<String> getResults() {
      return deps;
    }
  }

  class ErrorCollector implements DependencyNodeVisitor {
    // stack[0] = the module being built
    // stack[1] = direct dep
    // stack[...] = transitive deps
    private Stack<DependencyNode> stack = new Stack<>();
    private Set<String> providedArtifactKeys;

    private List<String> errors = new ArrayList<>();

    ErrorCollector(Set<String> providedArtifactKeys) {
      this.providedArtifactKeys = providedArtifactKeys;
    }

    @Override
    public boolean visit(DependencyNode node) {
      stack.push(node);

      // Ignore the module root
      if (stack.size() == 1) {
        return true;
      }

      // No need to check top level provided deps
      if (JavaScopes.PROVIDED.equals(stack.get(1).getArtifact().getScope())) {
        return true;
      }
      // If the transitive dep is already forced to provided
      if (JavaScopes.PROVIDED.equals(node.getArtifact().getScope())) {
        return true;
      }

      String key = artifactKey(node.getArtifact());
      if (!providedArtifactKeys.contains(key)) {
        return true;
      }

      String topLevelKey = artifactKey(stack.get(1).getArtifact());
      errors.add(String.format("%s imports %s, which is supposed to be provided", topLevelKey, key));


      return true;
    }

    @Override
    public boolean endVisit(DependencyNode node) {
      stack.pop();
      return true;
    }

    public List<String> getErrors() {
      return errors;
    }
  }

  static String artifactKey(Artifact a) {
    return String.join(":",
        a.getGroupId(),
        a.getArtifactId(),
        a.getType(),
        a.getClassifier()
    );
  }
}
