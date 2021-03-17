/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@RunWith(JUnit4.class)
public class AssemblyConfigTest {

  /**
   * This test ensures that assembly/shaded-byo-hadoop.xml {@code includes} match the expected HBase
   * marpreduce classpath. It parses the file to find the coordinates and compares them to the
   * classpath that {@link TableMapReduceUtil} generates.
   *
   * <p>This test expects that shaded-byu-hadoop will only have a single {@code dependencySet} to represent
   * the deps required to run hbase mapreduce jars. It identifies the correct {@code dependencySet}
   * by finding an {@code include} for {@code hbase-common} in the {@code provided} {@code scope}.
   */
  @Test
  public void verifyBYOHadoopIncludes() throws Exception {
    // If this fails, the provided dependencySet in assembly/shaded-byo-hadoop.xml should be updated
    // to reflect the deps specified by TableMapReduceUtil.
    Assert.assertEquals(
        "The assembly descriptor `shaded-byo-hadoop.xml` must be in sync with TableMapReduceUtil dependency jars",
        extractHBaseBaseArtifactDeps(),
        extractAssemblyBaseArtifactIncludes(new File("assembly/shaded-byo-hadoop.xml")));
  }

  private static Set<String> extractAssemblyBaseArtifactIncludes(File file)
      throws ParserConfigurationException, XPathExpressionException, IOException, SAXException {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file);
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPathExpression depSetPath = xPathFactory.newXPath().compile(
        "/assembly/dependencySets/dependencySet[scope='provided' and includes/include='org.apache.hbase:hbase-common']");

    NodeList depSetNodes = (NodeList) depSetPath.evaluate(doc, XPathConstants.NODESET);
    Assert.assertEquals("Expected single dependencySet with provided hbase-common", 1,
        depSetNodes.getLength());

    Node depSetNode = depSetNodes.item(0);
    depSetPath = xPathFactory.newXPath().compile("includes/include");
    NodeList includeNodes = (NodeList) depSetPath.evaluate(depSetNode, XPathConstants.NODESET);

    Set<String> actualIncludeIds = new HashSet<>();
    for (int i = 0; i < includeNodes.getLength(); i++) {
      String versionlessCoordinate = includeNodes.item(i).getTextContent();
      String artifactId = versionlessCoordinate.split(":")[1];
      actualIncludeIds.add(artifactId);
    }

    return actualIncludeIds;
  }

  /**
   * Extract the required base artifact ids from HBase's classpath.
   *
   * <p>This uses a heuristic that converts jar names into artifact ids. This approach is fragile,
   * but shouldnt need to be updated much.
   */
  private static Set<String> extractHBaseBaseArtifactDeps() throws IOException {
    Configuration configuration = new Configuration(false);
    TableMapReduceUtil.addHBaseDependencyJars(configuration);
    String classpath = TableMapReduceUtil.buildDependencyClasspath(configuration);

    Set<String> expectedIncludeIds = new HashSet<>();
    for (String pathStr : classpath.split(":")) {

      String filename = new File(pathStr).getName();
      // extract the artifact base name from the filename, stripping version and extension
      String artifactId = filename.replaceAll(
          "(.*)-\\d+\\.\\d+(\\.\\d+)?(?:-(?:incubating|alpha|beta)|\\.Final)?\\.jar$", "$1");
      expectedIncludeIds.add(artifactId);
    }
    return expectedIncludeIds;
  }
}
