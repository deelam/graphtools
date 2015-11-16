package net.deelam.graphtools;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;

@Slf4j
public class GraphExporter {

  public static void exportGraphml(Graph graph, String filename) throws IOException {
    exportGraphml(graph, filename, false);
  }

  public static void exportGraphml(Graph graph, String filename, boolean prettyPrint)
      throws IOException {
    // Not efficient but it works for now
    log.info("Exported graph {} to {}", graph, filename);
    GraphMLWriter.outputGraph(graph, filename);
    if (prettyPrint) {
      PrettyPrintXml.prettyPrint(filename, filename + "-pretty.graphml");
    }
  }

  public static void exportGson(Graph graph, String filename) throws IOException {
    log.info("Exported graph {} to {}", graph, filename);
    GraphSONWriter.outputGraph(graph, filename);
  }

}
