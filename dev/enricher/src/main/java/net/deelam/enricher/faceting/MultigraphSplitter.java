/**
 * 
 */
package net.deelam.enricher.faceting;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.commons.lang.mutable.MutableLong;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.enricher.indexing.IdMapper;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.PropertyMerger;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;

/**
 * @author dnlam
 *
 */
@RequiredArgsConstructor
@Slf4j
public class MultigraphSplitter {

  public static void main(String[] args) throws IOException {
    IdGraphFactoryTinker.register();
    GraphUri guri=new GraphUri("tinker:///");
    guri.getConfig().setProperty(GraphUri.CREATE_META_DATA_NODE, false);
    IdGraph<?> graph=guri.createNewIdGraph(true);
    
    GraphMLReader reader=new GraphMLReader(graph);
    reader.setVertexIdKey(IdGraph.ID);
    reader.setEdgeIdKey(IdGraph.ID);
    reader.setEdgeLabelKey("Label");
    reader.inputGraph("../../../adidis/bundlesSlave/817a-output_graphs/union Facet.graphml");
    System.out.println(GraphUtils.toString(graph, 30));
    
    MultigraphConsolidator consolidator=new MultigraphConsolidator(guri, true);
    MultigraphSplitter splitter=new MultigraphSplitter(consolidator);
    String srcGraphUri="neo4j:/tmp/adidis-sessions/query1-62887-817a/src3-63308-dummy_corr.csv";
    GraphUri srcGuri=new GraphUri("tinker:///");
    IdGraph<?> srcGraph=srcGuri.createNewIdGraph(true);
    splitter.extractTo(srcGraph, srcGuri.createPropertyMerger(), srcGraphUri);
    System.out.println(GraphUtils.toString(srcGraph, 30));
  }

  private final MultigraphConsolidator consolidator;
  
  public void extractTo(IdGraph<?> outGraph, PropertyMerger merger, String srcGraphUri){

    IdMapper idMapper = consolidator.getGraphIdMapper();
    String shortGraphId = idMapper.getShortIdIfExists(srcGraphUri);
    checkNotNull(shortGraphId);
    
    String graphIdPropKey = consolidator.origNodeCodec.getGraphIdPropKey();
    /// get all elements with shortGraphId
    log.info("Iterating through {}={} ", graphIdPropKey, shortGraphId);
    consolidator.getGraph().getVertices(graphIdPropKey, shortGraphId).forEach(v->{
      log.debug("{} {} ", consolidator.getOrigId(v), v /*GraphUtils.toString(v)*/);
      importVertex(v, outGraph, merger);
    });
  }

  private Vertex importVertex(Vertex v, IdGraph<?> graph, PropertyMerger merger) {
    String origId = consolidator.getOrigId(v);
    Vertex copyV = graph.getVertex(origId);
    if (copyV == null) {
      copyV = graph.addVertex(origId);
    } else {
      log.warn("Node should not exist: {} {}", origId, copyV);
    }
    merger.mergeProperties(v, copyV);
    consolidator.origNodeCodec.removeOrigNodeProperties(copyV);
    return copyV;
  }
  
}
