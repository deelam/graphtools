package net.deelam;

import javax.inject.Inject;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphUri;


@Slf4j
public class PackageFaceter implements Faceter {

  public static final String facetName = "IdentityFaceter";

  public String getFaceterName() {
    return facetName;
  }

  @Inject
  public PackageFaceter(FacetingService facetingSvc) {
    facetingSvc.registerFaceter(this);
    log.info("new faceter: " + this);
  }

  @Override
  public GraphUri createFacet(GraphUri srcGraphUri, GraphUri facetGraphUri) throws Exception {
    IdGraph<?> srcGraph = srcGraphUri.openExistingIdGraph();
    IdGraph<?> graph = facetGraphUri.getGraph();
    
    for(Vertex v:srcGraph.getVertices()){
      String type = GraphRecord.getType(v);
      switch(type){
        case "class":{
          String packageName=getPackageName(v);
          graph.addVertex(packageName);
        }
        default:{
          graph.addVertex(v.getId());
        }
      }
    }
    
    facetGraphUri.shutdown();
    srcGraphUri.copyTo(facetGraphUri);
    //Graph inGraph = helper.openInputGraph(srcGraphUri);
    //checkNotNull(inGraph, "Could not open inGraph=" + srcGraphUri);
    return facetGraphUri;
  }

  private String getPackageName(Vertex v) {
    return (String) v.getId();// TODO
  }

}
