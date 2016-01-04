package net.deelam;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;

@Singleton
@Slf4j
public class FacetingService {

  private Map<String, Faceter> faceters = new HashMap<>();

  public void registerFaceter(Faceter faceter) {
    faceters.put(faceter.getFaceterName(), faceter);
  }

  public void createFacet(String faceterName, GraphUri srcGraphUri, GraphUri facetGraphUri) throws IOException {
    Faceter faceter = faceters.get(faceterName);
    if (faceter == null) {
      throw new RuntimeException("Faceter not found: " + faceterName);
    }    
    log.info("Creating facet with {}", faceter);

    // initialize graph, accessible via graphUri.getGraph()
    facetGraphUri.createNewIdGraph(true);
    try {
      facetGraphUri = faceter.createFacet(srcGraphUri, facetGraphUri);
      if(facetGraphUri.getGraph()!=null) facetGraphUri.getGraph().commit();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      facetGraphUri.shutdown();
    }
  }

}
