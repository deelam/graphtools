package net.deelam;

import javax.inject.Inject;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;


@Slf4j
public class IdentityFaceter implements Faceter {

  public static final String facetName = "IdentityFaceter";

  public String getFaceterName() {
    return facetName;
  }

  @Inject
  public IdentityFaceter(FacetingService facetingSvc) {
    facetingSvc.registerFaceter(this);
    log.info("new faceter: " + this);
  }

  @Override
  public GraphUri createFacet(GraphUri srcGraphUri, GraphUri facetGraphUri) throws Exception {
    facetGraphUri.shutdown();
    srcGraphUri.copyTo(facetGraphUri);
    //Graph inGraph = helper.openInputGraph(srcGraphUri);
    //checkNotNull(inGraph, "Could not open inGraph=" + srcGraphUri);
    return facetGraphUri;
  }

}
