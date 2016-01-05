package net.deelam;

import net.deelam.graphtools.GraphUri;

/**
 * @author dnlam, Created:Oct 6, 2015
 */
public interface Faceter {
	String getFaceterName();
//	boolean needsOutputGraphCreated(); // whether the outputGraph should be created by FacetingService
	
	/**
	 * @param facetGraphUri graphUri encapsulating a graph; use facetGraphUri.getGraph()
	 * @return typically the same facetGraphUri or possibly a new one
	 */
	GraphUri createFacet(GraphUri srcGraphUri, GraphUri facetGraphUri) throws Exception;
	
}
