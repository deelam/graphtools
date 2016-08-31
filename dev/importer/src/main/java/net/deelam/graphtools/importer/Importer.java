/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Map;

import net.deelam.graphtools.GraphUri;

/**
 * @author deelam
 *
 */
public interface Importer<R> {

  void importFile(SourceData<R> sourceData, GraphUri graphUri, Map<String, Number> metrics) throws IOException;

}
