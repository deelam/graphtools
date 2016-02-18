/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;

import net.deelam.graphtools.GraphUri;

/**
 * @author deelam
 *
 */
public interface Importer<R> {

  void importFile(SourceData<R> sourceData, GraphUri graphUri) throws IOException;

}
