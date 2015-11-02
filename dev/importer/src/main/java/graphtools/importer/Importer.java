/**
 * 
 */
package graphtools.importer;

import java.io.IOException;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 *
 */
public interface Importer<R> {

	void importFile(SourceData<R> sourceData, IdGraph graph) throws IOException;

}
