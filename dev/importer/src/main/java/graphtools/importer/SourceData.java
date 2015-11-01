/**
 * 
 */
package graphtools.importer;

import java.io.IOException;

/**
 * @author dlam
 *
 */
public interface SourceData<R> {

	/**
	 * 
	 * @return null if EOF
	 * @throws IOException
	 */
	R getNextRecord() throws IOException;

}
