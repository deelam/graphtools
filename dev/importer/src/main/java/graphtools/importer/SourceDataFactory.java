/**
 * 
 */
package graphtools.importer;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * @author dlam
 *
 */
public interface SourceDataFactory {

	SourceData<?> createFrom(File file) throws FileNotFoundException;

}
