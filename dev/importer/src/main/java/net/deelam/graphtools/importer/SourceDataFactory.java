/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * @author deelam
 *
 */
public interface SourceDataFactory {

  SourceData<?> createFrom(File file) throws FileNotFoundException;

  SourceData<?> createFrom(Readable readable);

  SourceData<?> create();

}
