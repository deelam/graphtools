/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;

/**
 * @author deelam
 *
 */
public interface SourceData<R> {

  /**
   * 
   * @return null if EOF
   * @throws IOException
   */
  R getNextRecord() throws IOException;

  int getPercentProcessed();

}
