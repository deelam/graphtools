/**
 * 
 */
package net.deelam.graphtools.importer.csv;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;

/**
 * @author deelam
 *
 */
public interface CsvParser<B> {

  boolean shouldIgnore(String rowStr);

  CsvPreference getCsvPreferences();

  Class<B> getBeanClass();

  String[] getCsvFields();

  CellProcessor[] getCellProcessors();

}
