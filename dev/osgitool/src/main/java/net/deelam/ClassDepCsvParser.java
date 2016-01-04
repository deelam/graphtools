/**
 * 
 */
package net.deelam;

import net.deelam.graphtools.importer.csv.CsvParser;
import net.deelam.graphtools.importer.csv.CsvUtils;
import net.deelam.graphtools.importer.csv.FieldList;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;

/**
 * @author deelam
 *
 */
public class ClassDepCsvParser implements CsvParser<ClassDepBean> {

	private static final char DELIMITER=',';
	
	@Override
	public boolean shouldIgnore(String rowStr) {
		String field1=CsvUtils.getFirstField(rowStr, DELIMITER);
		if(field1 == null)
			return true;
		return false;
	}

	@Override
	public CsvPreference getCsvPreferences() {
		return new CsvPreference.Builder('"', DELIMITER, "\n")
		.surroundingSpacesNeedQuotes(true).build();
	}

	@Override
	public Class<ClassDepBean> getBeanClass() {
		return ClassDepBean.class;
	}

	@Override
	public String[] getCsvFields() {
		return fields.getCsvFields();
	}
	@Override
	public CellProcessor[] getCellProcessors() {
		return fields.getCellProcessors();
	}

	static FieldList fields=new FieldList(ClassDepBean.class);
	static{ // IMPORTANT: strings must match bean field names

		fields.add("srcJar");
		fields.add("dstJar");
		
		fields.add("srcClass");
		fields.add("dstClass");
	};
}
