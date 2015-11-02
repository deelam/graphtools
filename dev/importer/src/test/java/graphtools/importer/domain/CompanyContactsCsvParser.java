/**
 * 
 */
package graphtools.importer.domain;

import graphtools.importer.csv.CsvParser;
import graphtools.importer.csv.CsvUtils;
import graphtools.importer.csv.FieldList;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.prefs.CsvPreference;

/**
 * @author deelam
 *
 */
public class CompanyContactsCsvParser implements CsvParser<CompanyContactBean> {

	private static final char DELIMITER=',';
	
	@Override
	public boolean shouldIgnore(String rowStr) {
		String field1=CsvUtils.getFirstField(rowStr, DELIMITER);
		if(field1 == null)
			return true;
		return field1.contains("first_name");
	}

	@Override
	public CsvPreference getCsvPreferences() {
		return new CsvPreference.Builder('"', DELIMITER, "\n")
		.surroundingSpacesNeedQuotes(true).build();
	}

	@Override
	public Class<CompanyContactBean> getBeanClass() {
		return CompanyContactBean.class;
	}

	@Override
	public String[] getCsvFields() {
		return fields.getCsvFields();
	}
	@Override
	public CellProcessor[] getCellProcessors() {
		return fields.getCellProcessors();
	}

	static FieldList fields=new FieldList(CompanyContactBean.class);
	static{ // IMPORTANT: strings must match bean field names

		fields.add("firstName");
		fields.add("lastName");
		fields.add("company");
		
		fields.add("address");
		fields.add("city");
		fields.add("county");
		fields.add("state");
		fields.add("zip", new ParseInt());
		fields.add("phone1");
		fields.add("phone2");

		fields.add("email");
		fields.ignore("web");
	};
}
