/**
 * 
 */
package graphtools.importer;

import graphtools.GraphUri;
import graphtools.importer.csv.CsvBeanSourceDataFactory;
import graphtools.importer.domain.CompanyContactBean;
import graphtools.importer.domain.CompanyContactsCsvParser;
import graphtools.importer.domain.CompanyContactsEncoder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.Test;

/**
 * @author deelam
 */
public class ImportingTest {
	
	ImporterManager mgr=new ImporterManager();
	
	@Before
	public void setup(){
		mgr.register("companyContactsCsv",
				new CsvBeanSourceDataFactory<CompanyContactBean>(new CompanyContactsCsvParser()), 
				new DefaultImporter<CompanyContactBean>(
						new CompanyContactsEncoder(), 
						new DefaultPopulator("telephoneCsv")
					)
				);		
	}
	
	@Test
	public void importCsvFileTest() throws IOException, URISyntaxException {
		File csvFile = new File(getClass().getResource("/us-500.csv").getFile());
		mgr.importFile("companyContactsCsv", csvFile, new GraphUri("tinker:///./us500test?fileType=graphml"));
	}
}
