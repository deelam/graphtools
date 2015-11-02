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

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author deelam
 */
public class ImportingTest {
	
	static ImporterManager mgr=new ImporterManager();
	
	@BeforeClass
	public static void setup() throws IOException{
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
		
		FileUtils.deleteDirectory(new File("target/us500test"));
		mgr.importFile("companyContactsCsv", csvFile, new GraphUri("tinker:///./target/us500test?fileType=graphml"));
	}
}
