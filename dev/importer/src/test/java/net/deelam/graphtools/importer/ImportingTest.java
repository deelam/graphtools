/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.importer.DefaultImporter;
import net.deelam.graphtools.importer.DefaultPopulator;
import net.deelam.graphtools.importer.ImporterManager;
import net.deelam.graphtools.importer.csv.CsvBeanSourceDataFactory;
import net.deelam.graphtools.importer.domain.CompanyContactBean;
import net.deelam.graphtools.importer.domain.CompanyContactsCsvParser;
import net.deelam.graphtools.importer.domain.CompanyContactsEncoder;

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
