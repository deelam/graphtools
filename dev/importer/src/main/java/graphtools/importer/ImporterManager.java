package graphtools.importer;

import graphtools.GraphUri;
import graphtools.importer.csv.CsvBeanSourceDataFactory;
import graphtools.importer.csv.domain.TelephoneBean;
import graphtools.importer.csv.domain.TelephoneCsvParser;
import graphtools.importer.csv.domain.TelephoneEncoder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.AllArgsConstructor;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * 
 * @author deelam
 */
public class ImporterManager {
	@AllArgsConstructor
	public static class Ingester{
		String id;
		SourceDataFactory sdFactory;
		Importer importer;
	}
	
	private Map<String,Ingester> registry=new HashMap<>();
	public Ingester register(String id, SourceDataFactory sdFactory, Importer importer){
		return registry.put(id, new Ingester(id, sdFactory, importer));
	}
	public Set<String> getIngesterList(){
		return registry.keySet();
	}
		
	public static void main(String[] args) throws IOException {
		ImporterManager mgr=new ImporterManager();
		
		mgr.register("telephoneCsv",
				new CsvBeanSourceDataFactory<TelephoneBean>(new TelephoneCsvParser()), 
				new DefaultImporter<TelephoneBean>(new TelephoneEncoder(), new DefaultPopulator("telephoneCsv")));
		
		mgr.importFile(args[0], new File(args[1]), new GraphUri(args[2]));
	}
	
	@SuppressWarnings("unchecked")
	public void importFile(String ingesterId, File file, GraphUri destName) throws IOException{
		// get ingester
		Ingester ingester = registry.get(ingesterId);
		Preconditions.checkNotNull(ingester, "ingester not registered: "+ingesterId);

		// create graph
		Configuration conf=new BaseConfiguration();
		IdGraph<?> graph=destName.openIdGraph(conf);
		Preconditions.checkNotNull(graph, "Could not open graph: "+graph);
		
		// apply ingester on graph
		SourceData<?> sData=ingester.sdFactory.createFrom(file);
		ingester.importer.importFile(sData, graph);
		
		// close graph
		graph.shutdown();
	}
}
