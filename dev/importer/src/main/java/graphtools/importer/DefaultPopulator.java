package graphtools.importer;

import graphtools.GraphRecord;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@RequiredArgsConstructor
public class DefaultPopulator implements Populator{
	
	@Getter
	private final String importerName;
	
	private static final String IMPORTER_KEY = "_ingester";
	
	private void markRecords(Collection<GraphRecord> gRecords) {
		for(GraphRecord rec:gRecords){
			rec.setProperty(IMPORTER_KEY, importerName);
			for(Entry<String, Edge> e:rec.getOutEdges().entrySet()){
				e.getValue().setProperty(IMPORTER_KEY, importerName);
			}
			for(Entry<String, Edge> e:rec.getInEdges().entrySet()){
				e.getValue().setProperty(IMPORTER_KEY, importerName);
			}
		}
	}
		
	@Override
	public void populateGraph(IdGraph graph, Collection<GraphRecord> gRecords) {
		if(importerName!=null){
			markRecords(gRecords);
		}
	}
}

