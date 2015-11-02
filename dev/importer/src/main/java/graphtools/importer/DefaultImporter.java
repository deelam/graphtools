/**
 * 
 */
package graphtools.importer;

import graphtools.GraphRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Given sourceData, iterates through ioRecord of type R 
 * and populates provided graph.
 * @author deelam
 */
@RequiredArgsConstructor
@Slf4j
public class DefaultImporter<R> implements Importer<R> {
	
	private final Encoder<R> encoder;
	private final Populator populator;
	private final GraphRecordBuilder<R> grBuilder;
	
	public DefaultImporter(Encoder<R> encoder, Populator populator) {
		super();
		this.encoder = encoder;
		this.populator = populator;
		grBuilder=new GraphRecordBuilder<R>(encoder);
	}

	@Override
	public void importFile(SourceData<R> sourceData, IdGraph graph) throws IOException {
		encoder.reinit(sourceData);
		try{// TODO: open graphTransaction
			R ioRecord;
			while((ioRecord=sourceData.getNextRecord()) !=null){
				log.debug("-------------- row=" + ioRecord);
				Collection<GraphRecord> gRecords=grBuilder.build(ioRecord);
				//log.debug(gRecords.toString());
				
				populator.populateGraph(graph, gRecords);
			}
		}finally{
			encoder.close(sourceData);
		}
	}

}
