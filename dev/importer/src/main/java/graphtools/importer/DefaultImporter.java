/**
 * 
 */
package graphtools.importer;

import edu.utexas.arlut.cf.utils.GraphTransaction;
import graphtools.GraphRecord;
import graphtools.GraphRecordEdge;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Given sourceData, iterates through ioRecord of type R 
 * and populates provided graph.
 * @author deelam
 */
@RequiredArgsConstructor
@Slf4j
public class DefaultImporter<B> implements Importer<B> {
	
	private final Encoder<B> encoder;
	private final Populator populator;
	
	@Setter
	private int commitThreshold;
	
	@Override
	public void importFile(SourceData<B> sourceData, IdGraph graph) throws IOException {
		encoder.reinit(sourceData);
 		int tx=GraphTransaction.begin(graph);
		try{
			int gRecCounter=0;
			B inRecord;
			while((inRecord=sourceData.getNextRecord()) !=null){
				log.debug("-------------- row=" + inRecord);
				Collection<GraphRecord> gRecords=build(inRecord);
				//log.debug(gRecords.toString());
				gRecCounter+=gRecords.size();
				populator.populateGraph(graph, gRecords);
				if(gRecCounter>commitThreshold){
					GraphTransaction.commit(tx);
					gRecCounter=0;
				}
			}
			GraphTransaction.commit(tx);
		}catch(RuntimeException re){
			GraphTransaction.rollback(tx);
			throw re;
		}finally{
			encoder.close(sourceData);
		}
	}

	Map<String,GraphRecord> gRecords=new HashMap<>(40);
	@SuppressWarnings("unchecked")
	public Collection<GraphRecord> build(B bean) {
		RecordContext<B> rContext=encoder.createContext(bean);
		int numRelRules=encoder.getEntityRelationCount(rContext);
		// reuse map for efficiency, assumes this method is called for each bean, one at a time
		// which is true in the above implementation
		gRecords.clear(); 
		for(int i=0; i < numRelRules; ++i){
			@SuppressWarnings("rawtypes")
			EntityRelation relation=encoder.getEntityRelation(i);
			NodeFiller<RecordContext<B>> srcNodeFiller=relation.srcNodeFiller;
			NodeFiller<RecordContext<B>> dstNodeFiller=relation.dstNodeFiller;
			EdgeFiller<RecordContext<B>> edgeFiller=relation.edgeFiller;
			int numInstances=relation.numInstances(rContext);
			
			for(int j=0; j<numInstances; ++j){
				rContext.setInstanceIndex(j);
				GraphRecord outFv=null;
				{
					String outVertexId=srcNodeFiller.getId(rContext);
					if(outVertexId != null){
						outFv=gRecords.get(outVertexId);
						if(outFv==null){
							outFv=new GraphRecord(outVertexId, srcNodeFiller.getType());
							outFv.setProperty(IdGraph.ID, outVertexId);
							gRecords.put(outVertexId, outFv);
						}
						srcNodeFiller.fill(outFv, rContext);
					}
				}
	
				GraphRecord inFv=null;
				if(dstNodeFiller!=null){
					String inVertexId=dstNodeFiller.getId(rContext);
					if(inVertexId != null){
						inFv=gRecords.get(inVertexId);
						if(inFv==null){
							inFv=new GraphRecord(inVertexId, dstNodeFiller.getType());
							inFv.setProperty(IdGraph.ID, inVertexId);
							gRecords.put(inVertexId, inFv);
						}
						dstNodeFiller.fill(inFv, rContext);
					}
		
					String label=edgeFiller.getLabel();
					String edgeId=edgeFiller.getId(outFv, inFv, rContext);
					if(label != null && edgeId!=null && outFv != null && inFv != null){
						GraphRecordEdge fe=outFv.getOutEdge(edgeId);
						if(fe==null){
							fe=new GraphRecordEdge(edgeId, label, outFv, inFv);
							fe.setProperty(IdGraph.ID, edgeId);
							//fe.setProperty(CsvGraphFiller.LONG_ID_PROPKEY, generateEdgeId());
							outFv.addEdge(fe);
							//log.info("outEdges: "+Iterables.toString(outFv.getEdges(Direction.BOTH)));
							//log.info("Creating edge: "+fe);
			
							// use an empty edge to reduce merging properties later
							//StringIdEdgeWritable feEmpty=new StringIdEdgeWritable(label, outFv, inFv);
							if(!outFv.equals(inFv))  // if edge is self-edge, it has already been added to both IN and OUT edge tables
								inFv.addEdge(fe/*.emptyCopy()*/);
							//log.info("inEdges: "+Iterables.toString(inFv.getEdges(Direction.BOTH)));
						} else {
							log.info("Using existing edge: {}", edgeId);
							Preconditions.checkState(fe.getOutVertexStringId().equals(outFv.getStringId()));
							Preconditions.checkState(fe.getInVertexStringId().equals(inFv.getStringId()));
						}
						edgeFiller.fill(fe, rContext);
					}
				}
			}
		}
	
		return gRecords.values();
	}

}
