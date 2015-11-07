package graphtools;

import java.io.IOException;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
@RequiredArgsConstructor
@Slf4j
public final class GraphSchemaStats {

	final String nodeTypeProperty;
	
	@Setter
	private String[] subTypePropertyKeys=new String[0];

	public Graph getSchema(Graph inputGraph) throws IOException{
		//GraphSchemaStats util=new GraphSchemaStats(nodeTypeProperty);
		Graph schemaG=new IdGraph<>(new TinkerGraph());
		extractSchema(inputGraph, schemaG);
		return schemaG;
	}

	public void extractSchema(Graph graph, Graph schema){
		long nodeCount=0, edgeCount=0;;
		for(Vertex v:graph.getVertices()){
			++nodeCount;
			int outEdgeSize=Iterables.size(v.getEdges(Direction.OUT));
			edgeCount+=outEdgeSize;
			boolean outEdgeCountEmpty=(outEdgeSize==0);
			boolean inEdgeCountEmpty=Iterables.isEmpty(v.getEdges(Direction.IN));
			if(outEdgeCountEmpty){
				if(inEdgeCountEmpty){
					// unconnected node
					Vertex typeV=getTypeVertex(schema, v, null);
					extractProperties(v, typeV, "__NODES_WITH_NO_EDGES");
				}else{
					Vertex inTypeV=getTypeVertex(schema, v, Direction.IN);
					// this inVertex will be considered during extractEdgeSchema()
					extractProperties(v, inTypeV, "__NODES_WITH_IN_EDGES_ONLY");
				}
			} else {
				Vertex typeV=getTypeVertex(schema, v, Direction.OUT);
				incrementCount(typeV, "__NODES_WITH_OUT_EDGES");
				if(inEdgeCountEmpty){
					extractProperties(v, typeV, "__NODES_WITH_OUT_EDGES_ONLY");
				}
				
				extractEdgeSchema(schema, v);
			}
			if(!inEdgeCountEmpty){
				Vertex inTypeV=getTypeVertex(schema, v, Direction.IN);
				incrementCount(inTypeV, "__NODES_WITH_IN_EDGES");
			}
		}
		log.debug("Total: {} nodes and {} edges", nodeCount, edgeCount);
	}

	private static final String ID_PROPKEY = "IdGraph."+IdGraph.ID;
	private static void extractProperties(Element v, Element typeV, String countKey){
		incrementCount(typeV, countKey);
		
		for(String k:v.getPropertyKeys()){
			if(k.equals(IdGraph.ID)){
				String newKey=ID_PROPKEY;
				typeV.setProperty(newKey, v.getId().getClass().getSimpleName());
			}else{
				String existingSimpleClassName=typeV.getProperty(k);
				String className=v.getProperty(k).getClass().getSimpleName();
				if(!existingSimpleClassName.contains(className)){
					typeV.setProperty(k, existingSimpleClassName+","+className);
				}
			}
		}
	}

	private static void incrementCount(Element typeV, String countKey){
		if(countKey!=null){
			MutableInt counter=typeV.getProperty(countKey);
			if(counter==null){
				counter=new MutableInt();
				typeV.setProperty(countKey, counter);
			}
			counter.increment();
		}
	}

	static final String UNKNOWN_TYPE="Unknown";
	private Vertex getTypeVertex(Graph schema, Vertex v, Direction direction){
		Object type=v.getProperty(nodeTypeProperty);
		if(type == null){
			type=UNKNOWN_TYPE;
		}

		StringBuilder idSB=new StringBuilder(type.toString());
		for(String subTypePropKey:subTypePropertyKeys){
			String subType=v.getProperty(subTypePropKey);
			if(subType!=null)
				idSB.append("_").append(subType);
		}
		String id=idSB.append("_").append(direction).toString();
		
		Vertex typeV=schema.getVertex(id);
		if(typeV == null){
			typeV=schema.addVertex(id);
		}
		return typeV;
	}

	static final String UNKNOWN_LABEL="Unknown";
	private void extractEdgeSchema(Graph schema, Vertex outV){
		Vertex outTypeV=getTypeVertex(schema, outV, Direction.OUT);
		for(Edge e:outV.getEdges(Direction.OUT)){
			incrementCount(outTypeV, "__OUT_EDGES");

			Vertex inV=e.getVertex(Direction.IN);
			Vertex inTypeV=getTypeVertex(schema, inV, Direction.IN);
			extractProperties(inV, inTypeV, "__IN_EDGES");

			Edge typeE = getTypeEdge(schema, outTypeV, e, inTypeV);
			extractProperties(e, typeE, "__EDGES");
		}
	}

	private Edge getTypeEdge(Graph schema, Vertex outTypeV, Edge e, Vertex inTypeV) {
		String edgeId=new StringBuilder(outTypeV.getId().toString())
			.append("->").append(inTypeV.getId()).toString();
		
		Edge typeE=schema.getEdge(edgeId);
		if(typeE == null){
			String label=e.getLabel();
			if(label == null){
				label=UNKNOWN_LABEL;
			}
			typeE=schema.addEdge(edgeId, outTypeV, inTypeV, label);
		}
		return typeE;
	}

}
