package graphtools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

@Slf4j
@NoArgsConstructor
public class StringIdNodeWritable extends StringIdElementWritable implements Vertex, Serializable {
	private static final long serialVersionUID=201508251201L;

	@Setter
	private static String NODE_TYPE_KEY = "__type";
	
	public StringIdNodeWritable(String strId) {
		super(strId);
	}
	
	public StringIdNodeWritable(final String strId, String nodeType){
		super(strId);
		setProperty(NODE_TYPE_KEY, nodeType);
	}

	// TODO: 5: address supernode memory problem (i.e., in-memory Map of in and out edges) by splitting node.  How does Faunus/Gremlin address this?
	private static final int SUPERNODE_THRESHOLD=100000;

	@Getter
	Map<String,Edge> inEdges=new HashMap<>();
	@Getter
	Map<String,Edge> outEdges=new HashMap<>();

	public StringIdEdgeWritable getInEdge(String edgeId){
		return (StringIdEdgeWritable) inEdges.get(edgeId);
	}

	public StringIdEdgeWritable getOutEdge(String edgeId){
		return (StringIdEdgeWritable) outEdges.get(edgeId);
	}

	@Override
	public boolean equals(Object obj){
		return super.equals(obj);
	}

	@Override
	public int hashCode(){
		return super.hashCode();
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex){
		StringIdEdgeWritable edge=new StringIdEdgeWritable(label, this, inVertex);
		outEdges.put(edge.getStringId(), edge);
		return edge;
	}

	public void addEdge(StringIdEdgeWritable edge){
		// check that this is one of the edge's endpoints
		boolean valid=false;
		if(edge.getVertex(Direction.IN).equals(this)){
			valid=true;
			if(inEdges.get(edge.getStringId()) != null)
				new RuntimeException("InEdge already exists with id=" + edge.getStringId()).printStackTrace();
			inEdges.put(edge.getStringId(), edge);

			if(inEdges.size() % SUPERNODE_THRESHOLD == 0){ // supernode warning
				log.warn("Vertex id={} has at least {} in-edges!", getId(), inEdges.size());
			}

		}
		// edge could point to self, so add edge to both IN and OUT edge tables
		if(edge.getVertex(Direction.OUT).equals(this)){
			valid=true;
			if(outEdges.get(edge.getStringId()) != null)
				new RuntimeException("OutEdge already exists with id=" + edge.getStringId()).printStackTrace();
			outEdges.put(edge.getStringId(), edge);

			if(outEdges.size() % SUPERNODE_THRESHOLD == 0){ // supernode warning
				log.warn("Vertex id={} has at least {} out-edges!", getId(), outEdges.size());
			}
		}
		if(!valid){
			log.error("Edge does not connect to this vertex=" + this + ": edge: outV=" + edge.getVertex(Direction.OUT)
				+ " inV=" + edge.getVertex(Direction.IN) + " " + GraphUtils.toString(edge));
		}
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels){
		Set<String> labelSet=new HashSet<>(Arrays.asList(labels));

		if(direction == Direction.IN){
			return filterEdges(inEdges.values(), labelSet);
		}else if(direction == Direction.OUT){
			return filterEdges(outEdges.values(), labelSet);
		}else{
			List<Edge> bothEs=new ArrayList<>();
			Iterable<Edge> inE=filterEdges(inEdges.values(), labelSet);
			Iterables.addAll(bothEs, inE);
			Iterable<Edge> outE=filterEdges(outEdges.values(), labelSet);
			Iterables.addAll(bothEs, outE);
			return bothEs;
		}
	}

	Iterable<Edge> filterEdges(Collection<Edge> edges, Set<String> labelSet){
		ArrayList<Edge> filteredEdges=new ArrayList<>(edges.size());
		for(Edge e:edges){
			if(labelSet.size() > 0 && !labelSet.contains(e.getLabel()))
				continue;
			filteredEdges.add(e);
		}
		return Iterables.unmodifiableIterable(filteredEdges);
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels){
		Direction oppDir=getOppositeDirection(direction);

		List<Vertex> otherVs=new ArrayList<>();
		for(Edge e:getEdges(direction, labels)){
			if(oppDir == null){ // direction=BOTH
				Vertex inV=e.getVertex(Direction.IN);
				if(!inV.equals(this)){
					otherVs.add(inV);
				}else{
					otherVs.add(e.getVertex(Direction.OUT));
				}
			}else{
				otherVs.add(e.getVertex(oppDir));
			}
		}
		return Iterables.unmodifiableIterable(otherVs);
	}

	public static Direction getOppositeDirection(Direction direction){
		//return direction.opposite();
		if(direction == Direction.IN)
			return Direction.OUT;
		else if(direction == Direction.OUT)
			return Direction.IN;
		else
			return null;
	}

	@Override
	public VertexQuery query(){
		throw new UnsupportedOperationException();
	}

	public String toString(){
		return "VertexWritable[" +
			super.toString() + ",outEdges=" + outEdges.size() + " inEdges=" + inEdges.size()
			+ "]";
	}

	/// ======================

	public void setLongId(long id){
		setProperty(LONG_ID_PROPKEY, id);
	}

	public Long getLongId(){
		return getProperty(LONG_ID_PROPKEY);
	}

}
