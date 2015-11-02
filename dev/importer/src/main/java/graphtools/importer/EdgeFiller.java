package graphtools.importer;

import graphtools.GraphRecord;
import lombok.AllArgsConstructor;
import lombok.Getter;

import com.tinkerpop.blueprints.Edge;

/**
 * @author dnlam, Created:Jan 27, 2015
 */
@AllArgsConstructor
public abstract class EdgeFiller<C extends RecordContext<?>> {
	@Getter
	protected String label;
	
	public EdgeFiller<C> label(String edgeLabel){
		label=edgeLabel;
		return this;
	}

	abstract public String getId(GraphRecord outFv, GraphRecord inFv, C context);

	public void fill(Edge e, C context) {
	}
}
