package graphtools.importer;

import lombok.RequiredArgsConstructor;

/**
 * Template for modeling an entity relationship.
 * @author deelam
 */
@RequiredArgsConstructor
public class EntityRelation<C extends RecordContext<?>>{	
	final NodeFiller<C> srcNodeFiller;
	final NodeFiller<C> dstNodeFiller;
	final EdgeFiller<C> edgeFiller;

	protected int numInstances(C context){
		return 1;
	}

}	