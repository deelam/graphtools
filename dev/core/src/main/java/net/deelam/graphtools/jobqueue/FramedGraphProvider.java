package net.deelam.graphtools.jobqueue;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;
import com.tinkerpop.blueprints.util.wrappers.readonly.ReadOnlyGraph;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.FramedTransactionalGraph;
import com.tinkerpop.frames.VertexFrame;
import com.tinkerpop.frames.modules.AbstractModule;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import com.tinkerpop.frames.modules.typedgraph.TypedGraphModuleBuilder;

/**
 * 
 * @author dlam
 */
@Slf4j
public class FramedGraphProvider {

	private final FramedGraphFactory factory;
	
	private Map<String,Class<?>> frameClassesMap=new HashMap<>();

    public FramedGraphProvider(Class<?>[] typedClasses){
      this(null, typedClasses);
    }
    
	public FramedGraphProvider(AbstractModule initializersModule,
		Class<?>[] typedClasses){
		TypedGraphModuleBuilder tgmb=new TypedGraphModuleBuilder();
		for(Class<?> clazz:typedClasses){
			tgmb=tgmb.withClass(clazz);
			registerFrameClass(clazz);
		}

		log.debug("Registered Frame class types: {}", frameClassesMap.keySet());

		if(initializersModule==null){
			factory=new FramedGraphFactory(
				new JavaHandlerModule(), // required to activate @JavaHandler annotations
				tgmb.build() // required to store type info
				);
		} else {
			factory=new FramedGraphFactory(
				new JavaHandlerModule(), // required to activate @JavaHandler annotations
				initializersModule,
				tgmb.build() // required to store type info
				);
		}
	}

//	public boolean containsFrameType(String type){
//		return frameClassesMap.containsKey(type);
//	}
	
//	public Class<? extends VertexFrame> getFrameClassOf(Vertex v){
//		final String vertexType=v.getProperty(VertexFrame.TYPE_KEY);
//		if(vertexType==null){
//			log.error("getVertexType() returned null.  Does Frame class for vertex {} have a @TypeValue annotation?", v);
//		}
//		
//		final Class<? extends VertexFrame> ret=frameClassesMap.get(vertexType);
//		if(ret==null){
//			log.error("Could not find class for "+v+".  Has it been registered in *FrameRegistry?");
//		}
//		return ret;
//	}
	
//	public Class<? extends VertexFrame> getFrameClassOf(VertexFrame v){
//		return getFrameClassOf(v.asVertex());
//	}
	
	private void registerFrameClass(Class<?> clazz){
		try{
			String typeStr=(String) clazz.getField("TYPE_VALUE").get(clazz);
			frameClassesMap.put(typeStr, clazz);
		}catch(IllegalArgumentException e){
			e.printStackTrace();
		}catch(SecurityException e){
			e.printStackTrace();
		}catch(IllegalAccessException e){
			e.printStackTrace();
		}catch(NoSuchFieldException e){
			e.printStackTrace();
		}
	}

	/**
	 * Using the other get() methods that return a FramedTransactionalGraph is preferred.
	 */
	public <T extends Graph> FramedGraph<T> get(T graph){
		Preconditions.checkNotNull(graph);
		// acceptable types are those that do not require committing graph operations
		if(graph instanceof ReadOnlyGraph){ //acceptable since no graph operations will be performed
			// return a non-transactional graph 
		}else if(isGraphType(TinkerGraph.class, graph)){
			// don't worry about returning a TransactionFramedGraph
		}else if(isGraphType(TransactionalGraph.class, graph)){
			throw new IllegalArgumentException("Graph is transactional but unhandled: " + graph);
		}else{
			log.warn("Should add {} to the list of acceptable graph types.", graph.getClass().getSimpleName());
		}

		FramedGraph<T> framedGraph=factory.create(graph); // wrap the base graph
		return framedGraph;
	}

	//	public FramedTransactionalGraph<TransactionalPartitionGraph> get(PartitionGraph<? extends TransactionalGraph> graph){
	//		// Example: Given a partitiongraph[eventtransactionalindexablegraph[idgraph[...]]]
	//		FramedTransactionalGraph<TransactionalPartitionGraph> framedGraph=factory.create(
	//			new TransactionalPartitionGraph(graph));
	//		return framedGraph;
	//	}

	public FramedTransactionalGraph<TransactionalGraph> get(TransactionalGraph graph){
		FramedTransactionalGraph<TransactionalGraph> framedGraph=factory.create(graph); // wrap the base graph
		return framedGraph;
	}

	public static boolean isGraphType(Class<?> clazz, Graph g){
		if(clazz.isInstance(g))
			return true;

		while(g instanceof WrapperGraph){
			g= ((WrapperGraph<?>) g).getBaseGraph();
			if(clazz.isInstance(g))
				return true;
		}
		return false;
	}

}
