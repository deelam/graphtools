package net.deelam.graphtools.jobqueue;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.VertexFrame;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;
import com.tinkerpop.frames.modules.typedgraph.TypeField;

@Deprecated
@TypeField(BaseConcept.TYPE_KEY)
public interface BaseConcept extends VertexFrame {

	//***** Constants *********************
	static final String TYPE_KEY="_FRAME_TYPE";

	@Property(TYPE_KEY)
	String getVertexType();

    // property key should match IdGraph.ID
    @JavaHandler
    String getNodeId();
    
	//--------- Operations --------------

	abstract class Impl implements BaseConcept {
		public static String getFrameType(Vertex v){
			String type=v.getProperty(TYPE_KEY);
			return type;
		}
        public String getNodeId(){
          return asVertex().getId().toString();
      }
	}
}
