/**
 * 
 */
package graphtools;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

import org.apache.commons.configuration.Configuration;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author dlam
 *
 */
public class GraphUri {
	@Getter
	private final URI uri;
	public GraphUri(String uri) {
		this.uri=URI.create(uri);
	}
	public IdGraph openIdGraph(Configuration conf) {
		IdGraphFactory factory = graphFtry.get(uri.getScheme());
		Preconditions.checkNotNull(factory, "Unknown schema: "+uri.getScheme());
		return factory.open(conf);
	}
	
	private static Map<String,IdGraphFactory> graphFtry=new HashMap<>();
	public static void register(String scheme, IdGraphFactory factory){
		graphFtry.put(scheme, factory);
	}
	static{
		register("tinker", new IdGraphFactory(){
			@Override
			public IdGraph open(Configuration conf) {
				return new IdGraph(new TinkerGraph());
			}
		});
	}
}
