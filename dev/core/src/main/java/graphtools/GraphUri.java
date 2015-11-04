/**
 * 
 */
package graphtools;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph.FileType;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * GraphUri examples:
 * <li> "tinker:///": in-memory TinkerGraph 
 * <li> "tinker:///./testGraphs/tGraph": on-disk TinkerGraph in relative directory
 * <li> "tinker:///./testGraphs/tGraphML?fileType=graphml": on-disk TinkerGraph in GraphML format
 * @author deelam
 */
@RequiredArgsConstructor
@Slf4j
public class GraphUri {
	private static final String URI_PATH = "_uriPath";
	private static final String URI_SCHEMA_PART = "_uriSchemaSpecificPart";
	
	@Getter
	private final URI uri;
	public GraphUri(String uri) {
		this(uri, new BaseConfiguration());
	}
	public GraphUri(String uri, Configuration config) {
		this.uri=URI.create(uri);
		conf=config;
	}
	@Getter
	private Configuration conf;
	public IdGraph openIdGraph() {
		IdGraphFactory factory = graphFtry.get(uri.getScheme());
		Preconditions.checkNotNull(factory, "Unknown schema: "+uri.getScheme());
		if(conf==null)
			conf=new BaseConfiguration();
		conf.setProperty(URI_PATH, uri.getPath());
		conf.setProperty(URI_SCHEMA_PART, uri.getSchemeSpecificPart());
		parseQuery(uri, conf);
		IdGraph graph = factory.open(conf);
		log.info("Opened graph="+graph);
		return graph;
	}
	
	private static Map<String,IdGraphFactory> graphFtry=new HashMap<>();
	public static void register(String scheme, IdGraphFactory factory){
		graphFtry.put(scheme, factory);
	}
	
	static{
		register("tinker", new IdGraphFactory(){
			@Override
			public IdGraph open(Configuration conf) {
				// check desired output format
				FileType fileType = TinkerGraph.FileType.JAVA;
				String fileTypeStr = conf.getString("fileType");
				if(fileTypeStr!=null){
					fileType=TinkerGraph.FileType.valueOf(fileTypeStr.toUpperCase());
				}
				
				String path = conf.getString(URI_PATH);
				// check if path is relative
				if(path.length()>1 && path.charAt(1)=='.')
					path=path.substring(1);
				
				// open graph
				IdGraph<?> graph;
				if(path==null || path.equals("/")){
					log.debug("Opening tinker graph in memory");
					graph=new IdGraph<>(new TinkerGraph());
				}else{
					log.debug("Opening tinker graph at path="+path);
					graph=new IdGraph<>(new TinkerGraph(path, fileType));
				}
				return graph;
			}
		});
	}
	
	private void parseQuery(URI uri, Configuration conf){
		String queryStr = uri.getQuery();
		if(queryStr!=null){
			for(String kv:queryStr.split("&")){
				String[] pair=kv.split("=");
				conf.setProperty(pair[0],pair[1]);
			}
		}
	}
}
