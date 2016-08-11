package net.deelam.graphtools;

import java.net.URI;

public class GraphUriParser {

	public static void main(String[] args) throws Exception {
		URI uri = new URI("neo4j://host2:32/./path/to/dir");
		System.out.println(uri.getHost() + " " + uri.getPort() + " "
				+ uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("neo4j:path2/to/dir");
		System.out.println(uri.getHost() + " " + uri.getPort() + " "
				+ uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("neo4j:/path2/to/dir");
		System.out.println(uri.getHost() + " " + uri.getPort() + " "
				+ uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("titan:///tablename");
		System.out.println(uri.getHost() + " " + uri.getPort() + " "
				+ uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("file:///absolute");
		System.out.println(uri.getHost() + " " + uri.getPort() + " "
				+ uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("file://./relative");
		System.out.println(uri.getHost() + " " + uri.getPort() + " "
				+ uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("neo4j", "path/to/dir", null);
		System.out.println(uri + " == " + uri.getHost() + " " + uri.getPort()
				+ " " + uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("neo4j", "/path/to/dir", null);
		System.out.println(uri + " == " + uri.getHost() + " " + uri.getPort()
				+ " " + uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("neo4j", "host", "./path/to/dir", null);
		System.out.println(uri + " == " + uri.getHost() + " " + uri.getPort()
				+ " " + uri.getPath() + " " + uri.getSchemeSpecificPart());
		uri = new URI("neo4j", "host", "/path/to/dir", null);
		System.out.println(uri + " == " + uri.getHost() + " " + uri.getPort()
				+ " " + uri.getPath() + " " + uri.getSchemeSpecificPart());
	}

	public String parseNeoPath(URI uri) {
		return uri.getSchemeSpecificPart();
	}
}
