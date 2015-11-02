package graphtools.importer.domain;

import com.tinkerpop.blueprints.Vertex;

class Location {
	static final String ENTITY_TYPE="LOCATION";
	final Vertex node;

	public Location(Vertex node) {
		this.node = node;
		node.setProperty("locationType", "");
	}

	Location name(String val){
		if(val!=null) node.setProperty("name", val);
		return this;
	}

	Location address(String val){
		if(val!=null) node.setProperty("address", val);
		return this;
	}
	Location city(String val){
		if(val!=null) node.setProperty("city", val);
		return this;
	}
	Location county(String val){
		if(val!=null) node.setProperty("county", val);
		return this;
	}
	Location state(String val){
		if(val!=null) node.setProperty("state", val);
		return this;
	}
	Location zip(Integer val){
		if(val!=null) node.setProperty("zip", val);
		return this;
	}
}
