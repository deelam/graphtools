package net.deelam.graphtools.importer.domain;

import lombok.RequiredArgsConstructor;

import com.tinkerpop.blueprints.Vertex;

@RequiredArgsConstructor
class Person {
	static final String ENTITY_TYPE="PERSON";
	public static final String nameProp = null;
	
	final Vertex node;

	Person firstName(String fn){
		if(fn!=null) node.setProperty("firstName", fn);
		return this;
	}

	Person lastName(String ln){
		if(ln!=null) node.setProperty("lastName", ln);
		return this;
	}
}
