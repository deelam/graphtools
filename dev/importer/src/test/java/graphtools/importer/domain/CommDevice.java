package graphtools.importer.domain;

import lombok.RequiredArgsConstructor;

import com.tinkerpop.blueprints.Vertex;

@RequiredArgsConstructor
class CommDevice {
	static final String ENTITY_TYPE="DEVICE";
	final Vertex node;

	enum DEVICE_TYPES { PHONE, EMAIL };
	CommDevice deviceType(DEVICE_TYPES val){
		node.setProperty("deviceType", val);
		return this;
	}

	CommDevice identifier(String val){
		node.setProperty("identifier", val);
		return this;
	}
}
