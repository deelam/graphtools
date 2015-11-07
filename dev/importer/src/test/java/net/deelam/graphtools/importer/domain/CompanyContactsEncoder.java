package net.deelam.graphtools.importer.domain;

import java.util.ArrayList;
import java.util.List;

import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.importer.EdgeFiller;
import net.deelam.graphtools.importer.Encoder;
import net.deelam.graphtools.importer.EntityRelation;
import net.deelam.graphtools.importer.NodeFiller;
import net.deelam.graphtools.importer.RecordContext;
import net.deelam.graphtools.importer.SourceData;
import net.deelam.graphtools.importer.domain.CompanyContactsEncoder.CompanyContactsRC.ContactDevice;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class CompanyContactsEncoder implements Encoder<CompanyContactBean>{

	@Override
	public void reinit(SourceData<CompanyContactBean> sourceData) {
	}

	@Override
	public void close(SourceData<CompanyContactBean> sourceData) {
	}

	static class CompanyContactsRC implements RecordContext<CompanyContactBean> {
		@Getter
		@Setter
		CompanyContactBean bean;
		@Getter
		@Setter
		int instanceIndex;
		
		@AllArgsConstructor
		static class ContactDevice{
			CommDevice.DEVICE_TYPES type;
			String devId;
		}
		
		List<ContactDevice> devices=new ArrayList<>(3);
		
		CompanyContactsRC reinit(){
			devices.clear();
			if(bean.phone1!=null){
				devices.add(new ContactDevice(CommDevice.DEVICE_TYPES.PHONE, bean.phone1));
			}
			if(bean.phone2!=null){
				devices.add(new ContactDevice(CommDevice.DEVICE_TYPES.PHONE, bean.phone2));
			}
			if(bean.email!=null){
				devices.add(new ContactDevice(CommDevice.DEVICE_TYPES.EMAIL, bean.email));
			}
			return this;
		}
	}
	
	private CompanyContactsRC rc=new CompanyContactsRC();
	@Override
	public RecordContext<CompanyContactBean> createContext(CompanyContactBean bean) {
		rc.setBean(bean);
		return rc.reinit(); // reuse instance
	}

	@Override
	public int getEntityRelationCount(RecordContext<CompanyContactBean> context) {
		return graphFillers.size();
	}

	@Override
	public EntityRelation<CompanyContactsRC> getEntityRelation(int i) {
		return graphFillers.get(i);
	}
	
	@SuppressWarnings("unchecked")
	List<EntityRelation<CompanyContactsRC>> graphFillers=Lists.newArrayList(
	
		/// Person -> company Location
		new EntityRelation<CompanyContactsRC>(
			new PersonNodeFiller(),	new LocationNodeFiller(),
			new EmployeeAtEdgeFiller()),

		/// company Location -> USState
		new EntityRelation<CompanyContactsRC>(
			new LocationNodeFiller(), new StateNodeFiller(),
			new InStateEdgeFiller()),
			
		/// Person -> CommDevice
		new EntityRelation<CompanyContactsRC>(
			new PersonNodeFiller(),	new DeviceNodeFiller(),
			new HasDeviceEdgeFiller()
			){
			@Override
			protected int numInstances(CompanyContactsRC context){
				return context.devices.size();
			}
		}

	);
	
	
	
	static final class PersonNodeFiller extends NodeFiller<CompanyContactsRC> {
		public PersonNodeFiller() {
			super(Person.ENTITY_TYPE);
		}

		@Override
		public String getId(CompanyContactsRC context) {
			return "Person:" + context.bean.firstName+"_"+context.bean.lastName;
		}

		@Override
		public void fill(Vertex v, CompanyContactsRC context) {
			CompanyContactBean b = context.bean;
			new Person(v).firstName(b.firstName).lastName(b.lastName);
		}
	}

	static final class LocationNodeFiller extends NodeFiller<CompanyContactsRC> {
		public LocationNodeFiller(){
			super(Location.ENTITY_TYPE);
		}
		
		@Override
		public String getId(CompanyContactsRC context){
			return "Loc:" + context.bean.address + "," + context.bean.zip;
		}

		@Override
		public void fill(Vertex v, CompanyContactsRC context){
			CompanyContactBean b = context.bean;
			new Location(v, Location.LOCATION_TYPES.Company)
				.address(b.address).city(b.city)
				.county(b.county).state(b.state).zip(b.zip);
		}
	}

	static final class EmployeeAtEdgeFiller extends EdgeFiller<CompanyContactsRC> {
		public EmployeeAtEdgeFiller() {
			super("employeeAt");
		}

		@Override
		public String getId(GraphRecord outFv,	GraphRecord inFv, CompanyContactsRC context){
			return outFv.getId() + ":location>" + context.bean.address; 
		}
	}

	static final class StateNodeFiller extends NodeFiller<CompanyContactsRC> {
		public StateNodeFiller(){
			super(Location.ENTITY_TYPE);
		}
		
		@Override
		public String getId(CompanyContactsRC context){
			return "State:" + context.bean.state;
		}

		@Override
		public void fill(Vertex v, CompanyContactsRC context){
			CompanyContactBean b = context.bean;
			new Location(v, Location.LOCATION_TYPES.USState)
				.state(b.state);
		}
	}
	
	static final class InStateEdgeFiller extends EdgeFiller<CompanyContactsRC> {
		public InStateEdgeFiller() {
			super("inState");
		}

		@Override
		public String getId(GraphRecord outFv,	GraphRecord inFv, CompanyContactsRC context){
			return outFv.getId() + ":inState>" + context.bean.state; 
		}
	}
	
	/// CommDevice entity

	@Slf4j
	static final class DeviceNodeFiller extends NodeFiller<CompanyContactsRC> {
		public DeviceNodeFiller(){
			super(CommDevice.ENTITY_TYPE);
		}

		@Override
		public String getId(CompanyContactsRC context){
			int i=context.instanceIndex;
			return context.devices.get(i).type+":"+context.devices.get(i).devId;
		}

		@Override
		public void fill(Vertex v, CompanyContactsRC c){
			ContactDevice dev = c.devices.get(c.instanceIndex);
			new CommDevice(v).deviceType(dev.type).identifier(dev.devId);
		}
	}

	/// Person has CommDevice
	static final class HasDeviceEdgeFiller extends EdgeFiller<CompanyContactsRC> {
		public HasDeviceEdgeFiller(){
			super("hasDevice");
		}

		@Override
		public String getId(GraphRecord outFv,	GraphRecord inFv, CompanyContactsRC context){
			return outFv.getId() + "->" + inFv.getId();
		}

		@Override
		public void fill(Edge e, CompanyContactsRC context){
			e.setProperty("index", context.instanceIndex);
		}
	}
}
