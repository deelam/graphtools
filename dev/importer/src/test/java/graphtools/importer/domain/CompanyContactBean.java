package graphtools.importer.domain;

import lombok.Data;

@Data
public class CompanyContactBean {
	// Person
	String firstName, lastName;

	// Location
	String company;
	String address, city, county, state;
	Integer zip;
	
	// CommDevices
	String phone1, phone2;
	String email;

}
