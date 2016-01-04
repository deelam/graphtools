package net.deelam;

import lombok.Data;

@Data
public class ClassDepBean {
	// Jar
	String srcJar, dstJar;
	
	// fully-qualified class name
	String srcClass, dstClass;
}
