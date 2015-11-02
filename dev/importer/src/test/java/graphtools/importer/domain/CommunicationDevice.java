package graphtools.importer.domain;

/**
 * @author dnlam, Created:Apr 30, 2015
 */
public interface CommunicationDevice {
	static final String userIdProp="userId";
	static final String nameProp="name";
	static final String usernameProp="username";
	static final String emailProp="email";
	static final String phoneProp="phone";
	
	static final String REALM_SUFFIX=".realm"; // used to uniquely identify selectors/userIds within some domain
}
