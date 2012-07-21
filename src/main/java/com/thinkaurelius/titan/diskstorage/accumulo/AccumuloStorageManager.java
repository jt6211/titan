package com.thinkaurelius.titan.diskstorage.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.GraphDatabaseException;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.util.ConfigHelper;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyColumnValueIDManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Experimental storage manager for Accumulo.
 * 
 * This is not ready for production.
 * 
 * @author Jason Trost <jtrost@apache.org>
 */
public class AccumuloStorageManager implements StorageManager {

	private static final String AUTHORIZATIONS = "authorizations";

	private static final String USERNAME = "username";

	private static final String PASSWORD = "password";

	private static final String ZOOKEEPERS = "zookeepers";

	private static final String INSTANCE_NAME = "instanceName";

	private static final Logger log = LoggerFactory.getLogger(AccumuloStorageManager.class);
	
    static final String TABLE_NAME_KEY = "tablename";
    static final String TABLE_NAME_DEFAULT = "titan";
    
    public static final String LOCAL_LOCK_MEDIATOR_PREFIX_DEFAULT = "accumulo";

	private final String tableName;
	private final String username;
	private final String password;
	private final String zookeepers;
	private final String instanceName;
	private String auths;
	
    private final OrderedKeyColumnValueIDManager idmanager;
    private final int lockRetryCount;
    
    private final long lockWaitMS, lockExpireMS;
    
    private final byte[] rid;
    
    private final String llmPrefix;
    
    public AccumuloStorageManager(org.apache.commons.configuration.Configuration config) {
    	this.rid = ConfigHelper.getRid(config);
    	
        this.tableName = config.getString(TABLE_NAME_KEY, TABLE_NAME_DEFAULT);
        this.username = config.getString(USERNAME);
        this.password = config.getString(PASSWORD);
        this.zookeepers = config.getString(ZOOKEEPERS);
		this.instanceName = config.getString(INSTANCE_NAME);
		this.auths = config.getString(AUTHORIZATIONS);
		
        this.llmPrefix =
				config.getString(
						LOCAL_LOCK_MEDIATOR_PREFIX_KEY,
						LOCAL_LOCK_MEDIATOR_PREFIX_DEFAULT);
        
		this.lockRetryCount =
				config.getInt(
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT);
		
		this.lockWaitMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_WAIT_MS,
						GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT);
		
		this.lockExpireMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT);
		
        idmanager = new OrderedKeyColumnValueIDManager(
        		openDatabase("blocks_allocated", null, null), rid, config);
    }


    @Override
    public long[] getIDBlock(int partition) {
        return idmanager.getIDBlock(partition);
    }

	@Override
	public OrderedKeyColumnValueStore openDatabase(String name)
			throws GraphStorageException {
		
		OrderedKeyColumnValueStore lockStore =
				openDatabase(name + "_locks", null, null);
		LocalLockMediator llm = LocalLockMediators.INSTANCE.get(llmPrefix + ":" + name);
		OrderedKeyColumnValueStore dataStore =
				openDatabase(name, llm, lockStore);
		
		return dataStore;
	}
	
	private OrderedKeyColumnValueStore openDatabase(String name, LocalLockMediator llm, OrderedKeyColumnValueStore lockStore)
			throws GraphStorageException {

		Instance inst;
		Connector conn;
		
		try {
			inst = new ZooKeeperInstance(instanceName, zookeepers);
			conn = inst.getConnector(username, password);
		} catch (Exception e) {
			throw new GraphDatabaseException(e);
		}
		
		try {
			if(!conn.tableOperations().exists(tableName))
			{
				conn.tableOperations().create(tableName);
			}
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
		
		Authorizations authorizations;
		try {
			if(auths == null){
				authorizations = conn.securityOperations().getUserAuthorizations(username);
			} else {
				authorizations = new Authorizations(auths.split(","));
			}
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
		
		return new AccumuloOrderedKeyColumnValueStore(conn, tableName, authorizations, name, lockStore,
				llm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
	}

	@Override
	public TransactionHandle beginTransaction() {
		return new AccumuloTransaction();
	}

	@Override
	public void close() {
		//Nothing to do
		
	}



}
