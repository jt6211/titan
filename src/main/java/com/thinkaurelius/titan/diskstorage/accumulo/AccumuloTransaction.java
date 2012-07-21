package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.locking.LockingTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link LockingTransaction}; however, it creates a transaction type specific
 * to Accumulo, which lets us check for user errors like passing a Cassandra
 * transaction into an Accumulo method.
 * 
 * @author Jason Trost <jtrost@apache.org>
 */
public class AccumuloTransaction extends LockingTransaction {

	public AccumuloTransaction() {
		super();
	}
	
}
