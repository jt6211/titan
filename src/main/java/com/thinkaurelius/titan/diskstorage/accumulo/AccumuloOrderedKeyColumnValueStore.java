package com.thinkaurelius.titan.diskstorage.accumulo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.SimpleLockConfig;
import com.thinkaurelius.titan.diskstorage.writeaggregation.MultiWriteKeyColumnValueStore;

/**
 * Experimental Accumulo store.
 * 
 * @author Jason Trost <jtrost@apache.org>
 */
public class AccumuloOrderedKeyColumnValueStore implements
		OrderedKeyColumnValueStore, MultiWriteKeyColumnValueStore {
	
	private static final Logger log = LoggerFactory.getLogger(AccumuloOrderedKeyColumnValueStore.class);
	
	private final Connector conn;
	private Scanner scanner;
	private BatchWriter batchWriter;
	Authorizations authorizations;
	
	private final String tableName;
	private final LockConfig internals;
	
	// This is cf.getBytes()
	private Text cf;
	
	AccumuloOrderedKeyColumnValueStore(Connector conn, String tableName, Authorizations authorizations,
			String columnFamily, OrderedKeyColumnValueStore lockStore,
			LocalLockMediator llm, byte[] rid, int lockRetryCount,
			long lockWaitMS, long lockExpireMS) {

		this.conn = conn;
		this.tableName = tableName;
		this.authorizations = authorizations;
		this.cf = new Text(columnFamily.getBytes());
		
		if (null != llm && null != lockStore) {
			this.internals = new SimpleLockConfig(this, lockStore, llm,
					rid, lockRetryCount, lockWaitMS, lockExpireMS);
		} else {
			this.internals = null;
		}
		
		try {
			scanner = conn.createScanner(tableName, authorizations);
			batchWriter = conn.createBatchWriter(tableName, 50*1024*1024, 10000L, 10);
		} catch (TableNotFoundException e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public void close() throws GraphStorageException {
		try {
			
			
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column, TransactionHandle txh) {
		
		byte[] keyBytes = toArray(key);
		byte[] colBytes = toArray(column);
		
		Iterator<java.util.Map.Entry<Key, Value>> iter;
		synchronized (scanner) {
			scanner.clearColumns();
			scanner.setRange(new Range(new Text(keyBytes)));
			scanner.fetchColumn(cf, new Text(colBytes));
			scanner.setBatchSize(10);
			iter = scanner.iterator();
		}
		
		try {
			if(iter.hasNext())
			{
				java.util.Map.Entry<Key, Value> entry = iter.next();
				
				if(iter.hasNext())
				{
					// This should not happen
					log.error("Found more than 1 results for key {}, column {}, family {} (expected 0 or 1 results)", 
							new Object[] { new String(Hex.encodeHex(keyBytes)),
								           new String(Hex.encodeHex(colBytes)),
								           new String(Hex.encodeHex(cf.getBytes())) });
					
					return null;
				}
				
				return ByteBuffer.wrap(entry.getValue().get());
			}
			return null;
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		return null != get(key, column, txh);
	}

	@Override
	public boolean isLocalKey(ByteBuffer key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		
		byte[] keyBytes = toArray(key);
		synchronized (scanner) {
			scanner.clearColumns();
			scanner.setRange(new Range(new Text(keyBytes)));
			scanner.fetchColumnFamily(cf);
			scanner.setBatchSize(1);
			return scanner.iterator().hasNext();
		}
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, int limit, TransactionHandle txh) {

		byte[] colStartBytes = columnStart.hasRemaining() ? toArray(columnStart) : null;
		byte[] colEndBytes   = columnEnd.hasRemaining() ? toArray(columnEnd) : null;
		
		// Here, I'm falling back to retrieving the whole row and
		// cutting it down to the limit on the client.  This is obviously
		// not going to scale.  The long-term solution is probably to
		// reimplement ColumnCountGetFilter in such a way that it won't
		// drop the final row.
		List<Entry> ents = getHelper(key, colStartBytes, true, colEndBytes, false);
		if (ents.size() <= limit)
			return ents;
		
		List<Entry> result = new ArrayList<Entry>(limit);
		for (int i = 0; i < limit; i++) {
			result.add(ents.get(i));
		}
		
		return result;
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart,
			ByteBuffer columnEnd, TransactionHandle txh) {

		byte[] colStartBytes = columnStart.hasRemaining() ? toArray(columnStart) : null;
		byte[] colEndBytes   = columnEnd.hasRemaining() ? toArray(columnEnd) : null;
		return getHelper(key, colStartBytes, true, colEndBytes, false);
	}

	private List<Entry> getHelper(ByteBuffer key, byte[] colStartBytes, boolean colStartInclusive, byte[] colEndBytes, boolean colEndInclusive) {
		
		Text k = new Text(toArray(key));
		Text cq1 = colStartBytes == null?null:new Text(colStartBytes);
		Text cq2 = colEndBytes   == null?null:new Text(colEndBytes);

		Iterator<java.util.Map.Entry<Key, Value>> iter;
		synchronized (scanner) {
			scanner.clearColumns();
			scanner.setRange(new Range(new Key(k, cf, cq1), colStartInclusive, new Key(k, cf, cq2), colEndInclusive));
			scanner.setBatchSize(1000);
			iter = scanner.iterator();
		}
		
		try {
			List<Entry> ret = new LinkedList<Entry>();
			while(iter.hasNext()) {
				Map.Entry<Key,Value> ent = (Map.Entry<Key,Value>) iter.next();
				ret.add(new Entry(ByteBuffer.wrap(ent.getKey().getColumnQualifier().getBytes()), ByteBuffer.wrap(ent.getValue().get())));
			}

			return ret;
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
	}
	
	/*
	 * This method exists because HBase's API generally deals in
	 * whole byte[] arrays.  That is, data are always assumed to
	 * begin at index zero and run through the native length of
	 * the array.  These assumptions are reflected, for example,
	 * in the classes hbase.client.Get and hbase.client.Scan.
	 * These assumptions about arrays are not generally true when
	 * dealing with ByteBuffers.
	 * <p>
	 * This method first checks whether the array backing the
	 * ByteBuffer argument indeed satisfies the assumptions described
	 * above.  If so, then this method returns the backing array.
	 * In other words, this case returns {@code b.array()}.
	 * <p>
	 * If the ByteBuffer argument does not satisfy the array
	 * assumptions described above, then a new native array of length
	 * {@code b.limit()} is created.  The ByteBuffer's contents
	 * are copied into the new native array without modifying the
	 * state of {@code b} (using {@code b.duplicate()}).  The new
	 * native array is then returned.
	 *  
	 */
	private static byte[] toArray(ByteBuffer b) {
		if (0 == b.arrayOffset() && b.limit() == b.array().length)
			return b.array();
		
		byte[] result = new byte[b.limit()];
		b.duplicate().get(result);
		return result;
	}

	@Override
	public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, TransactionHandle txh) {
		
    	// null txh means a LockingTransaction is calling this method
    	if (null != txh) {
    		// non-null txh -> make sure locks are valid
    		AccumuloTransaction lt = (AccumuloTransaction)txh;
    		if (! lt.isMutationStarted()) {
    			// This is the first mutate call in the transaction
    			lt.mutationStarted();
    			// Verify all blind lock claims now
    			lt.verifyAllLockClaims(); // throws GSE and unlocks everything on any lock failure
    		}
    	}
		
		byte[] keyBytes = toArray(key);
		Text k = new Text(keyBytes);
		List<Mutation> muts = new LinkedList<Mutation>();
				
		// Deletes
		if (null != deletions && 0 != deletions.size()) {
			Mutation d = new Mutation(k);
			for (ByteBuffer del : deletions) {
				d.putDelete(cf, new Text(toArray(del.duplicate())));
			}
			muts.add(d);
		}
		
		// Inserts
		if (null != additions && 0 != additions.size()) {
			Mutation p = new Mutation(k);
			for (Entry e : additions) {
				byte[] colBytes = toArray(e.getColumn().duplicate());
				byte[] valBytes = toArray(e.getValue().duplicate());
				p.put(cf, new Text(colBytes), new Value(valBytes));
			}
			muts.add(p);
		}
		
		try {
			if(!muts.isEmpty())
			{
				batchWriter.addMutations(muts);
				batchWriter.flush();
			}
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public void mutateMany(
			Map<ByteBuffer, com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation> mutations,
			TransactionHandle txh) {
		
    	// null txh means a LockingTransaction is calling this method
    	if (null != txh) {
    		// non-null txh -> make sure locks are valid
    		AccumuloTransaction lt = (AccumuloTransaction)txh;
    		if (! lt.isMutationStarted()) {
    			// This is the first mutate call in the transaction
    			lt.mutationStarted();
    			// Verify all blind lock claims now
    			lt.verifyAllLockClaims(); // throws GSE and unlocks everything on any lock failure
    		}
    	}
		
		Map<byte[], Mutation> puts = new HashMap<byte[], Mutation>();
		Map<byte[], Mutation> dels = new HashMap<byte[], Mutation>();

		final long delTS = System.currentTimeMillis();
		final long putTS = delTS + 1;
		
		for (ByteBuffer keyBB : mutations.keySet()) {
			byte[] keyBytes = toArray(keyBB);
			
			com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation m = mutations.get(keyBB);
			if (m.hasDeletions()) {				
				Mutation d = dels.get(keyBytes);
				if (null == d) {
					d = new Mutation(new Text(keyBytes));
					dels.put(keyBytes, d);
				}
				
				for (ByteBuffer b : m.getDeletions()) {
					d.putDelete(cf, new Text(toArray(b)), delTS);
				}
			}
			
			if (m.hasAdditions()) {
				Mutation p = puts.get(keyBytes);
				
				if (null == p) {
					p = new Mutation(new Text(keyBytes));
					puts.put(keyBytes, p);
				}
				
				for (Entry e : m.getAdditions()) {
					p.put(cf, new Text(toArray(e.getColumn())), putTS, new Value(toArray(e.getValue())));
				}
			}
		}
				
		try {
			if(!dels.isEmpty())
				batchWriter.addMutations(dels.values());
			if(!puts.isEmpty())
				batchWriter.addMutations(puts.values());
			if(!puts.isEmpty() || !dels.isEmpty())
				batchWriter.flush();
			
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
		
		long now = System.currentTimeMillis(); 
		while (now <= putTS) {
			try {
				Thread.sleep(1L);
				now = System.currentTimeMillis();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column,
			ByteBuffer expectedValue, TransactionHandle txh) {
		AccumuloTransaction lt = (AccumuloTransaction)txh;
		if (lt.isMutationStarted()) {
			throw new GraphStorageException("Attempted to obtain a lock after one or more mutations");
		}
		
		lt.writeBlindLockClaim(internals, key, column, expectedValue);
	}

	public static void main(String[] args) {
		
	}
}
