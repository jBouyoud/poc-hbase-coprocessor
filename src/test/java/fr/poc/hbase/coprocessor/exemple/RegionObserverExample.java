package fr.poc.hbase.coprocessor.exemple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Example region observer checking for special get requests
 */
public class RegionObserverExample extends BaseRegionObserver {

	public static final Log LOG = LogFactory.getLog(HRegion.class);
	public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
		LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));

		// Check if the request row key matches a well known one.
		if (Bytes.equals(get.getRow(), FIXED_ROW)) {
			Put put = new Put(get.getRow());
			//  Create cell indirectly using a Put instance.
			put.addColumn(FIXED_ROW, FIXED_ROW,
					Bytes.toBytes(System.currentTimeMillis()));
			CellScanner scanner = put.cellScanner();
			scanner.advance();
			// Get first cell from Put using the CellScanner instance.
			Cell cell = scanner.current();
			LOG.debug("Had a match, adding fake cell: " + cell);
			// Create a special KeyValue instance containing just the current time on the server.
			results.add(cell);
		}
	}
}
