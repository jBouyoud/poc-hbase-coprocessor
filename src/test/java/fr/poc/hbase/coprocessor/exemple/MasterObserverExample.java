package fr.poc.hbase.coprocessor.exemple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

/**
 * Example master observer that creates a separate directory on the file system when a table is created.
 */
public class MasterObserverExample extends BaseMasterObserver {

	public static final Log LOG = LogFactory.getLog(HRegion.class);

	@Override
	public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		LOG.debug("Got postCreateTable callback");
		// Get the new table's name from the table descriptor.
		TableName tableName = desc.getTableName();

		LOG.debug("Created table: " + tableName + ", region count: " + regions.length);
		MasterServices services = ctx.getEnvironment().getMasterServices();
		// Get the available services and retrieve a reference to the actual file system.
		MasterFileSystem masterFileSystem = services.getMasterFileSystem();
		FileSystem fileSystem = masterFileSystem.getFileSystem();

		// Create a new directory that will store binary data from the client application.
		Path blobPath = new Path(tableName.getQualifierAsString() + "-blobs");
		fileSystem.mkdirs(blobPath);

		LOG.debug("Created " + blobPath + ": " + fileSystem.exists(blobPath));
	}
}
