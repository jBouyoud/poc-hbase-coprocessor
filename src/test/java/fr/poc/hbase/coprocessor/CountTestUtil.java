package fr.poc.hbase.coprocessor;

import fr.poc.hbase.HBaseHelper;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by JBD on 23/02/2017.
 */
@UtilityClass
public class CountTestUtil {

	public static final long ROW_COUNT = 25_000;
	public static final int COL_COUNT_PER_FAMILIES = 5;
	public static final String[] FAMILIES = new String[]{
			"colfam0", "colfam1", "colfam2", "colfam3"
	};

	public static void buildCountTestTable(@NonNull HBaseHelper helper,
												 @NonNull String tableName,
												 @NonNull Consumer<HTableDescriptor> hTableDescriptorConfigurer) throws Exception {
		helper.dropTable(tableName);
		helper.createTable(tableName, new byte[][]{
				Bytes.toBytes("row-8333"), Bytes.toBytes("row-16666")
		}, FAMILIES);
		helper.alterTable(tableName, hTableDescriptorConfigurer);
		helper.fillTable(tableName, 1, (int) ROW_COUNT, COL_COUNT_PER_FAMILIES, FAMILIES);

		Admin admin = helper.getConnection().getAdmin();
		// wait for the split to be done
		while (admin.getTableRegions(TableName.valueOf(tableName)).size() < 3) {
			Thread.sleep(1000);
		}
	}
}
