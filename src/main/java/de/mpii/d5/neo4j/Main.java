package main.java.de.mpii.d5.neo4j;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Main {
  /**
   * main method
   *
   * @param args arg1 is the path to Freebase dump, arg2 is the path to store noe4j DB,
   *             arg3 is the number of triples to read (optional)
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: 缺少参数: Freebase三元组文件路径.");
      return;
    }

    String freebasePath = args[0];
    String databaseDir = args[1];
    int numberOfTriples = -1;

    if (args.length == 3) {
      numberOfTriples = Integer.parseInt(args[2]);
    }

    BatchInserter db = null;

    // set these configuration based on the size of your data
    Map<String, String> config = new HashMap<>();

    // neo4j的页面缓存. 需要和本机条件相关.
    // bytes_needed = number_of_nodes * 15
    //                + number_of_relationships * 34
    //                + number_of_properties * 64
    // 按照估计公式 bytes_ needed = 46000000  * 15 + 595779207 * 34 + 46000000 * 64 = 23890493038 byte
    // ≈ 22.25 GiB
    config.put("dbms.pagecache.memory", "10g");

    try {
      db = BatchInserters.inserter(new File(databaseDir), config);
      Neo4jBatchHandler handler = new Neo4jBatchHandler(db);
      handler.createNeo4jDb(freebasePath, numberOfTriples);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (db != null) {
        db.shutdown();
      }
    }
  }
}
