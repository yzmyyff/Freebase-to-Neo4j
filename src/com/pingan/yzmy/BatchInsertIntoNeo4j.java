package com.pingan.yzmy;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * 从文件中批量导入neo4j.
 * Created by Kuang.Ru on 2016/4/11.
 */
public class BatchInsertIntoNeo4j {
  private static int addedNodes = 0;
  private static int addedRelationships = 0;

  /**
   * 获取全部过程的统计数据.
   *
   * @return 统计数据的字符串形式.
   */
  private static String getStatistics() {
    return "addedRelationships = " + addedRelationships + " : " + "addedNodes = " + addedNodes;
  }

  /**
   * 通过点文件将所有内容加入到neo4j.
   *
   * @param nodeFilePath 点文件路径.
   * @param db           neo4j数据库导入器
   */
  private static void addNodes(String nodeFilePath, BatchInserter db) {
    try {
      String line;
      int count = 0;
      int millions = 0;
      InputStreamReader isr = new InputStreamReader(new FileInputStream(nodeFilePath), "UTF-8");
      BufferedReader br = new BufferedReader(isr);
      line = br.readLine();

      while (line != null) {
        count++;

        try {
          String[] attribute = line.split("\t");
          Map<String, Object> props = new HashMap<>();
          long nodeId = Long.parseLong(attribute[0]);

          for (int i = 1; i < attribute.length; ++i) {
            String[] keyAndValue = attribute[i].split(":");

            if (keyAndValue.length == 2) {
              props.put(keyAndValue[0], keyAndValue[1]);
            } else {
              props.put(keyAndValue[0], "");
            }
          }

          db.createNode(nodeId, props, DynamicLabel.label("Entity"));
          ++addedNodes;
        } catch (ArrayIndexOutOfBoundsException ex) {
          System.out.println(count + ":" + line);
        }

        // print every 10M triples
        if (count == 10000000) {
          millions += 10;
          System.out.println("Nodes = " + millions + "M");
          count = 0;
        }

        line = br.readLine();
      }

      br.close();
      isr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 通过关系文件将所有内容加入到neo4j
   *
   * @param relationshipFilePath 关系文件路径.
   * @param db                   neo4j数据库批量导入器.
   */
  private static void addRelationships(String relationshipFilePath, BatchInserter db) {
    try (BufferedReader br = Files.newBufferedReader(Paths.get(relationshipFilePath))) {
      String line = br.readLine();
      int count = 0;
      int millions = 0;

      while (line != null) {
        count++;
        String[] attribute = line.split("\t");
        RelationshipType relType = DynamicRelationshipType.withName(attribute[1].split(":")[1]);
        Map<String, Object> props = new HashMap<>();
        props.put("prefix", "fb");
        long subjectNodeId = Long.parseLong(attribute[0]);
        long objectNodeId = Long.parseLong(attribute[2]);
        db.createRelationship(subjectNodeId, objectNodeId, relType, props);
        ++addedRelationships;

        if (count == 10000000) {
          millions += 10;
          System.out.println("Nodes = " + millions + "M");
          count = 0;
        }

        line = br.readLine();
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  /**
   * 参数是三元组文件的路径.
   *
   * @param args 1. 节点文件
   *             2. 边文件
   *             3. neo4j数据库文件夹路径.
   */
  public static void main(String[] args) {
    String nodeFilePath = args[0];
    String relationshipFilePath = args[1];
    String neo4jDbPath = args[2];
    Map<String, String> config = new HashMap<>();
    config.put("dbms.pagecache.memory", "10g");
    BatchInserter db = null;

    try {
      db = BatchInserters.inserter(new File(neo4jDbPath), config);
      addNodes(nodeFilePath, db);
      addRelationships(relationshipFilePath, db);
      System.out.println(getStatistics());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (db != null) {
        db.shutdown();
      }
    }
  }
}
