package alluxio.client.file.dora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.wire.WorkerNetAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JumpConsistentHashPolicyTest {
  private static final int NUM_WORKERS = 10;
  private static final int NUM_VIRTUAL_NODES = 10000;
  private static final int NUM_FILES = 100000;
  private List<BlockWorkerInfo> mBlockWorkerInfos = new ArrayList<>();
  private List<String> mFileIdList = new ArrayList<>();
  @Before
  public void setUp() {
    for(int i = 0; i < NUM_WORKERS; i++) {
      WorkerNetAddress workerAddr = new WorkerNetAddress()
          .setHost("master" + i).setRpcPort(29998).setDataPort(29999).setWebPort(30000);
      mBlockWorkerInfos.add(new BlockWorkerInfo(workerAddr, 1024, 0));
    }
    for(int i = 0; i < NUM_FILES; i++) {
      mFileIdList.add("hdfs://a/b/c" + i);
    }
  }

  @Test
  public void testConsistentHashPolicy() throws ResourceExhaustedException {
    Configuration.set(PropertyKey.USER_CONSISTENT_HASH_VIRTUAL_NODE_COUNT, NUM_VIRTUAL_NODES);
    AlluxioConfiguration conf = Configuration.global();
    WorkerLocationPolicy consistentHashPolicy = new ConsistentHashPolicy(conf);
    HashMap<BlockWorkerInfo, Integer> workerCount = new HashMap<>();
    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = consistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      for(BlockWorkerInfo worker : workers) {
        if(workerCount.containsKey(worker)) {
          workerCount.put(worker, workerCount.get(worker) + 1);
        } else {
          workerCount.put(worker, 1);
        }
      }
    }

    // 存到list里面
    List<Integer> workerCountList = new ArrayList<>(workerCount.values());
    // 计算方差
    double variance = 0;
    double average = 0;
    for(int i = 0; i < workerCountList.size(); i++) {
      average += workerCountList.get(i);
    }
    average /= workerCountList.size();
    for(int i = 0; i < workerCountList.size(); i++) {
      variance += Math.pow(workerCountList.get(i) - average, 2);
    }
    variance /= workerCountList.size();
    System.out.println("ConsistentHashPolicy variance: " + variance);
  }

  @Test
  public void testJumpConsistentHashPolicy() throws ResourceExhaustedException {
    WorkerLocationPolicy jumpConsistentHashPolicy = new JumpConsistentHashPolicy();
    HashMap<BlockWorkerInfo, Integer> workerCount = new HashMap<>();
    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = jumpConsistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      for(BlockWorkerInfo worker : workers) {
        if(workerCount.containsKey(worker)) {
          workerCount.put(worker, workerCount.get(worker) + 1);
        } else {
          workerCount.put(worker, 1);
        }
      }
    }

    // 存到list里面
    List<Integer> workerCountList = new ArrayList<>(workerCount.values());
    // 计算方差
    double variance = 0;
    double average = 0;
    for(int i = 0; i < workerCountList.size(); i++) {
      average += workerCountList.get(i);
    }
    average /= workerCountList.size();
    for(int i = 0; i < workerCountList.size(); i++) {
      variance += Math.pow(workerCountList.get(i) - average, 2);
    }
    variance /= workerCountList.size();
    System.out.println("JumpConsistentHashPolicy variance: " + variance);
  }

  @Test
  public void testConsistenceOfJumpConsistentHash() throws ResourceExhaustedException {
    Map<String, BlockWorkerInfo> getWorkerResults = new HashMap<>();

    WorkerLocationPolicy jumpConsistentHashPolicy = new JumpConsistentHashPolicy();
    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = jumpConsistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      getWorkerResults.put(fileId, workers.get(0));
    }

    // shuffle the worker info list
    Collections.shuffle(mBlockWorkerInfos);
    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = jumpConsistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      BlockWorkerInfo blockWorkerInfo = getWorkerResults.get(fileId);
      assertEquals(blockWorkerInfo.getNetAddress(), workers.get(0).getNetAddress());
    }
  }

  @Test
  public void testRemoveMiddleNode() throws ResourceExhaustedException {
    Map<String, BlockWorkerInfo> getWorkerResults = new HashMap<>();

    WorkerLocationPolicy jumpConsistentHashPolicy = new JumpConsistentHashPolicy();
    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = jumpConsistentHashPolicy.getPreferredWorkers(mBlockWorkerInfos, fileId, 1);
      getWorkerResults.put(fileId, workers.get(0));
    }

    // remove the node in the middle of the list
    int index = mBlockWorkerInfos.size() / 2;
    BlockWorkerInfo removedWorker = mBlockWorkerInfos.remove(index);
    List<BlockWorkerInfo> newList = new ArrayList<>();
    newList.add(removedWorker);

    for(String fileId : mFileIdList) {
      List<BlockWorkerInfo> workers = jumpConsistentHashPolicy.getPreferredWorkers(newList, fileId, 1);
      BlockWorkerInfo blockWorkerInfo = getWorkerResults.get(fileId);
      assertTrue(blockWorkerInfo.getNetAddress().equals(workers.get(0).getNetAddress()));
    }
  }
}
