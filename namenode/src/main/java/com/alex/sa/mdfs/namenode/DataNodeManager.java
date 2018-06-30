package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;
import org.springframework.stereotype.Service;

import java.util.*;


public class DataNodeManager {


    private Map<String, DataNodeInfo> URL_dataNodeInfos = new HashMap<>();

    public List<String> getAllDataNodeURL() {
        return new ArrayList<>(URL_dataNodeInfos.keySet());
    }

    public void removeDataNode(String URL) {
        URL_dataNodeInfos.remove(URL);
    }

    public void addDataNode(String URL) {
        DataNodeInfo dataNodeInfo = new DataNodeInfo(URL);
        URL_dataNodeInfos.put(URL, dataNodeInfo);
    }

    public List<Pair<String, Long>> getBlocks(String dataNodeURL) {
        return URL_dataNodeInfos.get(dataNodeURL).getFileBlocks();
    }

    public void addFileBlock(String dataNodeURL, String fileName, long blockIndex) {
        URL_dataNodeInfos.get(dataNodeURL).addBlock(fileName, blockIndex);
    }

    public boolean isValid(String dataNodeURL) {
        return URL_dataNodeInfos.get(dataNodeURL).isValid();
    }

    public void removeBlock(String fileName, long blockIndex) {
        for (String dataNodeURL : URL_dataNodeInfos.keySet()) {
            URL_dataNodeInfos.get(dataNodeURL).removeBlock(fileName, blockIndex);
        }
    }

    public boolean contain(String URL) {
        return URL_dataNodeInfos.containsKey(URL);
    }

    public List<Pair<String, Long>> getFileBlocks(String URL) {
        return URL_dataNodeInfos.get(URL).getFileBlocks();
    }

    public void transferBlock(String fileName, long blockIndex, String oldURL, String newURL) {
        URL_dataNodeInfos.get(oldURL).removeBlock(fileName, blockIndex);
        URL_dataNodeInfos.get(newURL).addBlock(fileName, blockIndex);
    }

    public void show() {
        System.out.println("Data node manager:");
        for (Map.Entry<String, DataNodeInfo> e : URL_dataNodeInfos.entrySet()) {
            System.out.println(e.getKey());
            for (Pair<String, Long> p: e.getValue().getFileBlocks()) {
                System.out.println("\t\t" + p.getKey() + "#" + p.getValue());
            }
        }
    }
}
