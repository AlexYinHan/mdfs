package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FileBlockManager {
    private Map<String, List<String>> fileBlock_dataNodeURL = new HashMap<>();

    public void addFileBlock(String dataNodeURL, String fileName, long blockIndex) {
        String fileBlock = getBlockName(fileName, blockIndex);
        List<String> URLS = fileBlock_dataNodeURL.get(fileBlock);
        if (URLS == null) {
            URLS = new LinkedList<>();
            URLS.add(dataNodeURL);
            fileBlock_dataNodeURL.put(fileBlock, URLS);
        } else {
            URLS.add(dataNodeURL);
        }
    }

    public Map<String, List<String>> listAll() {
        return new HashMap<>(fileBlock_dataNodeURL);
    }

    public List<String> getDataNodeURL(String fileName, long blockIndex) {
        return fileBlock_dataNodeURL.get(getBlockName(fileName, blockIndex));
    }

    public void removeFileBlock(String fileName, long blockIndex) {
        String fileBlock = getBlockName(fileName, blockIndex);
        fileBlock_dataNodeURL.remove(fileBlock);
    }

    public void removeDataNode(String URL) {
        for (Map.Entry<String, List<String>> e : fileBlock_dataNodeURL.entrySet()) {
            e.getValue().removeIf(u -> u.equals(URL));
        }
    }

    public void transferBlock(String fileName, long blockIndex, String oldURL, String newURL) {
        List<String> dataNodeURLs = fileBlock_dataNodeURL.get(getBlockName(fileName, blockIndex));
        dataNodeURLs.remove(oldURL);
        dataNodeURLs.add(newURL);
    }

    private String getBlockName(String fileName, long blockIndex) {
        return fileName + "#" + blockIndex;
    }

    public void show() {
        System.out.println("File block manager:");
        for (Map.Entry<String, List<String>> e : fileBlock_dataNodeURL.entrySet()) {
            System.out.println(e.getKey());
            for (String nodeURL : e.getValue()) {
                System.out.println("\t\t" + nodeURL);
            }
        }
    }
}
