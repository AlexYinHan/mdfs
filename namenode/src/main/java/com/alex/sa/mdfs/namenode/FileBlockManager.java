package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FileBlockManager {
    private Map<String, List<String>> fileBlock_dataNodeURL = new HashMap<>();

    public void addFileBlock(String dataNodeURL, String fileName, long blockIndex) {
        String fileBlock = fileName + "#" + blockIndex;
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
        return fileBlock_dataNodeURL.get(fileName + "#" + blockIndex);
    }
}
