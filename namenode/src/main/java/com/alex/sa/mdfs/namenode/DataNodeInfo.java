package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;

import java.util.LinkedList;
import java.util.List;

public class DataNodeInfo {
    private String URL;

    private boolean valid = true;

    private List<Pair<String, Long>> file_blocks = new LinkedList<>();

    public DataNodeInfo(String URL) {
        this.URL = URL;
        valid = true;
    }

    public void addBlock(String fileName, long blockIndex) {
        file_blocks.add(new Pair<>(fileName, blockIndex));
    }

    public List<Pair<String, Long>> getFileBlocks() {
        return new LinkedList<>(file_blocks);
    }


    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

}
