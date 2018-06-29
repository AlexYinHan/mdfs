package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Value;

import java.io.*;
import java.util.*;

public class ConsistentHashManager {

    private int numVisualNodes;
    private int visualNodeHashcodeStep;
    private Set<String> dataNodeURLS = new HashSet<>();
    private List<Pair<Integer, String>> hashcode_dataNodeURL = new LinkedList<>();
    private static final int hashCodeRange = Integer.MAX_VALUE;

    // load-balancer
    @Value(value="${load-balancer.num-visual-node:}")
    public int VISUAL_NODE_NUM;

    public ConsistentHashManager(int numVisualNodes) {
        this.numVisualNodes = numVisualNodes;
        this.visualNodeHashcodeStep = (hashCodeRange / numVisualNodes) * 2;
    }

    public void setNumVisualNodes(int numVisualNodes) {
        this.numVisualNodes = numVisualNodes;
        this.visualNodeHashcodeStep = (hashCodeRange / (numVisualNodes+1)) * 2;
    }

    public void addDataNode(String URL) {
        dataNodeURLS.add(URL);
        for (int visualNodeIndex = 0; visualNodeIndex < numVisualNodes + 1; visualNodeIndex ++) {
            addDataNode(stringHashCode( visualNodeIndex + "#" + URL), URL);
        }
    }

    public void removeDataNode(String URL) {
        dataNodeURLS.remove(URL);
        hashcode_dataNodeURL.removeIf(t -> t.getValue().equals(URL));
    }

    private int stringHashCode(String URL) {
        //  FNV1_32_HASH algorithm
        int p = 16777619;
        int hash = (int)2166136261L;
        for (int i = 0; i < URL.length(); i++)
            hash = (hash ^ URL.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        return hash;
    }

    private int fileHash(File file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String str = null, fileContent = "";
            while((str = br.readLine()) != null){
                fileContent += str;
            }
            return stringHashCode(fileContent);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return file.hashCode();
    }

    private synchronized void addDataNode(int hashCode, String dataNodeURL) {
        int hashCodeIndex = 0;
        // find the index for hashCode to insert
        for (; hashCodeIndex < hashcode_dataNodeURL.size(); hashCodeIndex ++) {
            Pair<Integer, String> node = hashcode_dataNodeURL.get(hashCodeIndex);
            if (node.getKey() == hashCode) {
                hashCode ++;
            }
            if (node.getKey() > hashCode) {
                break;
            }
        }

        if (hashCodeIndex == hashcode_dataNodeURL.size()) {
            // insert to the tail
            hashcode_dataNodeURL.add(new Pair<>(hashCode, dataNodeURL));
        } else {
            hashcode_dataNodeURL.add(hashCodeIndex, new Pair<>(hashCode, dataNodeURL));
        }
    }
    public List<String> getTargetDataNodeURLS(File file, int numReplicas) {
        List<String> targetDataNodeURLS = new LinkedList<>();
        if (dataNodeURLS.size() == 0) {
            return targetDataNodeURLS;
        }

        int hashcode = fileHash(file);
        String dataNodeURL = null;
        int dataNodeURLIndex = 0;
        for (Pair<Integer, String> node : hashcode_dataNodeURL) {
            if (node.getKey() >= hashcode) {
                dataNodeURL = node.getValue();
                dataNodeURLIndex = hashcode_dataNodeURL.indexOf(node);
                break;
            }
        }
        if (dataNodeURL == null) {
            dataNodeURL = hashcode_dataNodeURL.get(0).getValue();
        }
        targetDataNodeURLS.add(dataNodeURL);

        // allocate data nodes for replicas
        // simply find the next (numReplicas - 1) nodes
        dataNodeURLIndex = (dataNodeURLIndex + 1) % hashcode_dataNodeURL.size();
        for (int replicaIndex = 0; replicaIndex < numReplicas - 1; replicaIndex ++) {
            if (replicaIndex + 2 > dataNodeURLS.size()) {
                // not enough data nodes
                break;
            }
            while (targetDataNodeURLS.contains(hashcode_dataNodeURL.get(dataNodeURLIndex).getValue())) {
                dataNodeURLIndex = (dataNodeURLIndex + 1) % hashcode_dataNodeURL.size();
            }
            targetDataNodeURLS.add(hashcode_dataNodeURL.get(dataNodeURLIndex).getValue());
        }

        return targetDataNodeURLS;
    }

}
