package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Value;

import java.io.*;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ConsistentHashManager {

    private int numVisualNodes;
    private int visualNodeHashcodeStep;
    private Set<String> dataNodeURLS = new HashSet<>();
    private List<Pair<Integer, String>> hashcode_dataNodeIndex = new LinkedList<>();
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
        for (; hashCodeIndex < hashcode_dataNodeIndex.size(); hashCodeIndex ++) {
            Pair<Integer, String> node = hashcode_dataNodeIndex.get(hashCodeIndex);
            if (node.getKey() == hashCode) {
                hashCode ++;
            }
            if (node.getKey() > hashCode) {
                break;
            }
        }

        if (hashCodeIndex == hashcode_dataNodeIndex.size()) {
            // insert to the tail
            hashcode_dataNodeIndex.add(new Pair<>(hashCode, dataNodeURL));
        } else {
            hashcode_dataNodeIndex.add(hashCodeIndex, new Pair<>(hashCode, dataNodeURL));
        }
    }
    public List<String> getTargetDataNodeURLS(File file, int numReplicas) {
        List<String> targetDataNodeURLS = new LinkedList<>();
        if (dataNodeURLS.size() == 0) {
            return targetDataNodeURLS;
        }

        int hashcode = fileHash(file);
        int replicaHashcodeStep = (hashCodeRange / numReplicas) * 2;
        for (int replicaIndex = 0; replicaIndex < numReplicas; replicaIndex ++) {
            String dataNodeURL = null;
            for (Pair<Integer, String> node : hashcode_dataNodeIndex) {
                if (node.getKey() >= hashcode) {
                    dataNodeURL = node.getValue();
                    break;
                }
            }
            if (dataNodeURL == null) {
                dataNodeURL = hashcode_dataNodeIndex.get(0).getValue();
            }
            targetDataNodeURLS.add(dataNodeURL);

            hashcode += replicaHashcodeStep;
        }
        return targetDataNodeURLS;
    }

}
