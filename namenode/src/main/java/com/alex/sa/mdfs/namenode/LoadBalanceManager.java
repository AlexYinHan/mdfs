package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.w3c.dom.ls.LSInput;

import javax.persistence.Tuple;
import java.io.*;
import java.util.*;

public class LoadBalanceManager {

    private class HashNode {
        HashNode(int hashValue, String actualURL) {
            this.hashValue = hashValue;
            this.actualURL = actualURL;
        }

        int hashValue;
        String actualURL;
        Map<String, Integer> fileName_hashcodes = new HashMap<>();
    }

    private int numVisualNodes;
    private Set<String> dataNodeURLS = new HashSet<>();
    private List<HashNode> hashNodes = new LinkedList<>();
    private Map<String, List<String>> fileMaps = new HashMap<>();
//    private List<Pair<Integer, String>> hashNodes = new LinkedList<>();
    private Map<String, List<Integer>> fileName_nodeIndex = new HashMap<>();

    // load-balancer
    @Value(value="${load-balancer.num-visual-node:}")
    public int VISUAL_NODE_NUM;

    public LoadBalanceManager(int numVisualNodes) {
        this.numVisualNodes = numVisualNodes;
    }

    public void setNumVisualNodes(int numVisualNodes) {
        this.numVisualNodes = numVisualNodes;
    }

    public void addDataNode(String URL) {
        dataNodeURLS.add(URL);
        for (int visualNodeIndex = 0; visualNodeIndex < numVisualNodes + 1; visualNodeIndex ++) {
            addDataNode(stringHashCode( visualNodeIndex + "#" + URL), URL);
        }
        updateFileMap();
    }

    public void removeDataNode(String URL) {
        dataNodeURLS.remove(URL);
        hashNodes.removeIf(t -> t.actualURL.equals(URL));
        updateFileMap();
    }

    public void removeFile(String fileName) {
        for (HashNode node : hashNodes) {
            node.fileName_hashcodes.remove(fileName);
        }
        updateFileMap();
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
        for (; hashCodeIndex < hashNodes.size(); hashCodeIndex ++) {
            HashNode node = hashNodes.get(hashCodeIndex);
            if (node.hashValue == hashCode) {
                hashCode ++;
            }
            if (node.hashValue > hashCode) {
                break;
            }
        }

        if (hashCodeIndex == hashNodes.size()) {
            // insert to the tail
            hashNodes.add(new HashNode(hashCode, dataNodeURL));
        } else {
            hashNodes.add(hashCodeIndex, new HashNode(hashCode, dataNodeURL));
        }
        updateFileMap();
    }
    public List<String> getTargetDataNodeURLS(File file, int numReplicas) {
        String fileName = file.getName();
        List<String> targetDataNodeURLS = new LinkedList<>();
        if (dataNodeURLS.size() == 0) {
            return targetDataNodeURLS;
        }

        int hashcode = fileHash(file);
        String dataNodeURL = null;
        int hashNodeIndex = 0;
        for (HashNode node : hashNodes) {
            if (node.hashValue >= hashcode) {
                dataNodeURL = node.actualURL;
                hashNodeIndex = hashNodes.indexOf(node);
                node.fileName_hashcodes.put(fileName, hashcode);
                break;
            }
        }
        if (dataNodeURL == null) {
            dataNodeURL = hashNodes.get(0).actualURL;
            hashNodes.get(0).fileName_hashcodes.put(fileName, hashcode);
        }
        targetDataNodeURLS.add(dataNodeURL);

        // allocate data nodes for replicas
        // follow SimpleStrategy or RackUnawareStrategy of Cassandra
        // simply find the next (numReplicas - 1) nodes
        // set hashcode to half of the two nodes
        hashNodeIndex = (hashNodeIndex + 1) % hashNodes.size();
        for (int replicaIndex = 0; replicaIndex < numReplicas - 1; replicaIndex ++) {
            if (replicaIndex + 2 > dataNodeURLS.size()) {
                // not enough data nodes
                System.err.println("Not enough data nodes for replicas.");
                break;
            }
            while (targetDataNodeURLS.contains(hashNodes.get(hashNodeIndex).actualURL)) {
                hashNodeIndex = (hashNodeIndex + 1) % hashNodes.size();
            }
            targetDataNodeURLS.add(hashNodes.get(hashNodeIndex).actualURL);

            // set hashcode of replica
            // middle of the two nearest neighbours
            int code = hashNodes.get(hashNodeIndex).hashValue;
            int previousCode = hashNodeIndex - 1 >=0 ?
                    hashNodes.get(hashNodeIndex - 1).hashValue : hashNodes.get(hashNodes.size() - 1).hashValue;
            int codeGrowth = code - previousCode;
            int hashCode = codeGrowth >= 0 ? (previousCode + codeGrowth / 2) : Integer.MAX_VALUE;
            hashNodes.get(hashNodeIndex).fileName_hashcodes.put(fileName, hashCode);
        }

        updateFileMap();
        return targetDataNodeURLS;
    }

    public Map<String, String> fileName_replaceDataNoteURLs(String URL) {
        showFiles();

        Map<String, String> fileName_URL = new HashMap<>();
        int numHashNodes = hashNodes.size();
        for (HashNode node : hashNodes) {
            int nodeIndex = hashNodes.indexOf(node);
            if (node.actualURL.equals(URL)) {
                // actual data node or visual node
                for (String fileName : node.fileName_hashcodes.keySet()) {
                    // find a node to match the file
                    // find the first node following the original node which
                    //  1. doesn't possess the same file
                    //  2. doesn't belong to the same actual node
                    int hashCode = node.fileName_hashcodes.get(fileName);
                    for (int i = nodeIndex + 1; i != nodeIndex; i = (i+1)%numHashNodes) {
                        if (!fileMaps.get(fileName).contains(hashNodes.get(i).actualURL) &&
                                !hashNodes.get(i).actualURL.equals(URL)) {
                            fileName_URL.put(fileName, hashNodes.get(i).actualURL);
                            hashNodes.get(i).fileName_hashcodes.put(fileName, hashCode);
                            break;
                        }
                    }
                }
            }
        }

        updateFileMap();
        showFiles();

        return fileName_URL;
    }

    public Map<String, String> fileName_originalDataNoteURLs(String newNodeURL) {
        HashMap<String, String> fileName_URLs = new HashMap<>();

        // record the changes of block allocate
        class TransferRecord {
            int originalNodeIndex, newNodeIndex;
            String fileName;

            public TransferRecord(int originalNodeIndex, int newNodeIndex, String fileName, int hashcode) {
                this.originalNodeIndex = originalNodeIndex;
                this.newNodeIndex = newNodeIndex;
                this.fileName = fileName;
                this.hashcode = hashcode;
            }

            int hashcode;
        }
        List<TransferRecord> transferRecords = new LinkedList<>();

        // for every new node, allocate to it some blocks from the next node
        for (int nodeIndex = 0; nodeIndex < hashNodes.size(); nodeIndex ++) {
            HashNode thisNode = hashNodes.get(nodeIndex);
            if (thisNode.actualURL.equals(newNodeURL)) {
                int nextNodeIndex = (nodeIndex + 1) % hashNodes.size();
                HashNode nextNode = hashNodes.get(nextNodeIndex);
                if (nextNode.actualURL.equals(newNodeURL)) {
                    continue;
                }
                // for every file belonging to the next node
                for (Map.Entry<String, Integer> fileName_hashcode : nextNode.fileName_hashcodes.entrySet()) {
                    if (fileName_hashcode.getValue() <= thisNode.hashValue) {
                        // if by hashcode this file should be allocated to the new node
                        if (!fileName_URLs.keySet().contains(fileName_hashcode.getKey())) {
                            // if this file hasn't been allocated to the new node
                            fileName_URLs.put(fileName_hashcode.getKey(), nextNode.actualURL);
                            transferRecords.add(new TransferRecord(nextNodeIndex, nodeIndex, fileName_hashcode.getKey(), fileName_hashcode.getValue()));
                        }
                    }
                }
            }
        }

        // change block allocate record
        for (TransferRecord transferRecord : transferRecords) {
            hashNodes.get(transferRecord.originalNodeIndex).fileName_hashcodes.remove(transferRecord.fileName);
            hashNodes.get(transferRecord.newNodeIndex).fileName_hashcodes.put(transferRecord.fileName, transferRecord.hashcode);
        }

        updateFileMap();

        return fileName_URLs;
    }

    private void showFiles() {
        for (HashNode node : hashNodes) {
            if (node.fileName_hashcodes.size() > 0) {
                for (String file : node.fileName_hashcodes.keySet()) {
                    System.out.println("\t\t" + file + "," + node.actualURL);
                }
            }
        }
        System.out.println();
    }

    private void updateFileMap() {
        fileMaps.clear();
        for (HashNode node : hashNodes) {
            for (String file : node.fileName_hashcodes.keySet()) {
                if (!fileMaps.keySet().contains(file)) {
                    fileMaps.put(file, new LinkedList<>());
                }
                fileMaps.get(file).add(node.actualURL);

            }
        }
    }

    public void show() {
        System.out.println("Load balance manager:");
        showFiles();
    }

}
