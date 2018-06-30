package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;

import javax.swing.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FileTreeManager {
    private class FileTreeNode {
        boolean isDir;
        String name;
        long numBlocks;
        long size;
        List<FileTreeNode> children = new LinkedList<>();

        public FileTreeNode(boolean isDir, String name, long numBlocks, long size) {
            this.isDir = isDir;
            this.name = name;
            this.numBlocks = numBlocks;
            this.size = size;
        }
    }

    FileTreeNode root = new FileTreeNode(true, "root", 0, 0);

    public void addFile(String path, long numBlocks, long size) {
        FileTreeNode node = root;
        String[] dirs = path.split("/");
        int count = dirs.length;
        for (int i = 0; i < count - 1; i ++) {
            FileTreeNode next = null;
            for (FileTreeNode child : node.children) {
                if (child.isDir && child.name.equals(dirs[i])) {
                    next = child;
                    break;
                }
            }
            if (next == null) {
                FileTreeNode newDirNode = new FileTreeNode(true, dirs[i], 0, 0);
                node.children.add(newDirNode);
                next = newDirNode;
            }
            node = next;
        }

        String fileName = dirs[count - 1];
        node.children.add(new FileTreeNode(false, fileName, numBlocks, size));
    }

    public long getNumBlocks(String path) {
        Pair<FileTreeNode, String> parentNode_fileName = walk(path);
        if (parentNode_fileName == null) {
            return 0;
        }
        FileTreeNode node = parentNode_fileName.getKey();
        String fileName = parentNode_fileName.getValue();

        for (FileTreeNode child : node.children) {
            if (!child.isDir && child.name.equals(fileName)) {
                node = child;
                break;
            }
        }
        return node.numBlocks;
    }

    public void deleteFile(String path) {
        Pair<FileTreeNode, String> parentNode_fileName = walk(path);
        if (parentNode_fileName == null) {
            return;
        }
        FileTreeNode node = parentNode_fileName.getKey();
        String fileName = parentNode_fileName.getValue();

        for (FileTreeNode child : node.children) {
            if (!child.isDir && child.name.equals(fileName)) {
                node.children.remove(child);
                break;
            }
        }
    }

    public boolean contain(String path) {
        Pair<FileTreeNode, String> parentNode_fileName = walk(path);
        if (parentNode_fileName == null) {
            return false;
        }
        FileTreeNode node = parentNode_fileName.getKey();
        String fileName = parentNode_fileName.getValue();
        for (FileTreeNode child : node.children) {
            if (!child.isDir && child.name.equals(fileName)) {
                return true;
            }
        }
        return false;
    }

    private Pair<FileTreeNode, String> walk(String path) {
        FileTreeNode node = root;
        String[] dirs = path.split("/");
        int count = dirs.length;
        for (int i = 0; i < count - 1; i ++) {
            FileTreeNode next = null;
            for (FileTreeNode child : node.children) {
                if (child.isDir && child.name.equals(dirs[i])) {
                    next = child;
                    break;
                }
            }
            if (next == null) {
                System.err.println("Fail to find file with path : " + path);
                return null;
            }
            node = next;
        }

        String fileName = dirs[count - 1];
        return new Pair<>(node, fileName);
    }

    public Map<String, Long> list() {
        Map<String, Long> fileWithPath_fileSize = new HashMap<>();
        FileTreeNode node = root;
        for (FileTreeNode fileTreeNode : node.children) {
            addNodeToMap(fileTreeNode, "", fileWithPath_fileSize);
        }
        return fileWithPath_fileSize;
    }

    private void addNodeToMap(FileTreeNode node, String path, Map<String, Long> fileWithPath_fileSize) {
        if (!node.isDir) {
            fileWithPath_fileSize.put(path + node.name, node.size);
        } else {
            path += node.name + "/";
            for (FileTreeNode fileTreeNode : node.children) {
                addNodeToMap(fileTreeNode, path, fileWithPath_fileSize);
            }
        }
    }
}
