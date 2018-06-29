package com.alex.sa.mdfs.namenode;

import javafx.util.Pair;

import javax.swing.*;
import java.util.LinkedList;
import java.util.List;

public class FileTreeManager {
    private class FileTreeNode {
        boolean isDir;
        String name;
        long numBlocks;
        List<FileTreeNode> children = new LinkedList<>();

        public FileTreeNode(boolean isDir, String name, long numBlocks) {
            this.isDir = isDir;
            this.name = name;
            this.numBlocks = numBlocks;
        }
    }

    FileTreeNode root = new FileTreeNode(true, "root", 0);

    public void addFile(String path, long numBlocks) {
        FileTreeNode node = root;
        String[] dirs = path.split("/");
        int count = dirs.length;
        for (int i = 0; i < count - 1; i ++) {
            FileTreeNode next = null;
            for (FileTreeNode child : node.children) {
                if (child.isDir && child.name.equals(dirs)) {
                    next = child;
                    break;
                }
            }
            if (next == null) {
                FileTreeNode newDirNode = new FileTreeNode(true, dirs[i], 0);
                node.children.add(newDirNode);
                next = newDirNode;
            }
            node = next;
        }

        String fileName = dirs[count - 1];
        node.children.add(new FileTreeNode(false, fileName, numBlocks));
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
            return true;
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
}
