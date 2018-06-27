package com.alex.sa.mdfs.namenode;

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
            node = next;
        }

        String fileName = dirs[count - 1];
        for (FileTreeNode child : node.children) {
            if (!child.isDir && child.name.equals(fileName)) {
                node = child;
                break;
            }
        }
        return node.numBlocks;
    }
}
