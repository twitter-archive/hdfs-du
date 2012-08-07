/* 
 * Copyright 2012 Twitter, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.hdfsdu.data;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.common.collect.Lists;

public class Tree<T extends NodeData> {
	
	class Node {
		public String id;
		public String name;
		public String pathCmp;
		public T data;
		public List<Node> children;
		
		public Node(String id, String name, String pathCmp, T data) {
			this.id = id;
			this.name = name;
			this.pathCmp = pathCmp;
			this.data = data;
			this.children = new ArrayList<Node>();
		}
		
		public JSONObject toJSON() {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", this.id);
			jsonObject.put("name", this.name);
			jsonObject.put("data", data.toJSON());
			
			JSONArray jsonArray = new JSONArray();
			List<Node> children = this.children;
			for (Node n : children) {
				jsonArray.add(n.toJSON());
			}
			
			jsonObject.put("children", jsonArray);
			return jsonObject;
		}
	}
	
	private Node root;
	private String rootPath;
	
	public Tree(String rootPath) {
		this.rootPath = rootPath;
	}
	
	public void add(T node) {
		if (root == null) {
			String name = new File(node.path).getName();
			if (name.isEmpty()) {
				 name = "/";
			}
			
			root = new Node(node.path, 
							name, 
							node.path, 
							node);
			return;
		}
		
		List<String> stringCmp = new ArrayList<String>();
		stringCmp.add(rootPath);
		int len = rootPath.length();
		if (!rootPath.equals("/")) {
			len++;
		}
		String[] strippedPath = node.path.substring(len).split("/");
		for (String string : strippedPath) {
			stringCmp.add(string);
		}
		addNodeHelper(root, node, stringCmp.toArray(new String[] {}), 0);
	}
	
	private void addNodeHelper(Node node, NodeData data, String[] path, int index) {
		if (index + 1 >= path.length) {
			return;
		}
		
		String pathCmp = path[index + 1];
		List<Node> nodes = node.children;
		
		for (Node n : nodes) {
			if (n.pathCmp.equals(pathCmp)) {
				addNodeHelper(n, data, path, index + 1);
				return;
			}
		}
		
		Node newNode = new Node(node.id + (node.id.equals("/") ? "": "/") + pathCmp, pathCmp, pathCmp, (T) data);
		nodes.add(newNode);
		return;
	}
	
	public void prune(int depth) {
		pruneHelper(root, depth);
	}
	
	private void pruneHelper(Node n, int depth) {
		if (depth == 0) {
			n.children = Lists.newArrayList();
			return;
		}
		List<Node> ch = n.children;
		for (Node node : ch) {
			pruneHelper(node, depth -1);
		}
	}
	
	public void setDescendantCount() {
		setDescendantCountHelper(root);
	}
	
	private void setDescendantCountHelper(Node n) {
		List<Node> ch = n.children;
		for (Node node : ch) {
			setDescendantCountHelper(node);
		}
		long acum = 0;
		for (Node node : ch) {
			acum += node.data.nChildren + 1;
		}
		n.data.nChildren = acum;
	}
	
	public JSONObject toJSON() {
		if (root == null) {
			return null;
		}
		return root.toJSON();
	}
}
