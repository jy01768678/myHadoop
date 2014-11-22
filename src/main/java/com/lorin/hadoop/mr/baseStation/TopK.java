package com.lorin.hadoop.mr.baseStation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

public class TopK {
	public static void main(String[] args) {
		// 源数据
//		int[] data = { 56, 275, 12, 6, 45, 478, 41, 1236, 456, 12, 546, 45 };
//
//		// 获取Top5
//		int[] top5 = topK(data, 5);
//
//		for (int i = 0; i < 5; i++) {
//			System.out.println(top5[i]);
//		}
		List<HeapNode> nodes = new ArrayList<HeapNode>();
		HeapNode node = new HeapNode();
		node.setKey("A");
		node.setValue(56);
		nodes.add(node);
		
		node = new HeapNode();
		node.setKey("T");
		node.setValue(275);
		nodes.add(node);
		
		node = new HeapNode();
		node.setKey("G");
		node.setValue(6);
		nodes.add(node);
		
		node = new HeapNode();
		node.setKey("H");
		node.setValue(12);
		nodes.add(node);
		
		node = new HeapNode();
		node.setKey("A");
		node.setValue(45);
		nodes.add(node);
		
		node = new HeapNode();
		node.setKey("E");
		node.setValue(478);
		nodes.add(node);
		
		node = new HeapNode();
		node.setKey("B");
		node.setValue(41);
		nodes.add(node);
		node = new HeapNode();
		node.setKey("U");
		node.setValue(1236);
		nodes.add(node);
		
		// 获取Top5
		List<HeapNode> tops = topK(nodes, 3);

		for (int i = 0; i < 3; i++) {
			System.out.println(tops.get(i).getValue());
		}
		
//		HashMap<String,Float> map = new HashMap<String,Float>();
//		map.put("1", 56f);
//		map.put("2", 275f);
//		map.put("3", 6f);
//		map.put("4", 45f);
//		map.put("5", 478f);
//		map.put("6", 12f);
//		
//		System.out.println(getTopK(map,3));
	}
	
	private static HashMap<String, Float> getTopK(HashMap<String, Float> locs,int topN) {
		HashMap<String, Float> topMap = new HashMap<String,Float>(topN);
		Iterator<Entry<String,Float>> iter = locs.entrySet().iterator();
		Entry<String,Float> entry = null;
		String tempKey = "";
		TreeSet<String> keySet = new TreeSet<String>();
		while(iter.hasNext()){
			entry = (Entry<String,Float>) iter.next();
			float data = (Float)entry.getValue();
			if(topMap.size() < topN){
				topMap.put(entry.getKey().toString(), data);
				keySet.add(entry.getKey().toString());
			}else if(topMap.get(keySet.first()) < data){
				topMap.remove(keySet.first());
				tempKey = keySet.first();
				keySet.remove(tempKey);
				topMap.put(entry.getKey().toString(), data);
				keySet.add(entry.getKey().toString());
			}
		}
		return topMap;
	}
	

	private static List<HeapNode> topK(List<HeapNode> nodes, int k) {
		// 先取K个元素放入一个数组topk中
		List<HeapNode> topNode = new ArrayList<HeapNode>(k);
		topNode.addAll(nodes.subList(0, k));
		// 转换成最小堆
		MinHeap heap = new MinHeap(topNode);
		HeapNode temp = null;
		// 从k开始，遍历data
		for (int i = k; i < nodes.size(); i++) {
			float root = heap.getRoot();
			temp = nodes.get(i);
			// 当数据大于堆中最小的数（根节点）时，替换堆中的根节点，再转换成堆
			if (temp.getValue() > root) {
				heap.setRoot(temp.getValue(),temp.getKey());
			}
		}

		return topNode;
	}
//	// 从data数组中获取最大的k个数
//	private static int[] topK(int[] data, int k) {
//		// 先取K个元素放入一个数组topk中
//		int[] topk = new int[k];
//		for (int i = 0; i < k; i++) {
//			topk[i] = data[i];
//		}
//
//		// 转换成最小堆
//		MinHeap heap = new MinHeap(topk);
//
//		// 从k开始，遍历data
//		for (int i = k; i < data.length; i++) {
//			int root = heap.getRoot();
//
//			// 当数据大于堆中最小的数（根节点）时，替换堆中的根节点，再转换成堆
//			if (data[i] > root) {
//				heap.setRoot(data[i]);
//			}
//		}
//
//		return topk;
//	}
}
