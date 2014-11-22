package com.lorin.hadoop.mr.baseStation;

import java.util.List;

public class MinHeap {
	
	List<HeapNode> nodes;
	
	int length;
	
	public MinHeap(List<HeapNode> nodes) {
		this.nodes = nodes;
		this.length = nodes.size();
		buildHeap();
	}

	// 将数组转换成最小堆
	private void buildHeap() {
		// 完全二叉树只有数组下标小于或等于 (data.length) / 2 - 1 的元素有孩子结点，遍历这些结点。
		// *比如上面的图中，数组有10个元素， (data.length) / 2 - 1的值为4，a[4]有孩子结点，但a[5]没有*
		for (int i = (length) / 2 - 1; i >= 0; i--) {
			// 对有孩子结点的元素heapify
			heapify(i);
		}
	}

	private void heapify(int i) {
		// 获取左右结点的数组下标
		int l = left(i);
		int r = right(i);

		// 这是一个临时变量，表示 跟结点、左结点、右结点中最小的值的结点的下标
		int smallest = i;

		// 存在左结点，且左结点的值小于根结点的值
		if (l < length && nodeValue(l) < nodeValue(i))
			smallest = l;

		// 存在右结点，且右结点的值小于以上比较的较小值
		if (r < length && nodeValue(r) < nodeValue(smallest))
			smallest = r;

		// 左右结点的值都大于根节点，直接return，不做任何操作
		if (i == smallest)
			return;

		// 交换根节点和左右结点中最小的那个值，把根节点的值替换下去
		swap(i, smallest);

		// 由于替换后左右子树会被影响，所以要对受影响的子树再进行heapify
		heapify(smallest);
	}
	
	public float nodeValue(int index){
		return nodes.get(index).getValue();
	}

	// 获取右结点的数组下标
	private int right(int i) {
		return (i + 1) << 1;
	}

	// 获取左结点的数组下标
	private int left(int i) {
		return ((i + 1) << 1) - 1;
	}

	// 交换元素位置
	private void swap(int i, int j) {
		HeapNode temp = nodes.get(i);
		nodes.set(i, nodes.get(j));
		nodes.set(j, temp);
	}

	// 获取对中的最小的元素，根元素
	public float getRoot() {
		return nodes.get(0).getValue();
	}

	// 替换根元素，并重新heapify
	public void setRoot(float value,String key) {
		HeapNode root = new HeapNode();
		root.setKey(key);
		root.setValue(value);
		nodes.set(0, root);
		heapify(0);
	}
}
