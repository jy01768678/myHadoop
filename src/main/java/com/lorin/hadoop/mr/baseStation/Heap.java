package com.lorin.hadoop.mr.baseStation;

public class Heap {
	public static class HeapTree{         
        private int size=0;  
        private int[] heapArray;  
        private int capacity;  
        public HeapTree(int n){  
            heapArray = new int[n];  
            this.capacity = n;  
        }         
        public int size(){  
            return this.size;  
        }                 
          
        public void insert(int val){  
            if(this.size==0){                  
                 heapArray[0] = val;  
                 this.size++;  
            }else if(this.size==capacity){  
                //capacity full  
            	tryReplaceLeast(val);  
            }else{                
                heapArray[this.size]=val;  
                siftUp(this.size);            
                this.size++;  
            }  
            System.out.print(val + "===");
            for(int i = 0;i<heapArray.length;i++){
            	System.out.print(heapArray[i] + "\t");
            }
            System.out.println();
        }  
        private void tryReplaceLeast(int val){            
            int least =  Integer.MAX_VALUE;  
            int leastIndex=-1;  
            //the first leaf node starts with index n/2   
            for(int i=this.size/2;i<this.size;i++){  
                if(heapArray[i]<least){  
                    least = heapArray[i];  
                    leastIndex = i;  
                }  
            }  
            System.out.println("leastIndex:"+leastIndex);
            /* 
            //only check leaf nodes: 2*i+1>size-1 
            for(int i=this.size-1;i>=0;i--){ 
                if(2*i+1 > size - 1){ 
                    if(heapArray[i]<least){ 
                        least = heapArray[i]; 
                        leastIndex = i; 
                    } 
                }else{ 
                    break; 
                } 
            } 
            */  
              
            //try to replace the least with the input value if the input value is larger  
            if(val>heapArray[leastIndex]) {  
                heapArray[leastIndex] = val;  
                siftUp(leastIndex);  
            }         
        }  
        
        //由于最大堆的容量已定，所以不能直接加到最后 然后再上浮，开始节点为最后一个根节点往下寻找最小值
        private void tryReplaceLeast2(int val){
        	int least = Integer.MAX_VALUE;
        	int leastIndex = -1;
        	int start = (this.size-1) / 2;
        	//找到一个最小的叶子
        	for(int i = start ;i<this.size;i++){
        		if(heapArray[i]<least){  
                    least = heapArray[i];  
                    leastIndex = i;  
                } 
        	}
        	//如果比最小的叶子还要大就替换 当前插入的节点
        	if(val > heapArray[leastIndex]){
        		heapArray[leastIndex] = val;
        		siftUp(leastIndex);
        	}
        }
          
        private void siftUp(int index){  
            if(index<=0)return;  
            int parent = (index - 1)/2;  
              
            //a max-heap              
            if(heapArray[parent]<=heapArray[index]){  
                //swap  
                int tmp = heapArray[parent];  
                heapArray[parent] = heapArray[index];  
                heapArray[index] = tmp;  
                  
                siftUp(parent);  
            }             
        }  
          
        public void showTopN(){       
            while(this.size>0) {               
                //print the root  
                System.out.print(heapArray[0] + " ");                 
                //replace the root with last item  
                heapArray[0] = heapArray[this.size-1];  
                this.size--;  
                if(this.size>0){  
                    siftDown2(0);  
                }  
            }  
        }  
        private void siftDown(int index){  
            int leftChildIndex = 2*index+1;  
            int rightChildIndex =leftChildIndex+1;  
              
            int val = heapArray[index];  
            if(leftChildIndex>=this.size){  
                //index is a leaf node  
                return;  
            }else if(rightChildIndex>=this.size){  
                //only have a left child  
                int leftChildVal = heapArray[leftChildIndex];  
                if(val<=leftChildVal){  
                    //swap  
                    heapArray[leftChildIndex] = val;  
                    heapArray[index] = leftChildVal;  
                      
                    siftDown(leftChildIndex);  
                }  
            }else{  
                //both children are available  
                int maxChildVal = heapArray[leftChildIndex];  
                int maxIndex = leftChildIndex;  
                if(heapArray[rightChildIndex]>maxChildVal){  
                    maxChildVal = heapArray[rightChildIndex];  
                    maxIndex = rightChildIndex;  
                }  
                if(val <=  maxChildVal) {  
                    //swap with maxIndex  
                    heapArray[index] = maxChildVal;  
                    heapArray[maxIndex] = val;  
                    siftDown(maxIndex);  
                }  
            }  
        }
        
        public void siftDown2(int index){
        	int leftChild = index * 2 + 1;
        	int rightChild = leftChild + 1;
        	
        	int child = -1;
        	if(leftChild > this.size) {
        		return;
        	} else if(leftChild < this.size){
        		child = leftChild;
        		if(heapArray[leftChild] < heapArray[rightChild]){
        			child = rightChild;
        		}
        	} else if(leftChild == this.size){
        		child = leftChild;
        	}
        	
        	if(heapArray[child] > heapArray[index]){
        		int tmp = heapArray[index];  
                heapArray[index] = heapArray[child];  
                heapArray[child] = tmp;
                siftDown2(child);
        	}
        }
    }  
    //78	66	13	20	6	9
    public static void main(String[] args) {  
        int[] items = {3,6,2,78,0,13,66,9,1,20,4};  
        int N = 6;  
        HeapTree heaptree = new HeapTree(N);  
        for(int i=0;i<items.length;i++){  
            heaptree.insert(items[i]);  
        }  
//        for(int i = 0;i<heaptree.heapArray.length;i++){
//        	System.out.print(heaptree.heapArray[i] + "\t");
//        }
//        heaptree.showTopN();  
    }  
}
