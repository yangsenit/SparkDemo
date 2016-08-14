package com.ys.shuati;

/**
 * Created by Administrator on 2016/8/13.
 *
 * 实现的是堆排序
 *
 * 需要完成两个任务，
 * 任务一：建堆
 * 任务二：排序
 *
 */
public class HeapSort {
    public static int [] arr={1,4,0,3,2,7,6,9,8,5};
    public static void main(String [] args){
        create_heap();
        print_arr();
    }
    public static void create_heap(){
        int len=arr.length;
        for(int i=(len-1-1)/2;i>=0;i--){

            int left_chile_index=2*i+1;
            int right_chile_index=2*i+2;
            int temp=0;
            if (arr[i] < arr[left_chile_index] && left_chile_index<len) {
                temp=arr[i];
                arr[i]=arr[left_chile_index];
                arr[left_chile_index]=temp;
            }
            if (arr[i]<arr[right_chile_index] && right_chile_index<len) {
                temp=arr[i];
                arr[i]=arr[right_chile_index];
                arr[right_chile_index]=temp;
            }
        }
    }
    public static void heap_sort(){

    }
    public static void print_arr(){
        int len=arr.length;
        for(int i=0;i<len;i++){
            System.out.print(arr[i]+" ");
        }
        System.out.println();
    }
}
