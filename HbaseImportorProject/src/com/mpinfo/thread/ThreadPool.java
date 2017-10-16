package com.mpinfo.thread;

import java.util.concurrent.Semaphore;
import com.mpinfo.hadoop.hbase.*;

public final class ThreadPool
{
   public static final int MAX_AVAILABLE = 100;

   private Semaphore available = new Semaphore(MAX_AVAILABLE, true);
   private boolean[] used = new boolean[MAX_AVAILABLE];
   private HBaseOperator[] hbaseOperators = new HBaseOperator[10];

   public int obtainThread() throws InterruptedException
   {
      available.acquire();
      return getNextAvailableItem();
   }

   public HBaseOperator getHBaseOperator(int id) {
	   return hbaseOperators[id%10];
   }
   
   public void setHBaseOperator(int id, String hbaseUrl){
	   hbaseOperators[id%10] = new HBaseOperator(hbaseUrl);
   }

   public void releaseThread(int id)
   {
      if (markAsUnused(id))
         available.release();
   }

   private synchronized int getNextAvailableItem()
   {
      for (int i = 0; i < MAX_AVAILABLE; ++i)
      {
         if (!used[i])
         {
            used[i] = true;
            return i;
         }
      }
      return -1; // not reached
   }

   private synchronized boolean markAsUnused(int id){
	   
	   if (used[id]){
		   used[id] = false;
		   return true;
	   }else{
		   return false;
       }
   }
}