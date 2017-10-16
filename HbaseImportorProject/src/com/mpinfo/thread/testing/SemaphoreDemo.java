package com.mpinfo.thread.testing;

import java.util.concurrent.*;
import com.mpinfo.thread.*;

public class SemaphoreDemo
{
   public static void main(String[] args)
   {
      final ThreadPool pool = new ThreadPool();
      Runnable r = new Runnable()
      {
          @Override
                      public void run()
                      {
                         String name = Thread.currentThread().getName();
                         try
                         {
                            //while (true)
                            //{
                               int threadID;
                               System.out.printf("%s acquiring %s%n", name,
                            		   threadID = pool.obtainThread());
                               Thread.sleep(2000+(int)(Math.random()*1000));
                               System.out.printf("%s putting back %s%n", name, threadID);
                               pool.releaseThread(threadID);
                            //}
                         }
                         catch (InterruptedException ie)
                         {
                            System.out.printf("%s interrupted%n", name);
                         }
                      }
                   };
      ExecutorService[] executors = new ExecutorService[ThreadPool.MAX_AVAILABLE+1];
      for (int i = 0; i < 20; i++)
      {
         executors[i%5] = Executors.newSingleThreadExecutor();
         executors[i%5].execute(r);
      }
   }
}
