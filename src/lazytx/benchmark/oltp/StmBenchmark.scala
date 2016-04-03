package lazytx.benchmark.oltp

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.stm.TMap
import scala.concurrent.stm.atomic
import lazytrie.test.Util

object StmBenchmarkHelper {
  def create(dbsize : Int) : TMap[Int,Int] = {
    val map = TMap[Int,Int]()
    
    var i = 0
    while(i < dbsize) {
      //if(i % 1000000 == 0)
      //  println(i)
      atomic { implicit txn =>
        map += (i -> ThreadLocalRandom.current.nextInt(1000000))
      }
      i += 1
    }
    map
  }
}

class StmRandomAccessBenchmark extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = StmBenchmarkHelper.create(dbsize)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        atomic { implicit txn =>
          var i = 0
          while(i < txsize) {
            state.put(rnd.nextInt(dbsize), rnd.nextInt(10))
            i += 1
          }
        }
      } else {
        atomic { implicit txn =>
          var i = 0
          var sum = 0
          while(i < txsize) {
            sum += state.get(rnd.nextInt(dbsize)).get
            i += 1
          }
        } 
      }
    }
  }
}

class StmSkewedAccessBenchmark(p : Double) extends RandomAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = StmBenchmarkHelper.create(dbsize)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      if(rnd.nextDouble() < writeRatio) {
        atomic { implicit txn =>
          var i = 0
          while(i < txsize) {
            state.put(Util.skewedRandom(dbsize, p), rnd.nextInt(10))
            i += 1
          }
        }
      } else {
        atomic { implicit txn =>
          var i = 0
          var sum = 0
          while(i < txsize) {
            sum += state.get(Util.skewedRandom(dbsize, p)).get
            i += 1
          }
        } 
      }
    }
  }
}
  
class StmSequentialAccessBenchmark extends SequentialAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = StmBenchmarkHelper.create(dbsize)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      val start = rnd.nextInt(dbsize - txsize)
      if(rnd.nextDouble() < writeRatio) {
        atomic { implicit txn =>
          var i = 0
          while(i < txsize) {
            state.put(start + i, rnd.nextInt(10))
            i += 1
          }
        }
      } else {
        atomic { implicit txn =>
          var i = 0
          var sum = 0
          while(i < txsize) {
            sum += state.get(start + i).get
            i += 1
          }
        } 
      }
    }
  }
}

class StmMixedWriteAccessBenchmark extends SequentialAccessBenchmark {
  def workload(dbsize : Int, txsize : Int, writeRatio : Double) = {
    val state = StmBenchmarkHelper.create(dbsize)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      val start = rnd.nextInt(dbsize - txsize)
      if(rnd.nextDouble() < writeRatio) {
        atomic { implicit txn =>
          var i = 0
          while(i < txsize) {
            state.put(start + i, rnd.nextInt(10))
            i += 1
          }
        }
      } else {
        atomic { implicit txn =>
          var i = 0
          while(i < txsize) {
            state.put(rnd.nextInt(dbsize), rnd.nextInt(10))
            i += 1
          }
        }
      }
    }
  }
}

class StmSequentialReadWriteBenchmark extends OptimisticBenchmark {
  def workload(dbsize : Int, readSize : Int, writeSize : Int) = {
    val state = StmBenchmarkHelper.create(dbsize)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      atomic { implicit txn =>
        var i = 0
        while(i < writeSize) {
          val start = rnd.nextInt(dbsize - readSize)
          
          var sum = 0
          var j = 0
          while(j < readSize) {
            sum += state.get(start + j).get
            j += 1
          }
          
          val k = rnd.nextInt(dbsize)
          state.put(k, sum)
          
          i += 1
        }
      }
    }
  }
}

class StmRandomReadWriteBenchmark extends OptimisticBenchmark {
  def workload(dbsize : Int, readSize : Int, writeSize : Int) = {
    val state = StmBenchmarkHelper.create(dbsize)
    
    () => {
      val rnd = ThreadLocalRandom.current()
      
      atomic { implicit txn =>
        var i = 0
        while(i < writeSize) {
          var sum = 0
          var j = 0
          while(j < readSize) {
            val k = rnd.nextInt(dbsize)
            sum += state.get(k).get
            j += 1
          }
          
          val k = rnd.nextInt(dbsize)
          state.put(k, sum)
          
          i += 1
        }
      }
    }
  }
}

class StmOptimisticBenchmark extends OptimisticBenchmark {
  def workload(dbsize : Int, readSize : Int, writeSize : Int) : () => Unit = {
    val state = StmBenchmarkHelper.create(dbsize)
    
    () => {
      atomic { implicit txn =>
        val rnd = ThreadLocalRandom.current()
        var sum = 0
      
        var i = 0
        while(i < readSize) {
          sum += state.get(rnd.nextInt(dbsize)).get
          i += 1
        }
        
        var j = 0
        while(j < writeSize) {
          val k = rnd.nextInt(dbsize)
          val v = rnd.nextInt(1000000)
          state.put(k, v)
          j += 1
        }
      }
    }
  }
}
