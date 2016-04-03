package lazytx.benchmark.oltp

import lazytrie.test.Util

object Main {
  var THREADS = 8
  val WRITE_RATIOS = Array(0.1, 0.9)
  val TXSIZES = Array(1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 524288)
  //val TXSIZES = Array(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288)
  
  def main(args : Array[String]) : Unit = {
    val time = 5000
    val dbsize = 1000000
    
    
    
    for(lw <- Array(0, 1, 2, 3, 4, 5)) {
      throughput(new LazyRandomAccessBenchmark(lw, 5, true), time, dbsize)
      throughput(new LazySequentialAccessBenchmark(lw, 5, true), time, dbsize)
    }
    
    /*
    for(skew <- Array(0.5, 0.9, 0.99, 0.999)) {
      println(skew)
      throughput(new LazySkewedAccessBenchmark(skew, 1, 5, true), time, dbsize)
      throughput(new StmSkewedAccessBenchmark(skew), time, dbsize)
    }
    */
    
    //throughput(new LazyRandomAccessBenchmark, time, dbsize)
    //throughput(new StmRandomAccessBenchmark, time, dbsize)
    
    //throughput(new LazySequentialAccessBenchmark, time, dbsize)
    
    
    //lazyRead(new LazyRandomReadBenchmark(), time, dbsize, 1)
    //lazyRead(new StmRandomReadWriteBenchmark(), time, dbsize, 1)
    
    //lazyRead(new LazyRandomReadBenchmark(), time, dbsize, 10)
    //lazyRead(new StmRandomReadWriteBenchmark(), time, dbsize, 10)
 
    //optimistic(new LazyOptimisticBenchmark(1), time, dbsize, 1)
    //optimistic(new StmOptimisticBenchmark(), time, dbsize, 1)
    
    //optimistic(new LazyOptimisticBenchmark(1), time, dbsize, 10)
    //optimistic(new StmOptimisticBenchmark(), time, dbsize, 10)
    
    
    //THREADS = 8
    //throughput(new LazySequentialAccessBenchmark, time, dbsize)
    
    //THREADS = 64
    //throughput(new LazyRandomAccessBenchmark, time, dbsize)
    
    
    //optimistic(new MapdbOptimisticBenchmark(), time, dbsize)
    
    //optimistic(new StmOptimisticBenchmark(), time, dbsize)
    
    //throughput(new LazyRandomAccessBenchmark, time, dbsize)
    
    /*
    speedup(new LazyRandomAccessBenchmark, time, dbsize)
    speedup(new LazySequentialAccessBenchmark, time, dbsize)
    */
    
    /*
    speedup2(new LazySequentialAccessReadOldBenchmark, time, dbsize)
    speedup2(new LazySequentialAccessBenchmark, time, dbsize)
    */
    
    /*
    for(dbsize <- Array(1000000, 10000000, 100000000)) {
      println("size: " + dbsize)
      speedup(new LazyRandomAccessBenchmark, time, dbsize)
      speedup(new LazySequentialAccessBenchmark, time, dbsize)
      //speedup(new StmRandomAccessBenchmark, time, dbsize)
      //speedup(new StmSequentialAccessBenchmark, time, dbsize)
    }
    */
  }
  
  def lazyRead(benchmark : OptimisticBenchmark, time : Int, dbsize : Int, readsPerWrite : Int) = {
    println(benchmark.getClass.getName())
    
    for(i <- 1 to 10) {
      Benchmark.benchmark(8, 1000, benchmark.workload(dbsize, 1, readsPerWrite))
    }
    
    for(txsize <- TXSIZES) {
      val txs = Benchmark.benchmark(THREADS, time, benchmark.workload(dbsize, readsPerWrite, txsize))
      print((txs * txsize * (readsPerWrite + 1)).toLong + " ")
    }
    println
  }
  
  def optimistic(benchmark : OptimisticBenchmark, time : Int, dbsize : Int, readsPerWrite : Int) = {
    println(benchmark.getClass.getName())
    
    Benchmark.benchmark(8, 10000, benchmark.workload(dbsize, 1000, 1000))
    
    for(txsize <- TXSIZES) {
      val txs = Benchmark.benchmark(THREADS, time, benchmark.workload(dbsize, readsPerWrite * txsize, txsize))
      print((txs * txsize * (readsPerWrite + 1)).toLong + " ")
    }
    println
  }
  
  def dbsizeScaling(benchmark : SimpleAccessBenchmark, time : Int) = {
    println(benchmark.getClass.getName())
    
    for(i <- 1 to 10) {
      Benchmark.benchmark(8, 1000, benchmark.workload(1000000, 256, 0.5))
    }
    
    for(dbsize <- Array(1000, 10000, 100000, 1000000, 10000000)) {
      println("db size: " + dbsize)
      for(writeRatio <- WRITE_RATIOS) {
        print("writeRatio " +  writeRatio + ": ")
        for(txsize <- TXSIZES) {
          val txs = Benchmark.benchmark(THREADS, time, benchmark.workload(dbsize, txsize, writeRatio))
          print((txs * txsize).toLong + " ")
        }
        println()
      }
    }
  }
  
  def throughput(benchmark : SimpleAccessBenchmark, time : Int, dbsize : Int) = {
    println(benchmark.getClass.getName())
    
    for(i <- 1 to 10) {
      Benchmark.benchmark(8, 1000, benchmark.workload(dbsize, 256, 0.5))
    }
    
    for(writeRatio <- WRITE_RATIOS) {
      print("writeRatio " +  writeRatio + ": ")
      for(txsize <- TXSIZES) {
        val txs = Benchmark.benchmark(THREADS, time, benchmark.workload(dbsize, txsize, writeRatio))
        print((txs * txsize).toLong + " ")
      }
      println()
    }
  }
  
  def speedup(benchmark : SimpleAccessBenchmark, time : Int, dbsize : Int) = {
    println(benchmark.getClass.getName())
    
    // warmup
    for(i <- 1 to 10) {
      Benchmark.benchmark(64, 1000, benchmark.workload(dbsize, 256, 0.5))
    }

    // speedup scaling over footprint
    println("speedup scaling, 8 threads")
    for(writeRatio <- WRITE_RATIOS) {
      print("write ratio " + writeRatio + ": ")
      for(txsize <- TXSIZES) {
        val t1 = Benchmark.benchmark(1, time, benchmark.workload(dbsize, txsize, writeRatio))
        val t64 = Benchmark.benchmark(THREADS, time, benchmark.workload(dbsize, txsize, writeRatio))
        
        print((t64 / t1) + " ")
      }
      println()
    }
  }
  
  def speedup2(benchmark : SimpleAccessBenchmark, time : Int, dbsize : Int) = {
    println(benchmark.getClass.getName())
    
    // warmup
    for(i <- 1 to 10) {
      Benchmark.benchmark(64, 1000, benchmark.workload(dbsize, 256, 0.5))
    }

    // speedup scaling over footprint
    println("speedup scaling")
    for(writeRatio <- WRITE_RATIOS) {
      println("write ratio " + writeRatio + ": ")
      for(txsize <- Array(1)) {
        print(txsize + ": ")
        for(conc <- Array(1, 2, 4, 8, 16, 32, 64, 128)) {
          val r = Benchmark.benchmark(conc, time, benchmark.workload(dbsize, txsize, writeRatio))
          print((r * txsize).toLong + " ")
        }
        println
      }
      println()
    }
  }
}
