# Lazy Evaluation for Concurrent OLTP and Bulk Transactions
This repository hosts the prototype implementation and benchmarks for our IDEAS16 submission.

## Overview
The main parts of the implementation are the following:
* **laziness.Lazy**:		Classes for explicit lazy evaluation
* **lazytrie.Trie**:		Our lazy persistent trie
* **lazytrie.Map**:		A lazy key/value map, implemented using the lazy trie
* **lazytrie.Key**:		Classes for handling keys in the trie

## Benchmarks
The benchmarks can be found in the lazytx.benchmark package, where the main entry points are the following:
* **lazytx.benchmark.oltp.Ideas16**: The OLTP benchmarks
* **lazytx.benchmark.transform.Ideas16**: The bulk update benchmarks
* **lazytx.benchmark.memory.Ideas16**: Memory consumption benchmarks

To run the benchmarks and reproduce the results from the paper, import the project into [ScalaIDE](http://scala-ide.org/), and run the main method of the respective classes.