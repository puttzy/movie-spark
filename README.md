# MovieLens Apahce Spark Java Playground
---
## Overivew

The purpose of this repository is to give Java Developers and Data Scientists alike an example of operations that may be performed using the Apache Spark Java libraries.

## Topics Covered

1. Dataset loading
2. Querying
3. Statistical summation
  * Variance
  * Maximums
  * Minimums
  * Mean (average)
4. Statistical Correlation
   * Pearson's Methd
5. Filtering Datasets

## Loading Datasets

The [MovieLens 20M dataset](https://grouplens.org/datasets/movielens/) is the default dataset used within this repository. Download the .csv files, and place them within the `src/main/resources` directory. **Please do not commit dataset files** they are too large for GitHub.

## Running

The easiest way to run this application is to download [IntelliJ](https://www.jetbrains.com/idea/) and choose **Import from exisiting sources/Maven**. Then, hit the Run button (green play button.)

## Dependencies
You'll need to have the [apache hadoop 2.9.2 library](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz) on your path
> export path/to/hadoop/directory