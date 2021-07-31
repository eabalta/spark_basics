## Spark Basics
This repo contains basic Spark code examples to learn Apache Spark. The source codes of the following contents are included in this project.
### Low Level RDD
1. **WordCount**
   * The simple project which counts the words.
2. **CreateRDD**
   * Creating RDD type in several different ways 
3. **RDDBasicTransformations**
   * Transformation Methods
     * One RDD
       * _map_
         * It applies a function to each element and creates a new RDD as a result.
         * x iterates elementwise in iterative instance.
         * {1,2,3} --> map(x => x*x) --> {1,4,9}
       * _filter_
          * Filter takes a function and returns a new RDD generated with that function. The return type of the function which is given in the filter function must be boolean.
          * {1,2,3} --> filter(x => x > 2) --> {3}
       * _flatMap_
          * flatMap() method is identical to the map() method, but the only difference is that in flatMap the inner grouping of an item is removed and a sequence is generated.
          * {Enes,Alper} --> flatMap(_.toLowerCase) --> {e,n,e,s,a,l,p,e,r}
       * _distinct_
          * Returns each element only once
     * Two RDD
       * _union_
         * Concat two RDD's concent and return one RDD
       * _intersection_
         * Returns intersection of two RDDs in a RDD
       * _subtract_
         * Subtracts the content of one RDD from the content of another RDD then returns one RDD
       * _cartesian_
         * Returns cartesian product of two different RDD
4. **RDDBasicActions**
   * Action Methods
     * _collect_
       * Returns the content of the RDD to driver.
       * Its dangerous to use on large data
     * _count_
       * Counts elements of RDD then returns Long number
     * _countByValue_
       * Counts elements of RDD then returns each element and the count size in tuple
       * {1,2,2,2} --> countByValue() --> Map[Int,Long] = Map(1 -> 1 , 2 -> 3)
     * _take_
       * Returns the number of elements given as a parameter
       * It takes randomly
     * _top_
       * It returns the largest elements in the RDD as many as the number entered as a parameter.
     * _takeOrdered_
       * It orders the RDD then return the number of elements given as a parameter
     * _takeSample_
       * Creates a sample which is sample size given as a parameter
     * _reduce_
       * It produces a result by applying the function given as a parameter on the RDD elements in parallel.
       * {1,2,2,2} --> reduce((x,y) => x+y) --> Int = 57
### PairRDD