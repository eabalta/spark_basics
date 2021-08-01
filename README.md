## Spark Basics
This repo contains basic Spark code examples to learn Apache Spark. The source codes of the following contents are included in this project.
### Low Level RDD
1. **WordCount**
   * The simple project which counts the words.
2. **CreateRDD**
   * Creating RDD type in several ways 
3. **RDDBasicTransformations**
   * Transformation Methods
     * One RDD
       * _map_
         * It applies a function to each element and creates a new RDD as a result.
         * x iterates elementwise in iterative instance.
         >{1,2,3} --> map(x => x*x) --> {1,4,9}
       * _filter_
          * Filter takes a function and returns a new RDD generated with that function. The return type of the function which is given in the filter function must be boolean.
         >{1,2,3} --> filter(x => x > 2) --> {3}
       * _flatMap_
          * flatMap() method is identical to the map() method, but the only difference is that in flatMap the inner grouping of an item is removed and a sequence is generated.
         >{Enes,Alper} --> flatMap(_.toLowerCase) --> {e,n,e,s,a,l,p,e,r}
       * _distinct_
          * Returns each element only once
     * Two RDD
       * _union_
         * Concat two RDD's content and return one RDD
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
       * It's dangerous to use on large data
     * _count_
       * Counts elements of RDD then returns Long number
     * _countByValue_
       * Counts elements of RDD then returns each element and the count size in tuple
       >{1,2,2,2} --> countByValue() --> Map[Int,Long] = Map(1 -> 1 , 2 -> 3)
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
       >{1,2,2,2} --> reduce((x,y) => x+y) --> Int = 57

### Low Level PairRDD
* Generally use in aggregation operations
* Sometimes need preprocessing RDD to be PairRDD
* Main structure is Tuple.
* (key,value)
* Key-Value Examples
  * Who is the most commenting person?
  * What is the highest rated film?
  * What is average salary for each job?
1. **PairRDDBasicTransformations**
   * One RDD
     * reduceByKey
       * Aggregate values for same key. Returns new RDD which stores aggregated values and keys.
       >{(1,2),(3,4),(3,6)} --> reduceByKey((x,y) => x+y) --> {(1,2),(3,10)}
       * In this function, x and y iterates column by column.
         * Last two tuples have same key so in the first iteration;
           * x = 4 , y = 6 then x+y = 10
         * Iteration will be completed like this.
     * groupByKey
       * Groups for each key.
       >{(1,2),(3,4),(3,6)} --> groupByKey() --> {(1,(2)),(3,(4,6))}
     * mapValues
       * Apply function which is given as a parameter on only values.
       >{(1,2),(3,4),(3,6)} --> mapValues(x => x*100) --> {(1,200),(3,400),(3,600)}
     * keys
       * Returns RDD which stores only keys
       >{(1,2),(3,4),(3,6)} --> keys() --> {(1,3,3)}
     * values
       * Returns RDD which stores only values
       >{(1,2),(3,4),(3,6)} --> values() --> {(2,4,6)}
     * sortByKey
       * Sorts RDD by key then returns RDD.
   * Two RDD
     * subtractByKey
       * Subtracts tuples which have the same key.
       >RDD1 = {(1,2),(3,4),(3,6)} \
       RDD2 = {(3,9)}\
       RDD1.subtractByKey(RDD2) --> {(1,2)}
     * join
       * Must contain Primary Key - Foreign Key relation.
       >RDD1.join(RDD2) --> {((3,(4,9)),(3,(6,9)))}
     * rightOuterJoin
       >RDD1.rightOuterJoin(RDD2) --> {((3,(Some(4),9)),(3,(Some(6),9)))}
     * leftOuterJoin
       >RDD1.leftOuterJoin(RDD2) --> {(1,(2,None)),((3,(4,Some(9))),(3,(6,Some(9))))}

### Low Level Distributed Shared Variables
* Distributed Shared Variables are Broadcast Variables and Accumulators.
* Broadcast Variables are immutable, Accumulators are muteable.
* Accumulators are used to add up tasks' results. For example, counter.
* Broadcast Variables are used to create lookup table on data merging for decrease shuffle cost.
* **Broadcast Variables**
  * In the most used scenario is Lookup Table
  * It is always accessible in the Executor.
  * It created using _sc.broadcast()_
  * It accessed by _.value()_
* **Accumulators**
  * It is a variable used by sharing to reach aggregation results throughout the cluster.
  * The same job is handled by many tasks in many partitions, and a common variable is needed for the results.
  * The accumulator serves to update this variable between so many jobs and deliver this variable to the driver server in a fault-tolerant manner.