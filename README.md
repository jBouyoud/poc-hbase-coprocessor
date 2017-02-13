# poc-hbase-coprocessor

This POC aims to demonstrate how to solve those coprocessors "issues".

It based on Hbase version : *1.2.1*.

## Introduction

Coprocessors is a very power full mecanism it allow us to call "Map-Reduce" 
tasks for "real-time" applications.

But coprocessors are dangerous for several reasons. 
This reasons are summerized as well in Hbase Book :

> Coprocessors are an advanced feature of HBase and are intended to be used by system developers only


## Coprocessors "issues"

Identified coprocessors issues are :

1. can crash region servers.   
   An exception (other than IOException) bring down region server.
   
1. can break down the cluster in case of bad request  
  Client retry mecanism (this is good) can propagate a fail
   
1. hog a lot of memory/CPU   
   Coprocessors are executed in the RegionServer JVM.
   
1. comes without metrics 
   There is no metrics on custom Coprocessors in the Hbase API
      
1. comes without process isolation   
   Coprocessors are executed in the RegionServer JVM.
   
1. can beak down the cluster in case of load failures  
    
## How to solve coprocessors "issues"
 
One of the common solution is to __write defensive code__.
But it's an heavy process (review, test, etc.).

This table resume for each issue the state of the given solution :

|                       Problem                      | Manual Solution | Automated Solution |
|:--------------------------------------------------:|:---------------:|:------------------:|
| can crash region servers.                          | SOLVED          | SOLVED             |
| break down the cluster in case of bad request      | SOLVED          | SOLVED             |
| hog a lot of memory/CPU                            | PARTILLY SOLVED | PARTILLYSOLVED     |
| comes without metrics                              | SOLVED          | SOLVED             |
| comes without process isolation                    | UNSOLVED        | UNSOLVED           |
| can beak down the cluster in case of load failures | PARTILLY SOLVED | UNSOLVED           |


Those solutions are not perfect but it's try to gives a pragmatic solution to those issues.

1. __can crash region servers__    
	Create an coprocessor adapter that catch Throwble and rethrow it as IOException.
	And then wraps this adapter into an Java Agent class transformer to automate it.
	   
1. __can break down the cluster in case of bad request__  
	Create a coprocessor adapter that implements a retry limit (region server side) base on input queries3
	And then wraps this adapter into an Java Agent class transformer to automate it.
   
1. __hog a lot of memory/CPU__   
	Used a runtime profiler for CPU/Memory.
	Create a coprocessor adapter that implements a request timeout logic and monitor Memory.
	And then wraps this adapter into an Java Agent class transformer to automate it.
   
1. __comes without metrics__  
	Create a coprocessor adapter that implements a metrics logic and write some generics information in log3
	And then wraps this adapter into an Java Agent class transformer to automate it.
      
1. __comes without process isolation__  
	Create a separate process that communicate with pipe.
	This solution is not really good because it breaks coprocessors deployment model.
	   
1. __can beak down the cluster in case of load failures__  
	Use hbase.coprocessor.aborterror = false
	This avoid to break the entire RegionServer only Table with incriminated coprocessors are unloaded.

## Setup

TODO
