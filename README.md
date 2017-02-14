# poc-hbase-coprocessor

This POC aims to demonstrate how to solve those coprocessors "issues".

It based on Hbase version : *1.2.3*.

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
  Client retry mecanism (this is good) can propagate a fail and consume resources for nothing
   
1. hog a lot of memory/CPU   
   Coprocessors are executed in the RegionServer JVM.
   
1. comes without metrics 
   There is no metrics on custom Coprocessors in the Hbase API
      
1. comes without process isolation   
   Coprocessors are executed in the RegionServer JVM.
   
1. can beak down the cluster in case of load failures  

1. can break security configuration by bypass other coprocessors
    
## How to solve coprocessors "issues"
 
One of the common solution is to __write defensive code__.
But it's an heavy process (review, test, etc.).

This table resume for each issue the state of the given solution :

|                       Problem                      | Manual Solution  | Automated Solution |
|:--------------------------------------------------:|:----------------:|:------------------:|
| can crash region servers.                          | TO DO            | TO DO              |
| break down the cluster in case of bad request      | TO DO            | TO DO              |
| hog a lot of memory/CPU                            | TO DO            | TO DO	             |
| comes without metrics                              | TO DO            | TO DO              |
| comes without process isolation                    | UNSOLVED         | UNSOLVED           |
| can beak down the cluster in case of load failures | TO DO            | UNSOLVED           |
| can break security configuration by bypass other coprocessors | TO DO | TO DO  			 |


Those solutions are not perfect but it's try to gives a pragmatic solution to those issues.

1. __can crash region servers__    
	Create an coprocessor adapter that catch Throwable and rethrow it as IOException.
	And then wraps this adapter into an Java Agent class transformer to automate it.
	   
1. __can break down the cluster in case of bad request__  
	Create a coprocessor adapter that implements a retry limit (region server side) base on input queries3
	And then wraps this adapter into an Java Agent class transformer to automate it.
   
1. __hog a lot of memory/CPU__   
	Used a runtime profiler for CPU/Memory.
	Create a coprocessor adapter that implements a request timeout logic and monitor Memory.
	And then wraps this adapter into an Java Agent class transformer to automate it.
   
1. __comes without metrics__  
	Create a coprocessor adapter that implements a metrics logic and write some generics information in log
	And then wraps this adapter into an Java Agent class transformer to automate it.
      
1. __comes without process isolation__  
	Create a separate process that communicate with pipe.
	This solution is not really good because it breaks coprocessors deployment model.
	   
1. __can beak down the cluster in case of load failures__  
	Use hbase.coprocessor.aborterror = false
	This avoid to break the entire RegionServer only Table with incriminated coprocessors are unloaded.

1. __can break security configuration by bypass other coprocessors__  
	Create a coprocessor adapter wrap ObserverContext and throw Exception when bypass and/or complete method are called.
	And then wraps this adapter into an Java Agent class transformer to automate it.

## Setup

If you want to run integration tests outside gradle environment, 
you need to update `PATH` environment variable to add `workspace/developer/bin`.
```shell 
$ PATH=$PATH;`workspace`/developer/bin
```
    
### Run tests
```shell 
$ gradlew test
```
