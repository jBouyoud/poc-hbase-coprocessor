# poc-hbase-coprocessor

This POC aims to demonstrate how to secure hbase coprocessors by applying custom policies on it.

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

1. can break security configuration by bypass other coprocessors

1. can beak down the cluster in case of load failures  
    
## How to solve coprocessors "issues"
 
One of the common solution is to __write defensive code__.
But it's an heavy process (review, tests, etc.).

This table resume for each issue the state of the given solution :

|                       Problem                      | Solution         |
|:--------------------------------------------------:|:----------------:|
| can crash region servers.                          | DONE             |
| break down the cluster in case of bad request      | DONE             |
| hog a lot of memory/CPU                            | PARTIALLY DONE   |
| comes without metrics                              | DONE             |
| comes without process isolation                    | LIMITED          |
| can break coprocessors chains (bypass/complete)    | DONE             |
| can beak down the cluster in case of load failures | LIMITED          |

Those solutions are certainly not perfect but it's try to gives a pragmatic solution to those issues.

### How to apply custom policies

#### At compile time

Use your favorite design pattern : `proxy` to to besure that all methods call are wrapped through a policy verifier.

Pro : Easy, low overload
Cons: Intrusive, not possible on existing coprocessors, ne the library

### At runtime 

Use Java agent to enhance all hbase coprocessors host. 
For each instanciated coprocessors create a dynamic proxy witch applies policies. 

Pro : hard to implement, more important load time overhead
Cons: Hbase bytecode modification
 
### Implemented policies 

1. __can crash region servers__    
	Create a policy that catch Throwable and rethrow it as IOException (or derived ones).
	   
1. __can break down the cluster in case of bad request__  
	Create a policy that implements a retry limit (region server side) base on input queries.
   
1. __hog a lot of memory/CPU__   
	Create a policy that implemts a timeout logic.
	Create a policy that profile memory of execution at runtime.
	   
1. __comes without metrics__ 
 	Create a logger policy
 	Create a metrics policy based on hadoop metrics2.
      
1. __comes without process isolation__  
	Create a separate process that communicate with pipe.
	This solution is not really good because it breaks coprocessors deployment model.

1. __can break security configuration by bypass other coprocessors__  
	Create a policy that wrap ObserverContext and throw Exception when bypass and/or complete method are called.
	   
1. __can beak down the cluster in case of load failures__  
	Use hbase.coprocessor.aborterror = false
	This avoid to break the entire RegionServer only Table with incriminated coprocessors are unloaded.

### TODOs

- Add proxy for BulkLoadObserver, EndpointObserver
- Check/improve adaptation of multi coprocessor type (Master / Region, etc.) at Compile time
- Tests all coprocessors adapted methods
- Improve tests assertions
- Instanciates policies from configuration
- Implements an Hbase cluster wide fails cache (maybe based on an Hbase table?) 
- Run AgentTests in gradle (actually don't run them because they breaks down without policies test)
- Dynamic policies configuration (through zookeeper?)
- Configuration for a set of coprocessors
- Advanced benchmark
- Improve runtime bytecode weaving to intercept corpocessor class lading errors

## Setup

If you want to run integration tests outside gradle environment, 
you need to update `PATH` environment variable to add `workspace/developer/bin`.
```sh
$ PATH=$PATH;`workspace`/developer/bin
```
    
### Run tests
```sh
$ gradlew test
```

### Run 'real' test on Hortonwork Sandbox

1. Run : 
	```sh
	$ gradlew
	```
1. Copy `build/libs/poc-hbase-coprocessor-1.0.0-SNAPSHOT.jar` into the sandbox

1. Copy the jar file into `/usr/hdp/current/hbase-master/lib/` 
	and `/usr/hdp/current/hbase-regionserver/lib/`.
	
	```sh	
	$ cp poc-hbase-coprocessor-1.0.0-SNAPSHOT.jar /usr/hdp/current/hbase-master/lib/
	$ cp poc-hbase-coprocessor-1.0.0-SNAPSHOT.jar /usr/hdp/current/hbase-regionserver/lib/
	```
	
1. Go to `Ambari > Hbase> Configs > Advanced > Advanced hbase-env > hbase-env template`
	A the tail of config, add 
	```sh
	# Add Coprocessor policies agent
	export HBASE_REGIONSERVER_OPTS=" -javaagent:/usr/hdp/current/hbase-master/lib/poc-hbase-coprocessor-1.0.0-SNAPSHOT.jar $HBASE_REGIONSERVER_OPTS"
	export HBASE_MASTER_OPTS=" -javaagent:/usr/hdp/current/hbase-regionserver/lib/poc-hbase-coprocessor-1.0.0-SNAPSHOT.jar $HBASE_MASTER_OPTS "
	```
1. Add extra configuration (Custom hbase-site > Add property)

	Key   : hbase.coprocessors.policy.white-list
	Value : org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint,org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor,org.apache.hadoop.hbase.backup.master.BackupController
	
1. Restart Hbase
1. In hbase shell

	```sh
	$ disable 'table'
	$ alter 'table, 'coprocessor' => '|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'
	$ enable 'table'
	```
