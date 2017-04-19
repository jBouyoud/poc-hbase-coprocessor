# poc-hbase-coprocessor

This POC aims to demonstrate how to secure HBase coprocessors for industrial use (especially in multitenancy environment).

It based on HBase version : *1.2.3*.

## Introduction

Coprocessors is a very power full mechanism it allow us to call "Map-Reduce" 
tasks for "real-time" applications.

But coprocessors are dangerous for several reasons. 
This reasons are summarized as well in HBase Book :

> Coprocessors are an advanced feature of HBase and are intended to be used by system developers only

This term of `system developer` is an HBase team consensus for people that want's to open coprocessors to HBase users, 
and others that want's to keep Coprocessors for HBase developers.  
Thanks to @nkeywal fot this explanation.

Here we consider that coprocessors is use for business logic on top of HBase and they don't need to known HBase low level
 inner working, and often in a multitenancy environment.

## Coprocessors "issues"

Identified coprocessors issues are :

1. can crash region servers.   
   An exception (other than IOException) bring down region server.
   
1. can break down the cluster in case of bad request  
  Client retry mechanism (this is good) can propagate a fail and consume resources for nothing
   
1. hog a lot of memory/CPU   
   Long running and heavy memory consumption in HBAse JVM can slow down other HBase features
   
1. comes without metrics 
   There is no metrics on custom Coprocessors in the HBase API

1. can break security configuration by bypass other coprocessors

1. can beak down the cluster in case of load failures  
    
1. comes without process isolation   
   Coprocessors are executed in the RegionServer JVM.
   
1. API may changes on minor HBase version
	Interfaces are still in @InterfaceStability.Evolving state.
 
## How to solve coprocessors "issues"
 
One of the common solution is to __write defensive code__.
But it's an heavy process to setup in industry (review, tests, etc.).

The purpose of this project is to applies custom policies on HBase coprocessors, to fix up common identified issues.

This table bellow resume for each issue the state of the given solution :

|                       Problem                      | Solution             |
|:--------------------------------------------------:|:--------------------:|
| can crash region servers.                          | FOUND / IMPLEMENTED  | 
| break down the cluster in case of bad request      | FOUND / IMPLEMENTED  |
| hog a lot of memory/CPU                            | FOUND / PARTIALLY    |
| comes without metrics                              | FOUND / IMPLEMENTED  |
| can break coprocessors chains (bypass/complete)    | FOUND / IMPLEMENTED  |
| can beak down the cluster in case of load failures | FOUND / N/A          |
| comes without process isolation                    | FOUND / TO IMPLEMENT |
| API may changes on minor HBase version			 | N/A   				|

Those solutions are certainly not perfect but it's try to gives a pragmatic solution to those issues.

### How to apply custom policies

#### At compile time

Use your favorite design pattern : `proxy` to be sure that all methods call are wrapped through a policy verifier.

Pro : Easy, low overload
Cons: Intrusive, not possible on existing coprocessors, needs to be applies at compile time

### At runtime 

Use Java agent to enhance all HBase coprocessors hosts. 
For each instantiated coprocessors create a dynamic proxy witch applies policies. 

Pro : hard to implement, more important load time overhead
Cons: HBase bytecode modification

### Implemented policies 

1. __can crash region servers__    
	Create a policy that catch Throwable and rethrow it as IOException (or derived ones).
	   
1. __can break down the cluster in case of bad request__  
	Create a policy that implements a retry limit (region server side) base on input queries.
	TODO : Implements an HBase cluster wide fails cache (maybe based on an Hbase table?)
   
1. __hog a lot of memory/CPU__   
	Create a policy that implements a timeout logic.
	TODO : Create a policy that profile memory of execution at runtime.
	   
1. __comes without metrics__ 
 	Create a logger policy
 	Create a metrics policy based on hadoop metrics2.

1. __can break security configuration by bypass other coprocessors__  
	Create a policy that wrap ObserverContext and throw Exception when bypass and/or complete method are called.
	   
1. __can beak down the cluster in case of load failures__  
	Use hbase.coprocessor.aborterror = false
	This avoid to break the entire RegionServer only Table with incriminated coprocessors are unloaded.
      
1. __comes without process isolation__  
	TODO : Create a separate process that communicate with pipe with a demonized instance of the coprocessor in another PolicyVerifier.  
	This solution comes with a high overhead (+Security, -Complexity, -Performance).  
	This solution should be compatible with only CoprocessorService interface, 
    others interfaces contains non serializable fields).  
 	This solution should fixed an other HBase coprocessors issue in multitenancy environment which is : 
 	a coprocessor could access/modify other coprocessors in memory data.

1. __API may changes on minor HBase version__  
	I'm not really sure there is a real solution for that, just be sure to take care about before implementing a Coprocessor.
	You can note that endpoint coprocessors are not really impacted by this issue since his interface is based on Protobuf.
	
### TODOs

- Improve tests assertions
- Instantiates policies from configuration
- Dynamic policies configuration (through sighup see HBASE-14529?)
- Configuration for a set of coprocessors
- Advanced benchmark

#### For compile time solution
- Add proxy for BulkLoadObserver, EndpointObserver
- Check/improve adaptation of multi coprocessor type (Master / Region, etc.) at Compile time
- Tests all coprocessors adapted methods

#### For runtime solution
- Run AgentTests in gradle (actually don't run them because they breaks down without policies test)

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

### Run 'real' test on Hortonworks Sandbox

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
