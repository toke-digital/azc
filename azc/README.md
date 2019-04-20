# AZC - Toke Digital's Azure Blobstore Client

This simple java client can interact with Microsoft Azure blobstores. It works with Ansible as a module as well. 

## Prereq - Azure Blobstore setup

*Disclaimer - I have no financial stake in Microsoft and this tool is not an attempt to shill their services. Also, using Azure has a financial aspect and it's not my fault if you screw up.*  

### Portal Login

https://azure.microsoft.com/en-au/features/azure-portal/


## Getting Started

Download the jar including dependencies from maven central:

```
<dependency>
  <groupId>digital.toke</groupId>
  <artifactId>azc</artifactId>
  <version>1.0.3</version>
</dependency>

```

or

```
curl https://repo1.maven.org/maven2/digital/toke/azc/1.0.3/azc-1.0.3-jar-with-dependencies.jar -o D:/azc/azc.jar

```

Create a configuration file like this, called something like azc-config.properties

```
 # start file<br/>
AZURE_STORAGE_ACCOUNT=xxxx
AZURE_STORAGE_ACCESS_KEY=xxxxx
CONTAINER_NAME=xxxx


 # if needed
USE_PROXY=true
HTTP_PROXY=proxy.example.com
HTTPS_PROXY=proxys.example.com
HTTP_PROXY_PORT=80
HTTPS_PROXY_PORT=443

 # end file
```


It is possible to pass in the three values below as environment variables instead of putting them in the config file:

```

export AZURE\_STORAGE\_ACCOUNT=xxxx<br/>
export AZURE\_STORAGE\_ACCESS_KEY=xxxx<br/>
export CONTAINER\_NAME=xxxx<br/>

```

### Prerequisites

Requires a Java 8 runtime.

## Examples

#### list all the files in the blobstore

```
java -jar azc.jar --config azc-config.properties --verb list
```
#### Get an existing file from out of the blobstore

```
java -jar azc.jar --config azc-config.properties --verb get --file myfile.zip --dest /tmp
```
#### Get a file in silent mode (no console output, useful for ansible)

```
java -jar azc.jar --config azc-config.properties --verb get --silent --file myfile.zip --dest /tmp
```

#### Send a file into the blobstore

```
java -jar azc.jar --config azc-config.properties --verb send --file myfile.zip
```

#### Get all the files and put them into /tmp

```
java -jar azc.jar --config azc-config.properties --verb getAll --dest /tmp
```







