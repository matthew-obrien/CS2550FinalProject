# CS2550FinalProject

There are three components in our project: a transaction manager, a scheduler, and a data manager.

## Compiling the project

To compile the project, please use the following command line:
      
```
javac -cp ".;jgraph-5.13.0.0.jar;jgraphx-2.0.0.1.jar;jgrapht-core-1.0.1.jar" myPTA.java
```

## Usage

To run the project, we can use three or four arguments. The first argument is used for specifying the script file directory. The second argument is used for specifying the table script directory. The third is used for specifying the buffer size. The fourth argument is optional, used for specifying the seed of transaction randomized selection. The following is an example:

```
java myPTA scripts tables 100
```

Depending on your classpath setup, you may need to run the compiled program with the -cp option as well, pointing to the included jars as before.
## Contributing

* **Matthew** has been working on the *transaction manager and scheduler*. 
-In the Transaction Manager, Matthew was responsible for implementing the round robin/random reading, waiting on the completion of prior operations, early abort of transactions, and the detection of workload necessary for switching between 2PL and OCC.
-Matthew also implemented all code for switching between the modes in each component.
-In scheduler, Matthew was responsible for implementing OCC.
-Matthew also developed the general framework of threads and shared data structures for communicating between them, together with Henrique.
* **Henrique** has been working on the *transaction manager and scheduler*.
-In the TM, Henrique was responsible for reading the operations from file and into operation queues.
-In the Scheduler, Henrique was responsible for implementing 2PL 
-As stated above, Henrique was also involved in developing the general framework for the threads and the shared data structure between them.
* **Zhenjiang** has been working on the *data manager*.
-Zhenjiang handled all major functionality of the DM.

Tasks were assigned ahead of time to be worked on, with occassional meetings. The majority of direct collaboration was performed through Slack.

## License


