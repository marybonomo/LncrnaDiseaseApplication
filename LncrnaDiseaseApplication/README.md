# LDA-LncRNADiseaseAssociation
## Build
There are two ways to build this project:
- manually build using Maven
- automated build using docker-compose

### Maven Build
The Maven build will create the jar file into `target/` folder.
```
mvn clean install
```
### Docker Build
The Docker build will create the jar file into `target/` folder.
```
docker-compose up
```
The docker script requires to setup the maven local repository, in order to avoid the download of the packages for each build. To this end, modify the line
```
    volumes:
    - ...
    - your_local_directory:/root/.m2 # TODO: change "your_local_directory" with the path to the local .m2 package repository
    command: ...
```
Generally, the .m2 folder is placed into `C://Users/<You>/.m2` or `/Users/<You>/.m2`.
## Execution
The execution of the jar via terminal is done with this command:
```
java -jar ./target/lda.jar <arguments>
```
There are several arguments that can be used. For more information use the argument `-h`. Using the `-h` argument will print the helper, ignoring the other arguments. If no arguments are specified, the helper will be shown automatically.

#### Example of execution with results save in specified output
An example of execution is:

```
java -jar ./target/lda.jar -f auc -m pvalue -o C:\\<user>\Documents\result_folder
```

will execute the software with model pValue and will save the results of predictions in the given folder. The output consists of a csv with header sorted alphabetically by couple lncRNA-disease.