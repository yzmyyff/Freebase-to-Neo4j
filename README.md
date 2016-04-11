
# Freebase to Neo4j(Version 2.3.3) Graph Database Loader

## Freebase dump used is based on: 
            https://github.com/percyliang/sempre

It is a canonicalized and scaled down dump. It's format like **freebase/data/tutorial.ttl** in sempre project. You must remove all the blank lines in triple tuple file.

### Acquire Full Freebase ttl

```Shell
        ./pull-dependencies fullfreebase-ttl
```
            
## Easy setup

1. Clone the GitHub repository:

            git clone https://github.com/yzmyyff/Freebase-to-Neo4j.git

2. Compile the source code (this produces `target/neo4j-0.0.1-SNAPSHOT-jar-with-dependencies.jar`):

            mvn package

3. Run Python code to generate nodes file and edges file. 
4. Run jar to store nodes and edges files into Neo4j db.