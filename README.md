Luzzu - A Quality Assessment Framework for Linked Open Datasets
===============================================================

Luzzu is a Quality Assessment Framework for Linked Open Datasets. It is a generic framework based on the Dataset Quality Ontology (daQ), allowing users to define their own quality metrics. Luzzu is an integrated platform that:
- assesses Linked Data quality using a library of generic and user-provided domain specific quality metrics in a scalable manner;
- provides queryable quality metadata on the assessed datasets;
- assembles detailed quality reports on assessed datasets.

Furthermore, the infrastructure:
- scales for the assessment of big datasets;
- can be easily extended by the users by creating their custom and domain-specific pluggable metrics, either by employing a novel declarative quality metric specification language or conventional imperative plugins;
- employs a comprehensive ontology framework for representing and exchanging all quality related information in the assessment workflow;
- implements quality-driven dataset ranking algorithms facilitating use-case driven discovery and retrieval.

More information regarding the framework can be found at our website (http://eis-bonn.github.io/Luzzu)

# Building
```mvn clean install```

# Executing the Application
```mvn exec:java -pl luzzu-communications```
 
You should now be able to navigate to [http://localhost:8080/Luzzu/application.wadl](http://localhost:8080/Luzzu/application.wadl) and view a simplified Web Application Description Language (WADL) descriptior for the application with user and core resources only.

To get full WADL with extended resources use the query parameter detail e.g [http://localhost:8080/Luzzu/application.wadl?detail=true](http://localhost:8080/Luzzu/application.wadl?detail=true)
