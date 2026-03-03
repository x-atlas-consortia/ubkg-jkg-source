# Universal Biomedical Knowledge Graph - JSON Knowledge Graph format (UBKG-JKG)
## UMLS to JKG Converter

# Background
## UBKG
UBKG is a framework for building and distributing knowledge graphs of assertions obtained 
from multiple biomedical data sources. The UBKG extends the _concept-code synonymy_ of the UMLS 
(where a single _concept_ can be represented by _codes_ in multiple vocabularies), combining UMLS
concept data with information from other sources. For a detailed explanation of the 
UBKG, consult the [UBKG documentation](https://ubkg.docs.xconsortia.org/).

The initial release of the UBKG was a knowledge graph with a schema that adhered closely to the UMLS architecture. 
In particular, almost all properties of concepts and codes (representations of concepts in vocabularies), including
terms and definitions, were nodes in the UBKG. Although this schema supported knowledge graph analysis, it
was complex and bound both to the UMLS architecture and the neo4j platform. 

## JKG
The [JSON Knowledge Graph](https://github.com/x-atlas-consortia/json-knowledge-graph), or JKG schema supports a simple,
platform-agnostic structure for a knowledge graph, as well as specification for transferring data between knowledge graphs.
The JKG schema consists of two types of arrays:
- **nodes**, with elements for 
  - sources (e.g., vocabularies)
  - semantic types
  - relationships
  - concepts
  - terms
- **rels**, with elements that describe relationships
  - between semantic types (i.e., that describes a semantic type hierarchy)
  - between concepts and concepts
  - between concepts and codes

## umls2jkg
Applications in this repository convert source data obtained from the UMLS to a JSON file that
conforms to the JKG schema.

### Architecture
![img.png](img.png)

#### Components
1. **umls2jkg.py**:  Python application that:
   - reads UMLS data files
   - transforms UMLS data to JKG format
   - writes to output
2. **umls2jkg.sh**:  Shell script that:
   - establishes a Python virtual environment for the **umls2kg.py** script
   - executes the **umls2kg.py** script

3. **umls2kjg.ini**: configuration file that drives the execution of the **umls2kg.py** script

# Prerequisites
## Metathesaurus and Semantic Network files

### MetamorphoSys configuration
## Machine characteristics
### Operating System
### Memory
### Disk space
# Application configuration
## Software
### Python
### Packages
## umls2jkg.ini
# Application workflow
# File processing and timing
## Scanning and reading
## Cleaning
## Completion
# Output
# Container deployment


