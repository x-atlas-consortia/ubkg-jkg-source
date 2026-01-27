Readme for Canonical UMLS-JKG

The Directory with this Readme contains all artifacts necessary except for the Neptune UMLS data resource - and even includes every artifact needed and produced at every step of the UMLS-JKG and related tools validation process...

Here's what was done:

1. Run UMLS Extract notebook (**UMLS-JKG.ipynb**) - recommend one cell at a time to get familiar and consider what should be fully automated versus parameterized versus as notebook and how to use various parts for other JKG creations - an hour?

2. Run validation notebook with UMLS-JKG.json re-named JKG.json (and use JKG_Schema.json) - recommend one cell at a time to get familiar and consider what should be fully automated versus parameterized versus a notebook - timing seems to vary depending upon machine resources - an hour?

Note that validation will have the following warning: "The followingRel end id are not asserted as a node id" - see below - this may be is due to being concepts linked with MTH foreign concepts in English MRRELs - so they have no pref_term in MRCONSO, so they are not created as nodes, but still they appear in MRREL subsetted to ENG SABs and are created as relationships  - I wrote to UMLS to correct this exact list - for now, fortunately, these fail silently by the ingest excluding the relationships because they don't match node ids, which is fine given that the validation states they occur and then having the ingest skip them as not matching - perhaps validation could say that non-matching nodes are skipped on relationship ingests in some documentation
The following Rel end id are not asserted as a node id:
141160     UMLS:C0949778
1428308    UMLS:C5234793
1612469    UMLS:C4082610
2436150    UMLS:C0949821
3335648    UMLS:C4300557
3812654    UMLS:C4082319
4603689    UMLS:C0031324
4635977    UMLS:C0949906
5333716    UMLS:C2825126
6198437    UMLS:C3179396
7507312    UMLS:C0949835
7544827    UMLS:C4082410
8465206    UMLS:C0949781
9163829    UMLS:C2825126
9448240    UMLS:C2348859
9616278    UMLS:C0949777
Name: properties.id, dtype: object

3. If ten million nodes and rels which is true for UMLS, recommended to run JKG extract to batch files for import - no need to use if a JKG.json is only a  million lines -then can just use original script:

4. Get JKG_Batch directory from Batch script output

5. From Brand new Neo4j download, the following setup features were used:

Instantiate apoc by copying apoc jar from labs to plugins and getting extended apoc from neo4j online GitHub and placing in plugins also

In conf/neo4j.conf:
server.memory.heap.initial_size=16g
server.memory.heap.max_size=16g
server.memory.pagecache.size=16g
dbms.memory.transaction.total.max=12g

Create conf/apoc.conf:
apoc.export.file.enabled=true
apoc.import.file.enabled=true


6. Move JKG file or in this case the Batch Directory to import directory

7. Start neo4j - and run the following indexes 

CREATE CONSTRAINT FOR (n:Source) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Term) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Concept) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Rel_Label) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Node_Label) REQUIRE n.id IS UNIQUE;


8. Run this small JKG.json load OR in this case the batch below - NOT both - UMLS needs the batch load (too big for straight JKG.json load)

NOT 8a. Reference Implementation (unchanged) for "small" JKG.json (can handle million objects not 10 Million)
// Import source_in.json (from file)
// Source - https://stackoverflow.com/a
// Posted by Benoit Verhaeghe
// Retrieved 2025-12-05, License - CC BY-SA 4.0
// modified by JCS to enable apoc.export.json.all to regenerate re-importable content
CALL apoc.load.json('file:///JKG.json') YIELD value
WITH value.nodes AS nodes, value.rels AS rels
UNWIND nodes AS n
CALL apoc.create.node(n.labels, apoc.map.setKey(n.properties, 'id', n.properties.id)) YIELD node
WITH rels, apoc.map.fromPairs(COLLECT([n.properties.id, node])) AS node_map
UNWIND rels AS r
WITH r, node_map[r.start.properties.id] AS start, node_map[r.end.properties.id] AS end
CALL apoc.create.relationship(start, r.label, r.properties, end) YIELD rel
RETURN null


8b. Run each of these three batch loads separately sequentially - total under 15 min for 3 loads (1 min, 7 min, 6 min)

CALL apoc.load.directory("n*JKG.json", "JKG_Batch") YIELD value as files
UNWIND files as file
CALL apoc.periodic.iterate(
'CALL apoc.load.jsonArray($file, "$.nodes") YIELD value as n RETURN n',
'CREATE (node:$(n.labels)) SET node = n.properties',
{batchSize: 1000, parallel: true, params: {file: file}})
YIELD batches, total, committedOperations RETURN batches, total, committedOperations

CALL apoc.load.directory("r*JKG.json", "JKG_Batch") YIELD value as files
UNWIND files as file
CALL apoc.periodic.iterate(
'CALL apoc.load.jsonArray($file, "$.rels") YIELD value as r RETURN r',
'MATCH (start:Concept|Node_Label{id:r.start.properties.id}) MATCH (end:Concept|Node_Label{id:r.end.properties.id}) CREATE (start)-[rel:$(r.label)]->(end) SET rel = r.properties',
{batchSize: 1000, parallel: false, params: {file: file}})
YIELD batches, total, committedOperations RETURN batches, total, committedOperations

CALL apoc.load.directory("cr*JKG.json", "JKG_Batch") YIELD value as files
UNWIND files as file
CALL apoc.periodic.iterate(
'CALL apoc.load.jsonArray($file, "$.rels") YIELD value as r RETURN r',
'MATCH (start:Concept{id:r.start.properties.id}) MATCH (end:Term{id:r.end.properties.id}) CREATE (start)-[rel:$(r.label)]->(end) SET rel = r.properties',
{batchSize: 1000, parallel: false, params: {file: file}})
YIELD batches, total, committedOperations RETURN batches, total, committedOperations


9. Export all as JSON (to source_out.json file - 10 min export 

CALL apoc.export.json.all("source_out.json", {jsonFormat: 'JSON', stream: false, writeNodeProperties: true})


10. then process with JKG_Convert_Neo4joutput notebook back to source_in.json -
Source out is then back to source_in and then ....

11. batched and then re-ingested a second time to a fresh neo4j again to be sure two cycles are the same - they are!


Other interesting thoughts:

MATCH ()-[r:CODE{codeid:"MTH:NOCODE"}]->() return count(r)

Result: 259440

MATCH (a)-[r:CODE{codeid:"MTH:NOCODE"}]->(b) with a, b
CALL {
WITH a, b
MATCH (a)-[c:CODE]->(b) where c.codeid <> "MTH:NOCODE" return c limit 1
}
with c return count(c)

Result: 224247

86% have other codes but 14% do not - over 200,000 "wasted" codes? - but 35,000 rather useful ones -> leave them all in - could alternately query for and delete the irrelevant ones in cypher post-load given the large number...not!

Also, considerations of other indexes that could be appropriate but so far haven't needed others than the id ones above.
