# How to develop this repo
## create virtual env
one-off
```
python3 -m venv venv
```

Every time
```
source venv/bin/activate
```
You should see (venv) after actuvation.
# Install PySpark locally
```
pip install -r requirements.txt
```
# Run Spark locally with 4GB RAM
```
spark-submit --driver-memory 4g --executor-memory 4g src/spark_deduplication_vanilla.py
```
- spark_deduplication.py - Complete implementation for web-scale deduplication
- common_crawl_explorer.py: PoC


# How to use this library
setup
```
pip install spark-llm-dedup
```

in codebase, first run vanila spark ml library's text-deduplication. 
This will OOM error out after TB of text documets.
```
from spark_llm_dedup import deduplicate_corpus
deduplicate_corpus("s3://common-crawl/", threshold=0.8)
```

Next, run partition aware text de-duplication
```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --executor-memory 16g \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=1000 \
  spark_partition_aware_deduplicattion_v2.py.py
```
# Or run locally for testing
```
python src/spark_partition_aware_deduplicattion_v2.py
```

# math behind

1.128 sampling called min hash
We take 128 samples of min hash where hash is based on N gram tokens and we take 128 based on different seeds.

2.bands/bucketing (= partition pruning )
We partition 128 samples into partitions called bands. This is based on reasoning that same signature falls into the same band/bucketing so we only need to compare hash in the same bucket.
With this, we don't need to brute force 128 sampels for doc against another doc.

WRONG Understanding:
"We only compare 8 values (one band) to determine similarity"

CORRECT Understanding:
"We use bands to quickly filter 10B × 10B pairs down to maybe 1M pairs,
 then we compare all 128 values for those 1M candidate pairs"

Note:
- Note that order is preserved since 128 samples have to be based on same seeds.
- When we compare 8 sample min hash per partition, we further create hash(tuple(8 sample min hash)).
    - By doing this, we can do full eact match and this is memory efficient than 8 string concatenation.

3.After at least one bucket matches among two doc, run full 128 sample min hash comparison.
This will compare x % match out of 128 samples, then if it exceeds user parameter of threashold %, 
these two docs are considered to be near duplicate.
