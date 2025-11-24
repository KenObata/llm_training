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
spark-submit --driver-memory 4g --executor-memory 4g src/spark_deduplication.py
```
- spark_deduplication.py - Complete implementation for web-scale deduplication
- common_crawl_explorer.py: PoC


# How to use this library
setup
```
pip install spark-llm-dedup
```

in codebase
```
from spark_llm_dedup import deduplicate_corpus
deduplicate_corpus("s3://common-crawl/", threshold=0.8)
```