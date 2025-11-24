# create virtual env
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