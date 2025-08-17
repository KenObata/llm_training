# spark_deduplication.py - Complete implementation for web-scale deduplication

# spark-submit --driver-memory 4g --executor-memory 4g test/spark_deduplication_test.py

# Import Python's built-in functions before they get overwritten by PySpark
builtin_hash = hash
builtin_sum = sum

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import mmh3
import numpy as np
from typing import List, Tuple
from collections import defaultdict
import hashlib

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.spark_utils import create_deduplication_spark_session
from src.spark_deduplication import deduplicate_documents, deduplicate_at_scale


def generate_test_data(spark: SparkSession, num_docs: int = 1000) -> DataFrame:
    """Generate test data with known duplicates"""
    
    import random
    
    base_texts = [
        "Machine learning is a method of data analysis that automates analytical model building",
        "Deep learning is part of a broader family of machine learning methods based on artificial neural networks",
        "Natural language processing is a subfield of linguistics computer science and artificial intelligence",
        "Computer vision is an interdisciplinary scientific field that deals with how computers gain understanding from digital images",
        "Reinforcement learning is an area of machine learning concerned with how intelligent agents take actions",
    ]
    
    docs = []
    for i in range(num_docs):
        if random.random() < 0.3:  # 30% are near-duplicates
            base_text = random.choice(base_texts)
            # Add small modifications
            modifications = [
                "",
                "!",
                ".",
                " Really interesting",
                " (updated)",
                " - revised version",
            ]
            text = base_text + random.choice(modifications)
        else:
            # Generate unique text
            words = ["data", "science", "analysis", "model", "algorithm", "training",
                    "prediction", "classification", "regression", "clustering"]
            text = f"Document {i}: " + " ".join(random.choices(words, k=20))
        
        docs.append((f"doc_{i:04d}", text))
    
    return spark.createDataFrame(docs, ["doc_id", "text"])

if __name__ == "__main__":
    # Create Spark session
    spark = create_deduplication_spark_session()
    
    print("=" * 60)
    print("MinHash LSH Deduplication Demo")
    print("=" * 60)
    
    # Test with small sample data first
    print("\n1. Testing with provided sample data...")
    sample_data = [
        ("doc1", "The quick brown fox jumps over the lazy dog."),
        ("doc2", "The quick brown fox jumps over the lazy dog!"),  # Similar
        ("doc3", "A completely different document about cats and mice."),
        ("doc4", "The quick brown fox jumps over the lazy dog"),  # Very similar to doc1
        ("doc5", "Another different document with unique content here."),
        ("doc6", "The quick brown fox leaps over the lazy dog."),  # Slightly different
    ]
    
    df = spark.createDataFrame(sample_data, ["doc_id", "text"])
    result = deduplicate_documents(spark, df, similarity_threshold=0.7)
    
    print("\n2. Testing with larger generated dataset...")
    large_df = generate_test_data(spark, num_docs=100)
    result_large = deduplicate_documents(spark, large_df, similarity_threshold=0.8)
    
    # Show final results
    print("\n=== Final Deduplicated Dataset ===")
    result_large.filter(~col("is_duplicate")).select("doc_id", "text").show(10, truncate=False)
    
    # For production use with file paths:
    # deduplicate_at_scale(
    #     spark=spark,
    #     input_path="s3://your-bucket/input/documents.parquet",
    #     output_path="s3://your-bucket/output/deduplicated.parquet",
    #     similarity_threshold=0.85
    # )
    
    spark.stop()
    print("\nSpark session closed successfully!")