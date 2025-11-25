# test_partition_aware_deduplication.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
from src.spark_partition_aware_deduplicattion_v2 import (
    compute_minhash_signature,
    estimate_similarity,
    partition_aware_deduplicate
)
from src.spark_utils import create_spark_session_partition_aware

"""
@pytest.fixture(scope="session")
def spark():
    
    spark = SparkSession.builder \
        .appName("TestDeduplication") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    yield spark

    spark.stop()
"""
def test_integration_small_samples():
    """
    Main execution function
    """
    # Create Spark session
    spark = create_spark_session_partition_aware()
    
    print("="*60)
    print("PARTITION-AWARE MINHASH LSH DEDUPLICATION")
    print("="*60)
    
    # Test with sample data
    print("\nTesting with sample data...")
    
    sample_data = [
        ("doc1", "The quick brown fox jumps over the lazy dog."),
        ("doc2", "The quick brown fox jumps over the lazy dog!"),
        ("doc3", "A completely different document about cats."),
        ("doc4", "The quick brown fox jumps over a lazy dog."),
        ("doc5", "Another unique document with different content."),
        ("doc6", "The quick brown fox leaps over the lazy dog."),
        ("doc7", "Yet another document about something else entirely."),
        ("doc8", "The quick brown fox jumps over the lazy dog"),
    ]
    
    df = spark.createDataFrame(sample_data, ["doc_id", "text"])
    
    # Run partition-aware deduplication
    result = partition_aware_deduplicate(
        spark=spark,
        input_df=df,
        text_column="text",
        similarity_threshold=0.7,
        num_hashes=128,
        num_bands=16,
        num_partitions=10
    )
    
    print("\nUnique documents after deduplication:")
    result.filter(~col("is_duplicate")).select("doc_id", "text").show(truncate=False)
    
    print("\nDuplicate groups found:")
    result.filter(col("is_duplicate")).select(
        "doc_id", "representative_id", "text"
    ).show(truncate=False)
    
    # Run performance comparison 
    # ToDo: uncomment after v1 ready.
    # compare_with_vanilla(spark, test_size=1000)
    
    # Assert - expect between 4-6 unique documents after deduplication  
    unique_count = result.filter(~col("is_duplicate")).count()
    total_count = result.count()
    
    assert total_count == len(sample_data), f"Expected {len(sample_data)} total documents, got {total_count}"
    assert 4 <= unique_count <= 6, f"Expected 4-6 unique documents, got {unique_count}" 
    # Clean up
    spark.stop()
    print("\nSpark session closed successfully!")


@pytest.mark.skip(reason="Temporarily skipping large dataset test")
def test_integration_large_dataset():
    # Load sample data
    df = spark.read.parquet("s3://your-bucket/commoncrawl-1gb-sample.parquet")
    print(f"Original count: {df.count()}")

    # Deduplicate
    dedup = SparkLLMDeduplicator(spark, similarity_threshold=0.8)
    start = time.time()
    result = dedup.deduplicate(df, text_col="content", id_col="url")
    dedup_count = result.count()
    elapsed = time.time() - start
    

    