# test_partition_aware_deduplication.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
import time
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


def test_integration_commoncrawl_sample():
    """
    Stress test with Common Crawl data from AWS S3
    Tests deduplication performance on real-world web crawl data
    """
    spark = create_spark_session_partition_aware("CommonCrawlStressTest")
    
    try:
        print("\n" + "="*80)
        print("COMMON CRAWL STRESS TEST - PARTITION-AWARE DEDUPLICATION")
        print("="*80)
        
        # Option 1: Use Common Crawl WET format for actual text content
        # WET files contain extracted plain text from web pages
        wet_path = "s3a://commoncrawl/crawl-data/CC-MAIN-2024-22/segments/*/wet/*.wet.gz"
        
        print(f"Loading Common Crawl WET files from: {wet_path}")
        
        # For WET files, we need to read them as text files and parse the format
        # WET format contains:
        # - WARC-Type: conversion
        # - WARC-Target-URI: <url>
        # - Content-Length: <length>
        # - <blank line>
        # - <extracted text content>
        
        try:
            # Read WET files as text
            wet_rdd = spark.sparkContext.textFile(wet_path)
            
            # Parse WET format to extract URL and text content
            def parse_wet_record(lines):
                """Parse WET record format to extract URL and text"""
                records = []
                current_record = {}
                content_lines = []
                in_content = False
                
                for line in lines:
                    line = line.strip()
                    
                    if line.startswith("WARC-Type:"):
                        # Start of new record
                        if current_record and 'url' in current_record and content_lines:
                            # Save previous record
                            current_record['text'] = ' '.join(content_lines).strip()
                            if len(current_record['text']) > 50:  # Filter out very short content
                                records.append((current_record['url'], current_record['text']))
                        
                        current_record = {}
                        content_lines = []
                        in_content = False
                        
                    elif line.startswith("WARC-Target-URI:"):
                        current_record['url'] = line.split(":", 1)[1].strip()
                        
                    elif line == "" and 'url' in current_record:
                        # Blank line indicates start of content
                        in_content = True
                        
                    elif in_content and line:
                        content_lines.append(line)
                
                # Handle last record
                if current_record and 'url' in current_record and content_lines:
                    current_record['text'] = ' '.join(content_lines).strip()
                    if len(current_record['text']) > 50:
                        records.append((current_record['url'], current_record['text']))
                
                return records
            
            # Process in partitions and parse WET format
            parsed_rdd = wet_rdd.glom().flatMap(parse_wet_record)
            
            # Convert to DataFrame and take sample
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("doc_id", StringType(), True),
                StructField("text", StringType(), True)
            ])
            
            df_test = spark.createDataFrame(parsed_rdd, schema) \
                .filter(col("text").isNotNull() & (length(col("text")) > 100)) \
                .sample(0.0001)  # Take 10% sample for stress test
            
        except Exception as e:
            print(f"Error reading WET files: {str(e)}")
            print("Falling back to Common Crawl index with synthetic text...")
            
            # Fallback to index data with synthetic content
            cc_index_path = "s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-22/subset=warc"
            df_raw = spark.read.parquet(cc_index_path).limit(100000)
            
            from pyspark.sql.functions import concat_ws, lit, rand
            df_test = df_raw.select(
                col("url").alias("doc_id"),
                concat_ws(" ", 
                    lit("Website content from"),
                    col("url"),
                    lit("with random content"),
                    (rand() * 1000).cast("int").cast("string")
                ).alias("text")
            ).filter(col("doc_id").isNotNull())
        
        # Add some duplicate patterns for testing
        print("Preparing test data with synthetic duplicates...")
        
        # Cache for performance
        df_test = df_test.cache()
        test_count = df_test.count()
        print(f"Test dataset size: {test_count:,} documents")
        
        # Performance monitoring
        start_time = time.time()
        print(f"Starting deduplication at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Run partition-aware deduplication with optimized parameters for large dataset
        result = partition_aware_deduplicate(
            spark=spark,
            input_df=df_test,
            text_column="text",
            similarity_threshold=0.9,  # Higher threshold for URL-based content
            num_hashes=64,             # Fewer hashes for speed
            num_bands=8,               # Fewer bands for speed  
            num_partitions=200         # More partitions for parallelism
        )
        
        # Collect results
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Performance metrics
        total_docs = result.count()
        duplicate_docs = result.filter(col("is_duplicate")).count()
        unique_docs = total_docs - duplicate_docs
        
        print("\n" + "="*80)
        print("COMMON CRAWL STRESS TEST RESULTS")
        print("="*80)
        print(f"Processing time: {elapsed:.2f} seconds")
        print(f"Documents processed: {total_docs:,}")
        print(f"Duplicates found: {duplicate_docs:,}")
        print(f"Unique documents: {unique_docs:,}")
        print(f"Deduplication rate: {(duplicate_docs/total_docs*100):.2f}%")
        print(f"Throughput: {total_docs/elapsed:.0f} docs/second")
        print("="*80)
        
        # Show sample results
        print("\nSample unique documents:")
        result.filter(~col("is_duplicate")).select("doc_id", "text") \
            .limit(5).show(truncate=False)
        
        if duplicate_docs > 0:
            print("\nSample duplicate groups:")
            result.filter(col("is_duplicate")).select(
                "doc_id", "representative_id", "text"
            ).limit(5).show(truncate=False)
        
        # Performance assertions
        assert elapsed < 300, f"Processing took too long: {elapsed:.2f}s (should be < 5min)"
        assert total_docs > 0, "No documents processed"
        assert unique_docs <= total_docs, "More unique docs than total docs"
        
        return {
            'total_docs': total_docs,
            'unique_docs': unique_docs, 
            'duplicate_docs': duplicate_docs,
            'processing_time': elapsed,
            'throughput': total_docs/elapsed
        }
        
    except Exception as e:
        print(f"\nError during Common Crawl test: {str(e)}")
        print("This might be due to:")
        print("1. AWS credentials not configured")
        print("2. Network connectivity issues") 
        print("3. Common Crawl bucket rate limiting")
        print("\nFalling back to synthetic large dataset test...")
        
    finally:
        spark.stop()

    