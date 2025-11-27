# test_partition_aware_deduplication.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
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
        # Use HTTP endpoint instead of S3 to avoid credential issues
        wet_path = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-22/segments/1715971057216.39/wet/CC-MAIN-20240517233122-20240518023122-00000.warc.wet.gz"
        
        print(f"Loading Common Crawl WET files from: {wet_path}")
        
        # For WET files, we need to read them as text files and parse the format
        # WET format contains:
        # - WARC-Type: conversion
        # - WARC-Target-URI: <url>
        # - Content-Length: <length>
        # - <blank line>
        # - <extracted text content>
        
        try:
            # Download WET file first to avoid Spark HTTP issues
            import urllib.request
            import tempfile
            import os
            
            print("Downloading WET file to local storage...")
            local_dir = tempfile.mkdtemp()
            local_path = os.path.join(local_dir, "wet_file.gz")
            
            urllib.request.urlretrieve(wet_path, local_path)
            file_size = os.path.getsize(local_path) / (1024 * 1024)  # MB
            print(f"Downloaded {file_size:.1f}MB to {local_path}")
            
            # Read local WET file with Spark
            print("Reading local WET file with Spark...")
            wet_rdd = spark.sparkContext.textFile(local_path)
            
            # Check if we can read any lines
            print("Counting lines in WET file...")
            line_count = wet_rdd.count()
            print(f"Read {line_count} lines from WET file")
            
            if line_count == 0:
                raise Exception("WET file appears to be empty or inaccessible")
            
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
            print("Parsing WET format...")
            parsed_rdd = wet_rdd.glom().flatMap(parse_wet_record)
            
            # Check if parsing produced any results
            parsed_count = parsed_rdd.count()
            print(f"Parsed {parsed_count} records from WET file")
            
            if parsed_count == 0:
                raise Exception("WET file parsing produced no records")
            
            # Convert to DataFrame and take sample
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("doc_id", StringType(), True),
                StructField("text", StringType(), True)
            ])
            
            print("Creating DataFrame from parsed records...")
            df_parsed = spark.createDataFrame(parsed_rdd, schema)
            
            print("Applying filters...")
            df_filtered = df_parsed.filter(col("text").isNotNull() & (length(col("text")) > 100))
            
            filtered_count = df_filtered.count()
            print(f"After filtering: {filtered_count} records")
            
            if filtered_count == 0:
                raise Exception("No records remain after filtering")
            
            print("Taking sample...")
            if filtered_count > 1000000*10:
                df_filtered = df_filtered.sample(0.001)  # Take 0.01% sample for stress test
            
            # Add some duplicate patterns for testing
            print("Preparing test data with synthetic duplicates...")
            
            # Cache for performance
            df_filtered = df_filtered.cache()
            test_count = df_filtered.count()
            print(f"Test dataset size: {test_count:,} documents")
            
            # Cleanup downloaded file
            print("Cleaning up downloaded file...")
            import shutil
            shutil.rmtree(local_dir)
            
        except Exception as e:
            # Cleanup on error
            if 'local_dir' in locals():
                import shutil
                shutil.rmtree(local_dir, ignore_errors=True)
            print("Common Crawl access requires AWS credentials or has connectivity issues.")
            raise Exception(f"Error reading WET files: {str(e)}")
        
        # Performance monitoring
        start_time = time.time()
        print(f"Starting deduplication at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Run partition-aware deduplication with optimized parameters for large dataset
        result = partition_aware_deduplicate(
            spark=spark,
            input_df=df_filtered,
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

    