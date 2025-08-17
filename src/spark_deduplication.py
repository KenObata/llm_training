# spark_deduplication.py - Complete implementation for web-scale deduplication

# spark-submit --driver-memory 4g --executor-memory 4g src/spark_deduplication.py

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

def create_deduplication_spark_session() -> SparkSession:
    """Create Spark session optimized for deduplication"""
    
    spark = SparkSession.builder \
        .appName("WebScaleDeduplication") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def create_minhash_udf(num_hashes: int = 128, k: int = 9):
    """
    Create UDF for MinHash signature computation
    
    Args:
        num_hashes: Number of hash functions (signature size)
        k: Shingle size (9-10 recommended for web pages)
    """
    
    def compute_minhash_signature(text: str) -> List[int]:
        """Compute MinHash signature for text"""
        
        if not text or len(text) < k:
            return [0] * num_hashes
        
        # Create k-shingles
        shingles = set()
        text_lower = text.lower()  # Normalize to lowercase
        for i in range(len(text_lower) - k + 1):
            shingle = text_lower[i:i+k]
            shingles.add(shingle)
        
        if not shingles:
            return [0] * num_hashes
        
        # Compute MinHash signature
        signature = [float('inf')] * num_hashes
        
        for shingle in shingles:
            # Use different seeds for different hash functions
            for i in range(num_hashes):
                # MurmurHash3 with different seeds acts as different hash functions
                hash_val = mmh3.hash(shingle, seed=i, signed=False)
                signature[i] = hash_val if hash_val < signature[i] else signature[i]
        
        # Convert to integers, replacing infinity with 0
        return [int(h) if h != float('inf') else 0 for h in signature]
    
    # Return UDF with proper return type
    return udf(compute_minhash_signature, ArrayType(IntegerType()))

def create_lsh_bands_udf(num_bands: int = 16, rows_per_band: int = 8):
    """
    Create UDF for LSH band generation
    
    Args:
        num_bands: Number of bands for LSH
        rows_per_band: Number of MinHash values per band
    """
    
    def generate_bands(signature: List[int]) -> List[Tuple[int, int]]:
        """Generate LSH bands from MinHash signature"""
        
        if not signature:
            return []
        
        bands = []
        for band_id in range(num_bands):
            start_idx = band_id * rows_per_band
            if start_idx + rows_per_band < len(signature):
                end_idx = start_idx + rows_per_band
            else:
                end_idx = len(signature)
            # end_idx = min(start_idx + rows_per_band, len(signature))
            
            if start_idx >= len(signature):
                break
            
            # Create band hash from the subset of signature
            band_values = tuple(signature[start_idx:end_idx])
            # Use hashlib to avoid PySpark hash conflict
            band_hash = int(hashlib.md5(str(band_values).encode()).hexdigest()[:8], 16)
            
            bands.append((band_id, band_hash))
        
        return bands
    
    # Define the return schema for bands
    band_schema = ArrayType(
        StructType([
            StructField("band_id", IntegerType(), False),
            StructField("band_hash", LongType(), False)
        ])
    )
    
    return udf(generate_bands, band_schema)

def estimate_similarity_udf():
    """Create UDF to estimate Jaccard similarity from MinHash signatures"""
    
    def estimate_similarity(sig1: List[int], sig2: List[int]) -> float:
        """Estimate Jaccard similarity from two MinHash signatures"""
        
        if not sig1 or not sig2 or len(sig1) != len(sig2):
            return 0.0
        
        # Count matching MinHash values
        matches = builtin_sum(1 for h1, h2 in zip(sig1, sig2) if h1 == h2 and h1 != 0)
        
        # Avoid division by zero
        if all(h == 0 for h in sig1) or all(h == 0 for h in sig2):
            return 0.0
        
        # Estimated Jaccard similarity
        return float(matches) / len(sig1)
    
    return udf(estimate_similarity, FloatType())

def deduplicate_documents(spark: SparkSession, 
                         input_df, 
                         text_column: str = "text",
                         similarity_threshold: float = 0.8,
                         num_hashes: int = 128,
                         num_bands: int = 16) -> DataFrame:
    """
    Deduplicate documents using MinHash LSH
    
    Args:
        spark: SparkSession
        input_df: Input DataFrame with documents
        text_column: Name of the text column
        similarity_threshold: Threshold for considering documents as duplicates
        num_hashes: Number of MinHash functions
        num_bands: Number of LSH bands
    
    Returns:
        DataFrame with duplicates marked
    """
    
    print(f"Starting large-scale deduplication...")
    print(f"Parameters: threshold={similarity_threshold}, hashes={num_hashes}, bands={num_bands}")
    
    # Calculate rows per band
    rows_per_band = num_hashes // num_bands
    
    # Step 1: Create MinHash UDF and compute signatures
    print("Step 1: Computing MinHash signatures...")
    minhash_udf = create_minhash_udf(num_hashes=num_hashes, k=9)
    
    df_with_signatures = input_df.withColumn(
        "minhash_signature",
        minhash_udf(col(text_column))
    ).cache()  # Cache as we'll use this multiple times
    
    # Show sample signatures for debugging
    print("Sample signatures computed:")
    df_with_signatures.select("doc_id", slice(col("minhash_signature"), 1, 5)).show(3, truncate=False)
    
    # Step 2: Generate LSH bands
    print("Step 2: Generating LSH bands...")
    bands_udf = create_lsh_bands_udf(num_bands=num_bands, rows_per_band=rows_per_band)
    
    df_with_bands = df_with_signatures.withColumn(
        "bands",
        bands_udf(col("minhash_signature"))
    )
    
    # Step 3: Explode bands to find candidates
    print("Step 3: Finding candidate pairs...")
    df_exploded = df_with_bands.select(
        col("doc_id"),
        col("minhash_signature"),
        explode(col("bands")).alias("band")
    ).select(
        col("doc_id"),
        col("minhash_signature"),
        col("band.band_id").alias("band_id"),
        col("band.band_hash").alias("band_hash")
    )
    
    # Self-join to find documents that share at least one band
    candidates = df_exploded.alias("a").join(
        df_exploded.alias("b"),
        (col("a.band_id") == col("b.band_id")) & 
        (col("a.band_hash") == col("b.band_hash")) & 
        (col("a.doc_id") < col("b.doc_id"))  # Avoid duplicates and self-joins
    ).select(
        col("a.doc_id").alias("doc1"),
        col("b.doc_id").alias("doc2"),
        col("a.minhash_signature").alias("sig1"),
        col("b.minhash_signature").alias("sig2")
    ).distinct()
    
    print(f"Found {candidates.count()} candidate pairs")
    
    # Step 4: Compute actual similarity for candidates
    print("Step 4: Computing similarities for candidate pairs...")
    similarity_udf = estimate_similarity_udf()
    
    similar_pairs = candidates.withColumn(
        "similarity",
        similarity_udf(col("sig1"), col("sig2"))
    ).filter(
        col("similarity") >= similarity_threshold
    ).select("doc1", "doc2", "similarity")
    
    print(f"Found {similar_pairs.count()} similar pairs above threshold {similarity_threshold}")
    similar_pairs.show(truncate=False)
    
    # Step 5: Build connected components to group all related duplicates
    print("Step 5: Building duplicate groups using connected components...")
    
    # Create edges for GraphFrames (if available) or use custom approach
    edges = similar_pairs.select(
        col("doc1").alias("src"),
        col("doc2").alias("dst")
    )
    
    # Simple connected components using iterative approach
    # Initialize each document with its own group
    all_docs = input_df.select("doc_id").distinct()
    doc_groups = all_docs.withColumn("group_id", col("doc_id"))
    
    # Get all documents involved in duplicates
    docs_with_dups = edges.select("src").union(edges.select("dst")).distinct()
    
    # Assign group IDs (using the minimum doc_id in each connected component)
    # This is a simplified version - for production, use GraphFrames
    group_assignments = edges.groupBy("src").agg(
        collect_set("dst").alias("connected_docs")
    ).select(
        col("src").alias("doc_id"),
        array_union(array(col("src")), col("connected_docs")).alias("all_connected")
    ).select(
        col("doc_id"),
        array_min(col("all_connected")).alias("group_id")
    )
    
    # Step 6: Mark duplicates and representatives
    print("Step 6: Marking duplicate groups...")
    
    # Join back with original data
    result = input_df.join(
        group_assignments,
        on="doc_id",
        how="left"
    ).withColumn(
        "group_id",
        when(col("group_id").isNull(), col("doc_id")).otherwise(col("group_id"))
    ).withColumn(
        "is_duplicate",
        col("group_id") != col("doc_id")
    )
    
    # Add statistics
    dup_stats = result.groupBy("group_id").agg(
        count("*").alias("group_size"),
        collect_list("doc_id").alias("group_members")
    ).filter(col("group_size") > 1)
    
    print("\nDuplicate Groups Found:")
    dup_stats.show(truncate=False)
    
    # Add duplicate count
    total_docs = result.count()
    duplicate_docs = result.filter(col("is_duplicate")).count()
    unique_docs = total_docs - duplicate_docs
    
    print(f"\n=== Deduplication Summary ===")
    print(f"Total documents: {total_docs}")
    print(f"Duplicate documents: {duplicate_docs}")
    print(f"Unique documents: {unique_docs}")
    print(f"Deduplication rate: {duplicate_docs/total_docs*100:.2f}%")
    
    return result

def deduplicate_at_scale(spark: SparkSession,
                         input_path: str,
                         output_path: str,
                         similarity_threshold: float = 0.8):
    """
    Production-scale deduplication for large datasets
    
    Args:
        spark: SparkSession
        input_path: Path to input data (e.g., S3, HDFS)
        output_path: Path to save deduplicated data
        similarity_threshold: Similarity threshold for deduplication
    """
    
    print(f"Reading data from: {input_path}")
    
    # Read data based on format
    if input_path.endswith('.parquet'):
        df = spark.read.parquet(input_path)
    elif input_path.endswith('.json'):
        df = spark.read.json(input_path)
    else:
        # Assume text files
        df = spark.read.text(input_path).withColumn(
            "doc_id",
            monotonically_increasing_id()
        ).withColumnRenamed("value", "text")
    
    # Perform deduplication
    result = deduplicate_documents(
        spark=spark,
        input_df=df,
        text_column="text",
        similarity_threshold=similarity_threshold,
        num_hashes=128,
        num_bands=16
    )
    
    # Save results
    print(f"Saving deduplicated data to: {output_path}")
    
    # Save only unique documents
    unique_docs = result.filter(~col("is_duplicate"))
    unique_docs.write.mode("overwrite").parquet(output_path)
    
    # Save duplicate mapping separately
    dup_mapping_path = output_path.replace(".parquet", "_duplicates.parquet")
    result.filter(col("is_duplicate")).select(
        "doc_id", "group_id", "text"
    ).write.mode("overwrite").parquet(dup_mapping_path)
    
    print(f"Deduplication complete!")
    print(f"Unique documents saved to: {output_path}")
    print(f"Duplicate mappings saved to: {dup_mapping_path}")

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