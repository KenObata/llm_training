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

from spark_utils import create_deduplication_spark_session

def create_minhash_udf(num_hashes: int = 128, k: int = 9) -> SparkSession.udf:
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
        print(f"shingles: {shingles}")
        # Compute MinHash signature. signature is a number of samples to test for similarity. We fetch min hash 128 times randomly and then compare two documents.
        signature = [float('inf')] * num_hashes 
        
        for shingle in shingles:
            # Use different seeds for different hash functions
            for i in range(num_hashes):
                # MurmurHash3 with different seeds acts as different hash functions
                # each sample should have different ranking randomization. But among documents, this seed is consistent.
                hash_val = mmh3.hash(shingle, seed=i, signed=False)

                # here we are updating the signature with the minimum hash value
                signature[i] = hash_val if hash_val < signature[i] else signature[i]
        
        # Convert to integers, replacing infinity with 0
        return [int(h) if h != float('inf') else 0 for h in signature]
    
    # Return UDF with proper return type
    return udf(compute_minhash_signature, ArrayType(IntegerType()))

def create_lsh_bands_udf(num_bands: int = 16, rows_per_band: int = 8) -> SparkSession.udf:
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

def estimate_similarity_udf() -> SparkSession.udf:
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
        return float(matches) / len(sig1) #  Both signatures ALWAYS have the same length
    
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
    print(f"input_df:") 
    input_df.show(10, truncate=False)
    print(f"Parameters: threshold={similarity_threshold}, hashes={num_hashes}, bands={num_bands}")
    
    # Calculate rows per band
    rows_per_band = num_hashes // num_bands
    
    # Step 1: Create MinHash UDF and compute signatures
    print("Step 1: Computing MinHash signatures...")
    minhash_udf = create_minhash_udf(num_hashes=num_hashes, k=9) # type is udf
    
    df_with_signatures = input_df.withColumn(
        "minhash_signature",
        minhash_udf(col(text_column))
    ).cache()  # Cache as we'll use this multiple times
    
    # Show sample signatures for debugging
    print("Sample three records signatures computed:")
    df_with_signatures.select("doc_id", slice(col("minhash_signature"), 1, 5), "text").show(3, truncate=False)
    
    # Step 2: Generate LSH bands
    print("Step 2: Generating LSH bands...")
    bands_udf = create_lsh_bands_udf(num_bands=num_bands, rows_per_band=rows_per_band)
    
    df_with_bands = df_with_signatures.withColumn(
        "bands",
        bands_udf(col("minhash_signature"))
    )
    
    print("sample df_with_bands records:")
    df_with_bands.select("doc_id", "bands").show(3, truncate=False)

    # Step 3: Explode bands to find candidates
    print("Step 3: Finding candidate pairs...")

    # df_with_bands's bands column is an array of tuples (band_id, band_hash). We explode it to get a row for each band.
    df_bands_array_exploded = df_with_bands.select(
        col("doc_id"),
        col("minhash_signature"),
        explode(col("bands")).alias("band")
    )
    print("df_bands_array_exploded records:")
    df_bands_array_exploded.select("doc_id", "band").show(10, truncate=False)

    df_exploded = df_bands_array_exploded.select(
        col("doc_id"),
        col("minhash_signature"),
        col("band.band_id").alias("band_id"),
        col("band.band_hash").alias("band_hash")
    )
    print("df_exploded records:")
    df_exploded.select("doc_id", "band_id", "band_hash").show(10, truncate=False)

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
    candidates.select("doc1", "doc2", "sig1", "sig2").show(truncate=True)
    
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
    print("edges records:")
    edges.show(10, truncate=False)
    # Simple connected components using iterative approach
    # Initialize each document with its own group
    all_docs = input_df.select("doc_id").distinct()
    doc_groups = all_docs.withColumn("group_id", col("doc_id"))
    print("Initialized doc_groups from all_docs records:")
    doc_groups.show(10, truncate=False)
    # Get all documents involved in duplicates
    docs_with_dups = edges.select("src").union(edges.select("dst")).distinct()
    print("docs_with_dups records:")
    docs_with_dups.show(10, truncate=False)
    # Assign group IDs (using the minimum doc_id in each connected component)
    # This is a simplified version - for production, use GraphFrames
    edges_group_by_src_df = edges.groupBy("src").agg(
        collect_set("dst").alias("connected_docs") # creates an array of all the documents that are connected to the source document
    )

    print("edges_group_by_src_df records:")
    edges_group_by_src_df.show(10, truncate=False)

    combine_src_and_connected_docs_df = edges_group_by_src_df.select(
        col("src").alias("doc_id"),
        array_union(array(col("src")), col("connected_docs")).alias("all_connected")
    )

    print("combine_src_and_connected_docs_df records:")
    combine_src_and_connected_docs_df.show(10, truncate=False)
    
    # create doc_id, representative doc_id for each duplicated doc.
    doc_id_and_representative_doc_id_df = combine_src_and_connected_docs_df.select(
        explode(col("all_connected")).alias("doc_id"),
        array_min(col("all_connected")).alias("representative_doc_id")
    )
    print("doc_id_and_representative_doc_id_df records:")
    doc_id_and_representative_doc_id_df.show(10, truncate=False)

    # doc_id_and_representative_doc_id_df contains duplicates.
    """
    ex) 
    combine_src_and_connected_docs_df records:
    +------+------------------+
    |doc_id|all_connected     |
    +------+------------------+
    |doc1  |[doc1, doc4, doc2]|
    |doc2  |[doc2, doc4]      |
    +------+------------------+

    doc_id_and_representative_doc_id_df records:
    +------+---------------------+
    |doc_id|representative_doc_id|
    +------+---------------------+
    |doc1  |doc1                 |
    |doc4  |doc1                 |
    |doc2  |doc1                 |
    |doc2  |doc2                 |
    |doc4  |doc2                 |
    +------+---------------------+

    This is because we explode the all_connected array and then select the representative document id.
    So now we need to do this:
    """
    # Create temporary view for SQL query
    doc_id_and_representative_doc_id_df.createOrReplaceTempView("doc_id_and_representative_doc_id_df")
    
    sql_command = """
    SELECT doc_id, MIN(representative_doc_id) as representative_doc_id
    FROM doc_id_and_representative_doc_id_df
    GROUP BY doc_id
    """
    doc_id_and_representative_doc_id_df_deduped = spark.sql(sql_command)
    print("doc_id_and_representative_doc_id_df_deduped:")
    doc_id_and_representative_doc_id_df_deduped.show(10)

    # group_id is representation of all duplicated documents. We only neeed one representative document for each group.
    print("INFO: group_id is representation of all duplicated documents." + 
    "We only neeed one representative document for each group.")

    # Step 6: Mark duplicates and representatives
    print("Step 6: Marking duplicate groups...")
    
    # Join back with original data
    input_df_with_group_id = input_df.join(
        doc_id_and_representative_doc_id_df_deduped,
        on="doc_id",
        how="left"
    ).withColumn(
        "representative_doc_id",
        when(col("representative_doc_id").isNull(), col("doc_id")).otherwise(col("representative_doc_id"))
    )

    print("input_df_with_group_id records:")
    input_df_with_group_id.show(10, truncate=False)
    
    result = input_df_with_group_id.withColumn(
        "is_duplicate",
        col("representative_doc_id") != col("doc_id")
    )
    print("result records:")
    result.show(10, truncate=False)
    
    # Add statistics
    dup_stats = result.groupBy("representative_doc_id").agg(
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


if __name__ == "__main__":
    # How to test
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
    result.filter(~col("is_duplicate")).select("doc_id", "text").show(10, truncate=False)
    # print("\n2. Testing with larger generated dataset...")
    # large_df = generate_test_data(spark, num_docs=100)
    # result_large = deduplicate_documents(spark, large_df, similarity_threshold=0.8)
    
    # Show final results
    print("\n=== Final Deduplicated Dataset ===")
    # result_large.filter(~col("is_duplicate")).select("doc_id", "text").show(10, truncate=False)
    """
    # For production use with file paths:
    # deduplicate_at_scale(
    #     spark=spark,
    #     input_path="s3://your-bucket/input/documents.parquet",
    #     output_path="s3://your-bucket/output/deduplicated.parquet",
    #     similarity_threshold=0.85
    # )
    """
    spark.stop()
    print("\nSpark session closed successfully!")
    