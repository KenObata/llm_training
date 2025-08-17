import hashlib
import random
import numpy as np
from typing import List, Set, Dict, Tuple
from collections import defaultdict
import mmh3  # MurmurHash3 for better performance

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinHashSignature:
    def __init__(self, num_hashes: int = 128, seed: int = 42):
        self.num_hashes = num_hashes
        self.seed = seed
        
        # TODO: Generate hash functions
        random.seed(seed)
        self.hash_functions = []
        
        for i in range(num_hashes):
            # TODO: Create hash function parameters
            a = random.randint(1, 2**31 - 1)
            b = random.randint(0, 2**31 - 1)
            self.hash_functions.append((a, b))  # (a, b)
    
    def create_shingles(self, text: str, ngram_size: int = 3) -> Set[str]:
        """Create k-shingles from text"""
        
        if len(text) < ngram_size:
            return {text}
        
        # TODO: Create character k-shingles
        shingles = set()
        for i in range(len(text) - ngram_size + 1):
            shingle = text[i:i+ngram_size]
            shingles.add(shingle)  # shingle
        
        return shingles
    
    def compute_signature(self, shingles: Set[str]) -> List[int]:
        """Compute MinHash signature for set of shingles"""
        
        if not shingles:
            return [0] * self.num_hashes
        
        # TODO: Initialize signature with infinity
        signature = [float('inf')] * self.num_hashes
        
        # TODO: For each shingle, update signature
        for shingle in shingles:
            shingle_hash = mmh3.hash(shingle, signed=False)
            logger.info(f"regular hash from mmh3 library, shingle: {shingle}, shingle_hash: {shingle_hash}")
            
            # TODO: Apply each hash function
            for i, (a, b) in enumerate(self.hash_functions):
                hash_value = (a * shingle_hash + b) % (2**31 - 1)
                signature[i] = min(signature[i], hash_value)  # hash_value
        
        return [int(h) if h != float('inf') else 0 for h in signature]
    
    def estimate_jaccard_similarity(self, sig1: List[int], sig2: List[int]) -> float:
        """Estimate Jaccard similarity from MinHash signatures"""
        
        if len(sig1) != len(sig2):
            raise ValueError("Signatures must have same length")
        
        # TODO: Count matching hash values
        matches = sum(1 for h1, h2 in zip(sig1, sig2) if h1 == h2)
        
        # TODO: Return estimated Jaccard similarity
        return matches / len(sig1)  # matches

class LSHIndex:
    def __init__(self, num_bands: int = 16, rows_per_band: int = 8):
        self.num_bands = num_bands
        self.rows_per_band = rows_per_band
        self.signature_length = num_bands * rows_per_band
        
        # TODO: Initialize band hash tables
        self.band_tables = [defaultdict(list) for _ in range(num_bands)]  # num_bands
    
    def add_signature(self, doc_id: str, signature: List[int]) -> None:
        """Add document signature to LSH index"""
        
        # TODO: Split signature into bands and add to hash tables
        for band_idx in range(self.num_bands):
            start_idx = band_idx * self.rows_per_band
            end_idx = start_idx + self.rows_per_band
            
            band = signature[start_idx:end_idx]
            band_hash = hash(tuple(band))
            
            self.band_tables[band_idx][band_hash].append(doc_id)  # doc_id
        logger.info(f"band_tables: {self.band_tables}")
    
    def find_candidates(self, signature: List[int]) -> Set[str]:
        """Find candidate similar documents"""
        
        candidates = set()
        
        # TODO: Check each band for matches
        for band_idx in range(self.num_bands):
            start_idx = band_idx * self.rows_per_band
            end_idx = start_idx + self.rows_per_band
            
            band = signature[start_idx:end_idx]
            band_hash = hash(tuple(band))
            
            bucket_docs = self.band_tables[band_idx].get(band_hash, [])
            candidates.update(bucket_docs)  # bucket_docs
        
        return candidates

if __name__ == "__main__":
    # TODO: Test MinHash and LSH implementation
    minhash = MinHashSignature(num_hashes=128)
    lsh_index = LSHIndex(num_bands=16, rows_per_band=8)
    
    # Test with sample documents
    test_docs = {
        "doc1": "The quick brown fox jumps over the lazy dog.",
        "doc2": "The quick brown fox jumps over the lazy dog!",  # Similar
        "doc3": "A completely different document about cats."
    }
    
    signatures = {}
    for doc_id, text in test_docs.items():
        logger.info(f"doc_id: {doc_id}")
        shingles = minhash.create_shingles(text, ngram_size=3)
        logger.info(f"shingles: {shingles}")

        signature = minhash.compute_signature(shingles)
        logger.info(f"signature: {signature}")

        signatures[doc_id] = signature
        lsh_index.add_signature(doc_id, signature)
    
    # Test similarity detection
    for doc_id, signature in signatures.items():
        candidates = lsh_index.find_candidates(signature)
        print(f"Candidates for {doc_id}: {candidates}")