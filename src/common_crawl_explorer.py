# common_crawl_explorer.py - Modern implementation focused on WET files for LLM training
import boto3
import botocore
import gzip
import requests
import json
from urllib.parse import urlparse
from typing import List, Dict, Iterator, Optional, Tuple
import re
from dataclasses import dataclass
from io import BytesIO
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class WETRecord:
    """Represents a single WET record (text content from a webpage)"""
    url: str
    content: str
    content_length: int
    timestamp: str
    language: str = "unknown"
    
@dataclass
class CrawlInfo:
    """Information about a Common Crawl dataset"""
    crawl_id: str
    timestamp: str
    segment_count: int
    
class CommonCrawlExplorer:
    """
    Modern Common Crawl explorer optimized for WET files (text data for LLM training)
    Supports both AWS S3 access and HTTP access via CloudFront
    """
    
    def __init__(self, use_aws_auth: bool = False, aws_access_key: str = None, aws_secret_key: str = None):
        """
        Initialize the explorer with optional AWS authentication
        
        Args:
            use_aws_auth: If True, use authenticated S3 access (faster from EC2)
            aws_access_key: Optional AWS access key
            aws_secret_key: Optional AWS secret key
        """
        self.bucket = 'commoncrawl'
        self.http_base_url = 'https://data.commoncrawl.org'
        self.use_aws_auth = use_aws_auth
        
        if use_aws_auth:
            # Authenticated access (required for S3 API as of 2024)
            if aws_access_key and aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name='us-east-1'  # Common Crawl is in us-east-1
                )
                logger.info("aws_access_key and aws_secret_key provided.")
            else:
                # Use IAM role credentials (for EC2 instances)
                self.s3_client = boto3.client('s3', region_name='us-east-1')
                logger.info("Using IAM role credentials.")
        else:
            # Anonymous access (deprecated but may still work)
            self.s3_client = boto3.client(
                's3', 
                region_name='us-east-1',
                config=botocore.client.Config(signature_version=botocore.UNSIGNED)
            )
            logger.info("Using anonymous access.")

    
    def get_available_crawls(self) -> List[CrawlInfo]:
        """
        Get list of available Common Crawl datasets
        Returns most recent crawls first
        """
        print("Fetching available crawls...")
        
        try:
            if self.use_aws_auth:
                logger.info("Using S3 API to list crawls")
                # Use S3 API to list crawls
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix='crawl-data/',
                    Delimiter='/'
                )
                logger.info(f"self.bucket: {self.bucket}")
                logger.info(f"S3 response: {response}")
                
                crawls = []
                for prefix in response.get('CommonPrefixes', []):
                    crawl_name = prefix['Prefix'].split('/')[-2]
                    if crawl_name.startswith('CC-MAIN'):
                        # Extract timestamp from crawl name (CC-MAIN-YYYY-WW)
                        parts = crawl_name.split('-')
                        if len(parts) >= 4:
                            year_week = f"{parts[2]}-{parts[3]}"
                            crawls.append(CrawlInfo(
                                crawl_id=crawl_name,
                                timestamp=year_week,
                                segment_count=0  # Will be populated later
                            ))
            else:
                # Use HTTP to get crawl list
                logger.info("Using HTTP to list crawls")
                # Download the crawl list from a known endpoint
                url = f"{self.http_base_url}/crawl-data/"
                print(f"Note: HTTP listing not fully supported. Using recent known crawls.")
                
                # Return recent known crawls (you'd update this list periodically)
                crawls = [
                    CrawlInfo("CC-MAIN-2024-10", "2024-10", 0),
                    CrawlInfo("CC-MAIN-2024-06", "2024-06", 0),
                    CrawlInfo("CC-MAIN-2024-03", "2024-03", 0),
                ]
            
            return sorted(crawls, key=lambda x: x.crawl_id, reverse=True)
            
        except Exception as e:
            print(f"Error listing crawls: {e}")
            return []
    
    def get_wet_paths(self, crawl_id: str, max_files: int = 10) -> List[str]:
        """
        Get list of WET file paths for a specific crawl
        
        Args:
            crawl_id: The crawl identifier (e.g., 'CC-MAIN-2024-10')
            max_files: Maximum number of file paths to return
        """
        print(f"Fetching WET file paths for {crawl_id}...")
        
        # Path to the WET paths file
        wet_paths_key = f"crawl-data/{crawl_id}/wet.paths.gz"
        
        try:
            if self.use_aws_auth:
                # Download from S3
                response = self.s3_client.get_object(Bucket=self.bucket, Key=wet_paths_key)
                content = gzip.decompress(response['Body'].read())
            else:
                # Download via HTTP
                url = f"{self.http_base_url}/{wet_paths_key}"
                response = requests.get(url, stream=True)
                response.raise_for_status()
                content = gzip.decompress(response.content)
            
            # Parse paths
            paths = content.decode('utf-8').strip().split('\n')
            
            # Return requested number of paths
            return paths[:max_files]
            
        except Exception as e:
            print(f"Error fetching WET paths: {e}")
            return []
    
    def sample_wet_file(self, wet_path: str, max_records: int = 5, 
                       sample_size_mb: float = 1.0) -> List[WETRecord]:
        """
        Sample records from a WET file without downloading the entire file
        
        Args:
            wet_path: Path to the WET file (without bucket/domain prefix)
            max_records: Maximum number of records to extract
            sample_size_mb: Maximum MB to download (using range requests)
        """
        print(f"Sampling WET file: {wet_path}")
        
        records = []
        sample_size_bytes = int(sample_size_mb * 1024 * 1024)
        
        try:
            # Use HTTP with range request for sampling
            url = f"{self.http_base_url}/{wet_path}"
            
            # First, get file size
            head_response = requests.head(url)
            file_size = int(head_response.headers.get('Content-Length', 0))
            
            print(f"File size: {file_size / (1024*1024):.2f} MB")
            
            # Download first chunk using range request
            headers = {'Range': f'bytes=0-{sample_size_bytes-1}'}
            response = requests.get(url, headers=headers, stream=True)
            
            # Handle gzipped content
            if wet_path.endswith('.gz'):
                # For gzipped files, we need to be careful with partial downloads
                # Download a bit more to ensure we get complete records
                full_response = requests.get(url, stream=True)
                content_stream = gzip.GzipFile(fileobj=BytesIO(full_response.content[:sample_size_bytes*2]))
            else:
                content_stream = BytesIO(response.content)
            
            # Parse WET records using WARC format
            current_record = None
            current_content = []
            record_count = 0
            
            for line in content_stream:
                try:
                    line = line.decode('utf-8', errors='ignore')
                except:
                    continue
                
                if line.startswith('WARC/'):
                    # Start of new record
                    if current_record and current_content:
                        # Save previous record
                        content_text = ''.join(current_content).strip()
                        if content_text:  # Only save non-empty records
                            records.append(WETRecord(
                                url=current_record.get('uri', ''),
                                content=content_text[:5000],  # Limit content size
                                content_length=len(content_text),
                                timestamp=current_record.get('date', ''),
                                language=current_record.get('language', 'unknown')
                            ))
                            record_count += 1
                            
                            if record_count >= max_records:
                                break
                    
                    # Reset for new record
                    current_record = {}
                    current_content = []
                    
                elif line.startswith('WARC-Target-URI:'):
                    if current_record is not None:
                        current_record['uri'] = line.split(':', 1)[1].strip()
                        
                elif line.startswith('WARC-Date:'):
                    if current_record is not None:
                        current_record['date'] = line.split(':', 1)[1].strip()
                        
                elif line.startswith('WARC-Identified-Content-Language:'):
                    if current_record is not None:
                        current_record['language'] = line.split(':', 1)[1].strip()
                        
                elif line.strip() == '' and current_record is not None and 'uri' in current_record:
                    # Empty line after headers, content follows
                    continue
                    
                elif current_record is not None and 'uri' in current_record:
                    # This is content
                    current_content.append(line)
            
            print(f"Extracted {len(records)} records from sample")
            
        except Exception as e:
            print(f"Error sampling WET file: {e}")
        
        return records
    
    def analyze_crawl_sample(self, crawl_id: str, num_files: int = 3, 
                            records_per_file: int = 5) -> Dict:
        """
        Analyze a sample of WET files from a crawl to understand data characteristics
        
        Args:
            crawl_id: The crawl identifier
            num_files: Number of WET files to sample
            records_per_file: Number of records to extract per file
        """
        print(f"\n{'='*60}")
        print(f"Analyzing crawl: {crawl_id}")
        print(f"{'='*60}\n")
        
        # Get WET file paths
        wet_paths = self.get_wet_paths(crawl_id, max_files=num_files)
        
        if not wet_paths:
            print("No WET paths found!")
            return {}
        
        # Analyze samples
        all_records = []
        language_counts = {}
        url_domains = set()
        content_lengths = []
        
        for i, wet_path in enumerate(wet_paths[:num_files], 1):
            print(f"\n--- Sampling file {i}/{num_files} ---")
            records = self.sample_wet_file(wet_path, max_records=records_per_file)
            
            for record in records:
                all_records.append(record)
                
                # Track statistics
                language_counts[record.language] = language_counts.get(record.language, 0) + 1
                content_lengths.append(record.content_length)
                
                # Extract domain
                try:
                    domain = urlparse(record.url).netloc
                    url_domains.add(domain)
                except:
                    pass
        
        # Calculate statistics
        stats = {
            'crawl_id': crawl_id,
            'files_sampled': num_files,
            'total_records': len(all_records),
            'unique_domains': len(url_domains),
            'languages': language_counts,
            'avg_content_length': sum(content_lengths) / len(content_lengths) if content_lengths else 0,
            'min_content_length': min(content_lengths) if content_lengths else 0,
            'max_content_length': max(content_lengths) if content_lengths else 0,
            'sample_records': all_records[:3]  # Keep a few examples
        }
        
        # Print summary
        print(f"\n{'='*60}")
        print("ANALYSIS SUMMARY")
        print(f"{'='*60}")
        print(f"Total records sampled: {stats['total_records']}")
        print(f"Unique domains: {stats['unique_domains']}")
        print(f"Average content length: {stats['avg_content_length']:.0f} characters")
        print(f"Content length range: {stats['min_content_length']} - {stats['max_content_length']}")
        print(f"\nTop languages:")
        for lang, count in sorted(language_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {lang}: {count} records")
        
        print(f"\nSample records:")
        for i, record in enumerate(stats['sample_records'], 1):
            print(f"\n  Record {i}:")
            print(f"    URL: {record.url}")
            print(f"    Language: {record.language}")
            print(f"    Content preview: {record.content[:200]}...")
        
        return stats
    
    def download_wet_file(self, wet_path: str, output_path: str) -> bool:
        """
        Download a complete WET file for local processing
        
        Args:
            wet_path: Path to the WET file
            output_path: Local path to save the file
        """
        print(f"Downloading {wet_path} to {output_path}")
        
        try:
            if self.use_aws_auth:
                # Download via S3
                self.s3_client.download_file(self.bucket, wet_path, output_path)
            else:
                # Download via HTTP
                url = f"{self.http_base_url}/{wet_path}"
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                # Stream to file
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            print(f"Successfully downloaded to {output_path}")
            return True
            
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False

def main():
    """
    Main function to test the Common Crawl Explorer
    """
    print("Common Crawl Explorer - WET Files for LLM Training")
    print("=" * 60)
    
    # Initialize explorer (use HTTP by default for accessibility)
    explorer = CommonCrawlExplorer(use_aws_auth=False)
    
    # Get available crawls
    crawls = explorer.get_available_crawls()
    
    if crawls:
        print("\nAvailable crawls:")
        for i, crawl in enumerate(crawls[:5], 1):
            print(f"  {i}. {crawl.crawl_id} (timestamp: {crawl.timestamp})")
        
        # Analyze most recent crawl
        if crawls:
            latest_crawl = crawls[0]
            print(f"\nAnalyzing latest crawl: {latest_crawl.crawl_id}")
            
            # Perform analysis
            stats = explorer.analyze_crawl_sample(
                latest_crawl.crawl_id,
                num_files=2,  # Sample 2 files
                records_per_file=3  # Extract 3 records per file
            )
            
            # Optional: Download a single WET file for deeper analysis
            # wet_paths = explorer.get_wet_paths(latest_crawl.crawl_id, max_files=1)
            # if wet_paths:
            #     explorer.download_wet_file(wet_paths[0], 'sample.warc.wet.gz')
    
    else:
        print("No crawls found. Check your internet connection or AWS credentials.")

if __name__ == "__main__":
    main()