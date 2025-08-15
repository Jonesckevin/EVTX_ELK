import os
import sys
import glob
import time
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count

try:
    import ujson as jsonlib
except Exception:
    jsonlib = json

try:
    from Evtx.Evtx import Evtx  # python-evtx
except Exception as e:
    print(f"[FATAL] python-evtx not available: {e}")
    sys.exit(1)

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
BULK_SIZE = int(os.environ.get("BULK_SIZE", "5000"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", min(4, cpu_count())))


def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


def wait_for_es(es: Elasticsearch, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if es.ping():
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def clear_existing_indices(es: Elasticsearch):
    try:
        indices = es.indices.get(index="*")
        for idx in indices.keys():
            if idx.startswith("."):
                continue  # keep system indices
            log(f"Deleting index {idx}")
            try:
                es.indices.delete(index=idx, ignore=[400, 404])
            except Exception as e:
                log(f"Warning: failed to delete {idx}: {e}")
    except Exception as e:
        log(f"Warning: listing indices failed: {e}")


def evtx_record_iter(evtx_path):
    with Evtx(evtx_path) as evtx:
        for record in evtx.records():
            try:
                xml = record.xml()
                yield xml
            except Exception:
                continue


def _local(tag: str) -> str:
    # return the local name of an XML tag possibly with namespace
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_child(parent: ET.Element, name: str):
    for c in list(parent):
        if _local(c.tag) == name:
            return c
    return None


def xml_to_doc(xml_text: str):
    """Enhanced EVTX Event XML parser that extracts comprehensive event data.

    Extracts detailed fields from System, EventData, and other sections.
    Handles various XML structures and data types properly.
    """
    doc: dict = {
        "@ingested_at": datetime.utcnow().isoformat() + "Z",
    }
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        # Skip documents that can't be parsed rather than storing raw XML
        log(f"Warning: Failed to parse XML document: {str(e)[:100]}")
        return None  # Return None to indicate this document should be skipped

    # System fields - Enhanced parsing
    system = _find_child(root, "System")
    if system is not None:
        # Provider information
        provider = _find_child(system, "Provider")
        if provider is not None:
            doc["provider_name"] = provider.attrib.get("Name")
            if "Guid" in provider.attrib:
                doc["provider_guid"] = provider.attrib.get("Guid")

        # Event ID with qualifiers
        event_id = _find_child(system, "EventID")
        if event_id is not None:
            try:
                doc["event_id"] = int((event_id.text or "").strip())
            except Exception:
                pass
            if "Qualifiers" in event_id.attrib:
                try:
                    doc["event_qualifiers"] = int(event_id.attrib.get("Qualifiers"))
                except Exception:
                    doc["event_qualifiers"] = event_id.attrib.get("Qualifiers")

        # Standard system fields
        for name, field in (
            ("Version", "version"),
            ("Level", "level"),
            ("Task", "task"),
            ("Opcode", "opcode"),
            ("Keywords", "keywords"),
            ("EventRecordID", "record_id"),
            ("Channel", "channel"),
            ("Computer", "computer"),
        ):
            el = _find_child(system, name)
            if el is not None and el.text is not None:
                val = el.text.strip()
                if field in ("version", "level", "task", "opcode"):
                    try:
                        doc[field] = int(val)
                    except Exception:
                        doc[field] = val
                elif field == "record_id":
                    try:
                        doc[field] = int(val)
                    except Exception:
                        doc[field] = val
                elif field == "keywords":
                    # Handle hex keywords
                    if val.startswith("0x"):
                        try:
                            doc[field] = int(val, 16)
                            doc["keywords_hex"] = val
                        except Exception:
                            doc[field] = val
                    else:
                        try:
                            doc[field] = int(val)
                        except Exception:
                            doc[field] = val
                else:
                    doc[field] = val

        # Enhanced timestamp parsing
        time_created = _find_child(system, "TimeCreated")
        if time_created is not None:
            ts = time_created.attrib.get("SystemTime")
            if ts:
                # Normalize timestamp to ISO8601 for Elasticsearch
                t = ts.strip()
                if "T" not in t and " " in t:
                    t = t.replace(" ", "T", 1)
                if "Z" not in t and "+" not in t and t[-1].isdigit():
                    t = t + "Z"
                doc["@timestamp"] = t

        # Security information
        security = _find_child(system, "Security")
        if security is not None:
            user_id = security.attrib.get("UserID")
            if user_id:
                doc["user_sid"] = user_id

        # Correlation information
        correlation = _find_child(system, "Correlation")
        if correlation is not None:
            activity_id = correlation.attrib.get("ActivityID")
            related_activity_id = correlation.attrib.get("RelatedActivityID")
            if activity_id:
                doc["activity_id"] = activity_id
            if related_activity_id:
                doc["related_activity_id"] = related_activity_id

        # Execution information
        execution = _find_child(system, "Execution")
        if execution is not None:
            process_id = execution.attrib.get("ProcessID")
            thread_id = execution.attrib.get("ThreadID")
            if process_id:
                try:
                    doc["process_id"] = int(process_id)
                except Exception:
                    doc["process_id"] = process_id
            if thread_id:
                try:
                    doc["thread_id"] = int(thread_id)
                except Exception:
                    doc["thread_id"] = thread_id

    # Enhanced EventData parsing
    event_data: dict = {}
    ed = _find_child(root, "EventData")
    if ed is None:
        ed = _find_child(root, "UserData")  # sometimes used instead
    
    if ed is not None:
        for child in list(ed):
            if _local(child.tag) == "Data":
                key = child.attrib.get("Name") or "Data"
                val = (child.text or "").strip()
                
                # Enhanced data type conversion
                converted_val = _convert_value(val)
                
                # If key repeats, aggregate into list
                if key in event_data:
                    if isinstance(event_data[key], list):
                        event_data[key].append(converted_val)
                    else:
                        event_data[key] = [event_data[key], converted_val]
                else:
                    event_data[key] = converted_val
                    
            elif _local(child.tag) == "Binary":
                # Handle binary data
                val = (child.text or "").strip()
                if val:
                    event_data["binary_data"] = val
            else:
                # Handle other nested structures
                val = (child.text or "").strip()
                if val:
                    # Handle XML-encoded content
                    if val.startswith("&lt;") and val.endswith("&gt;"):
                        # This is XML-encoded content, decode it
                        try:
                            import html
                            decoded_val = html.unescape(val)
                            event_data[_local(child.tag)] = decoded_val
                        except Exception:
                            event_data[_local(child.tag)] = val
                    else:
                        event_data[_local(child.tag)] = _convert_value(val)
    
    if event_data:
        doc["event_data"] = event_data

    # Raw XML removed - only parsed fields are stored
    # doc["raw_xml"] = xml_text  # Commented out to save space and focus on parsed data
    return doc


def _convert_value(value: str):
    """Convert string values to appropriate data types."""
    if not value or value == "(NULL)":
        return None
    
    # Try to convert to integer
    try:
        if value.startswith("0x"):
            return int(value, 16)
        elif value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            return int(value)
    except ValueError:
        pass
    
    # Try to convert to float
    try:
        if "." in value and value.replace(".", "").replace("-", "").isdigit():
            return float(value)
    except ValueError:
        pass
    
    # Check for boolean-like values
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    
    # Handle timestamp-like values
    if len(value) > 10 and ("-" in value or ":" in value):
        # This might be a timestamp, keep as string for now
        # Elasticsearch will handle date parsing if needed
        pass
    
    return value

# Optimized bulk indexing with better error handling and performance.
def bulk_index(es: Elasticsearch, index: str, docs):
    if not docs:
        return 0, 0
        
    actions = [
        {
            "_index": index,
            "_source": doc,
        }
        for doc in docs
    ]
    
    try:
        # Use parallel_bulk for better performance with larger datasets
        success_count = 0
        error_count = 0
        first_errors = []
        
        for success, info in helpers.parallel_bulk(
            es,
            actions,
            chunk_size=BULK_SIZE,
            thread_count=2,                     # Limit concurrent threads to avoid overwhelming ES
            max_chunk_bytes=100 * 1024 * 1024,  # 100MB chunks
            request_timeout=300,                # 5 minutes timeout for large batches
        ):
            if success:
                success_count += 1
            else:
                error_count += 1
                if len(first_errors) < 10:      # Keep more error examples for debugging
                    first_errors.append(info)
        
        if error_count:
            log(f"Bulk indexing had {error_count} errors; showing first few: {jsonlib.dumps(first_errors[:3])[:1000]}")
        
        return success_count, error_count
        
    except Exception as e:
        log(f"Bulk indexing failed with exception: {e}")
        return 0, len(docs)


def ensure_index(es: Elasticsearch, index: str):
    if es.indices.exists(index=index):
        return
    
    # Optimized index settings for better ingestion performance with enhanced field mapping
    es.indices.create(
        index=index,
        mappings={
            "dynamic": False,
            "properties": {
                "@timestamp": {"type": "date"},
                "@ingested_at": {"type": "date"},
                # Provider information
                "provider_name": {"type": "keyword"},
                "provider_guid": {"type": "keyword"},
                # Event identification
                "event_id": {"type": "integer"},
                "event_qualifiers": {"type": "long"},  # Can be hex values
                # System fields
                "version": {"type": "integer"},
                "level": {"type": "integer"},
                "task": {"type": "integer"},
                "opcode": {"type": "integer"},
                "record_id": {"type": "long"},
                "channel": {"type": "keyword"},
                "computer": {"type": "keyword"},
                # Security and correlation
                "user_sid": {"type": "keyword"},
                "activity_id": {"type": "keyword"},
                "related_activity_id": {"type": "keyword"},
                # Process information
                "process_id": {"type": "integer"},
                "thread_id": {"type": "integer"},
                # Keywords (can be hex or integer)
                "keywords": {"type": "long"},
                "keywords_hex": {"type": "keyword"},
                # File and case metadata
                "file_path": {"type": "keyword"},
                "case": {"type": "keyword"},
                # Event data (flattened for flexible querying)
                "event_data": {
                    "type": "flattened",
                    "split_queries_on_whitespace": True
                }
                # Raw XML field removed to save storage space
            }
        },
        settings={
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,  # No replicas for faster indexing
                "refresh_interval": "30s",  # Less frequent refresh for better performance
                "mapping": {"total_fields": {"limit": 15000}},  # Increased limit for more fields
                # Optimizations for bulk indexing
                "translog": {
                    "flush_threshold_size": "1GB",
                    "sync_interval": "30s"
                },
                "merge": {
                    "policy": {
                        "max_merge_at_once": 5,
                        "segments_per_tier": 4
                    }
                }
            }
        }
    )


def derive_index_name(subfolder: str) -> str:
    # Normalize to a valid Elasticsearch index name
    # rules: lowercase, no spaces, allowed chars [a-z0-9-_+], cannot start with - _ +
    name = subfolder.strip().lower().replace(" ", "_")
    cleaned = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_", "+"):
            cleaned.append(ch)
        else:
            cleaned.append("-")
    safe = "".join(cleaned).strip(" ")
    if not safe or safe[0] in ("-", "_", "+"):
        safe = f"idx-{safe.lstrip('-_+')}"
    return safe


# Process a single EVTX file and return documents.
def process_evtx_file(evtx_file: str, index_name: str) -> list:
    
    docs = []
    try:
        log(f"Processing {evtx_file}")
        for xml in evtx_record_iter(evtx_file):
            doc = xml_to_doc(xml)
            if doc is not None:  # Skip documents that failed to parse
                doc["file_path"] = evtx_file
                doc["case"] = index_name
                docs.append(doc)
    except Exception as e:
        log(f"Error processing {evtx_file}: {e}")
    return docs


def ingest_folder(es: Elasticsearch, folder_path: str, index_name: str):
    """Optimized folder ingestion with parallel processing."""
    ensure_index(es, index_name)
    evtx_files = glob.glob(os.path.join(folder_path, "*.evtx"))
    if not evtx_files:
        log(f"No EVTX files in {folder_path}")
        return

    log(f"Found {len(evtx_files)} EVTX files to process")
    total_count = 0
    
    # Process files in parallel batches
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all file processing tasks
        future_to_file = {
            executor.submit(process_evtx_file, evtx_file, index_name): evtx_file 
            for evtx_file in evtx_files
        }
        
        # Process completed tasks and bulk index in batches
        batch = []
        for future in as_completed(future_to_file):
            evtx_file = future_to_file[future]
            try:
                docs = future.result()
                batch.extend(docs)
                
                # Bulk index when batch gets large enough
                if len(batch) >= BULK_SIZE:
                    success, errors = bulk_index(es, index_name, batch)
                    total_count += success
                    if success > 0:
                        log(f"Indexed {success} documents from batch (total: {total_count})")
                    batch.clear()
                    
            except Exception as e:
                log(f"Error processing future for {evtx_file}: {e}")
        
        # Index remaining documents
        if batch:
            success, errors = bulk_index(es, index_name, batch)
            total_count += success
            if success > 0:
                log(f"Indexed final batch of {success} documents")
    
    # Optimize index after ingestion
    try:
        log(f"Optimizing index {index_name}...")
        es.indices.forcemerge(index=index_name, max_num_segments=1)
        # Re-enable refresh for normal operations
        es.indices.put_settings(
            index=index_name, 
            body={"refresh_interval": "1s"}
        )
    except Exception as e:
        log(f"Warning: Failed to optimize index {index_name}: {e}")
    
    log(f"Indexed {total_count} documents into {index_name}")


def ingest_folder_original(es: Elasticsearch, folder_path: str, index_name: str):
    """Original sequential processing function - kept as backup."""
    ensure_index(es, index_name)
    evtx_files = glob.glob(os.path.join(folder_path, "*.evtx"))
    if not evtx_files:
        log(f"No EVTX files in {folder_path}")
        return

    batch = []
    count = 0
    for evtx_file in evtx_files:
        log(f"Processing {evtx_file}")
        for xml in evtx_record_iter(evtx_file):
            doc = xml_to_doc(xml)
            doc["file_path"] = evtx_file
            doc["case"] = index_name
            batch.append(doc)
            if len(batch) >= BULK_SIZE:
                success, errors = bulk_index(es, index_name, batch)
                count += success
                batch.clear()
    if batch:
        success, errors = bulk_index(es, index_name, batch)
        count += success
    log(f"Indexed {count} documents into {index_name}")


def main():
    log(f"Starting EVTX ingestion with optimized settings:")
    log(f"  - Elasticsearch URL: {ES_URL}")
    log(f"  - Data root: {DATA_ROOT}")
    log(f"  - Bulk size: {BULK_SIZE}")
    log(f"  - Max workers: {MAX_WORKERS}")
    
    es = Elasticsearch(ES_URL, request_timeout=300, verify_certs=False)  # Increased timeout
    if not wait_for_es(es):
        log("Elasticsearch not ready after timeout")
        sys.exit(2)

    # Clear all non-system indices
    log("Clearing existing indices")
    clear_existing_indices(es)

    # Each immediate subfolder under DATA_ROOT is an index, e.g., /data/Case01 -> index "Case01"
    if not os.path.isdir(DATA_ROOT):
        log(f"DATA_ROOT {DATA_ROOT} does not exist or is not a directory")
        return

    folders = [entry for entry in sorted(os.listdir(DATA_ROOT)) 
              if os.path.isdir(os.path.join(DATA_ROOT, entry))]
    
    if not folders:
        log("No folders found to process")
        return
    
    log(f"Found {len(folders)} folders to process: {folders}")
    
    start_time = time.time()
    for i, entry in enumerate(folders, 1):
        full = os.path.join(DATA_ROOT, entry)
        index = derive_index_name(entry)
        log(f"[{i}/{len(folders)}] Processing folder: {entry} -> index: {index}")
        folder_start = time.time()
        ingest_folder(es, full, index)
        folder_time = time.time() - folder_start
        log(f"Completed {entry} in {folder_time:.2f} seconds")

    total_time = time.time() - start_time
    log(f"Ingestion complete! Total time: {total_time:.2f} seconds")
    log("Access Kibana by going to http://localhost:5601")
    log("Saved objects should be automatically imported by the kibana-setup assuming your version is supported")

if __name__ == "__main__":
    main()
