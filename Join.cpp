#include "Join.hpp"

#include <vector>
#include <unordered_map>
using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));
	// partite left relation
	Page* page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
	for (uint page_id = left_rel.first; page_id < left_rel.second; page_id++) {
		//Page* page = disk->diskRead(page_id);//dont call load to  in memory
		mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 1);

		for (uint record_id = 0; record_id < page->size(); record_id++) {
			Record record = page->get_record(record_id);
            uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			//load record and put it in memory corresponding to bucket and page
			//if full put the page in disk, (flush)
			if (mem->mem_page(bucket_id)->full()) {
				uint flush_idx = mem->flushToDisk(disk, bucket_id);
				//only if full
				//partition hold disk page
				partitions[bucket_id].add_left_rel_page(flush_idx);
			}
			mem->mem_page(bucket_id)->loadRecord(page->get_record(record_id));
		}
	}
	//flush all records in memory to disk 
	for (uint k = 0; k < MEM_SIZE_IN_PAGE - 1; k++) {
		if (!mem->mem_page(k)->empty()) {
			uint flush_idx = mem->flushToDisk(disk, k);
			partitions[k].add_left_rel_page(flush_idx);
		}
	}


	// partite right relation
	for (uint page_id = right_rel.first; page_id < right_rel.second; page_id++) {
		mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 1);

		for (uint record_id = 0; record_id < page->size(); record_id++) {
			Record record = page->get_record(record_id);
			uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			if (mem->mem_page(bucket_id)->full()) {
				uint flush_idx = mem->flushToDisk(disk, bucket_id);
				partitions[bucket_id].add_right_rel_page(flush_idx);
			}
			mem->mem_page(bucket_id)->loadRecord(page->get_record(record_id));
		}
	}
	//flush all records in memory to disk 
	for (uint k = 0; k < MEM_SIZE_IN_PAGE - 1; k++) {
		if (!mem->mem_page(k)->empty()) {
			uint flush_idx = mem->flushToDisk(disk, k);
			partitions[k].add_right_rel_page(flush_idx);
		}
	}

	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	//no hash table 
	vector<uint> disk_pages; 
	for (Bucket& bucket : partitions) {
        unordered_map<uint, vector<Record>> hashtable;
		// unordered_map<uint, vector<bool>> matched; // record whether match
        for (uint left_page_id : bucket.get_left_rel()) {
            Page* page = mem->mem_page(0);
            mem->loadFromDisk(disk, left_page_id, 0);
            for (uint i = 0; i < page->size(); ++i) { // each record in page
                Record r = page->get_record(i);
                hashtable[r.probe_hash() % (MEM_SIZE_IN_PAGE - 2)].push_back(r);
				// matched[r.probe_hash()].push_back(false); // initialize with unmatch
            }
        }

		for (uint right_page_id : bucket.get_right_rel()) {
			Page* page = mem->mem_page(1);
			mem->loadFromDisk(disk, right_page_id, 1);
			for (uint i = 0; i < page->size(); ++i) { // each record in page
				Record s = page->get_record(i);
				uint hash_value = s.probe_hash() % (MEM_SIZE_IN_PAGE - 2); // get hash value for that record

				if (hashtable.count(hash_value)) { // if the slot have records
					for (Record& r : hashtable[hash_value]) { // iterate every records in the slot
						if (r == s) { // test whether it equail to the current one
							Page* output_page = mem->mem_page(2);
							if (output_page->full()) {
								uint disk_page_id = mem->flushToDisk(disk, 2); // send the full page with result to disk
								disk_pages.push_back(disk_page_id);
								output_page->reset();
							}
							output_page->loadPair(r, s);
						}
					}
				}
			}
		}

		// Flush the last page if it's not empty.
        Page* output_page = mem->mem_page(2);
        if (!output_page->empty()) {
            uint disk_page_id = mem->flushToDisk(disk, 2);
            disk_pages.push_back(disk_page_id);
        }
	}

	return disk_pages;
}
