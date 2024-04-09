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
	for (uint page_id = left_rel.first; page_id < left_rel.second; page_id++) {
		Page* page = disk->diskRead(page_id);
		for (uint record_id = 0; record_id < page->size(); record_id++) {
			Record record = page->get_record(record_id);
            uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            partitions[bucket_id].add_left_rel_page(page_id);
		}
	}
	// partite right relation
	for (uint page_id = right_rel.first; page_id < right_rel.second; page_id++) {
		Page* page = disk->diskRead(page_id);
		for (uint record_id = 0; record_id < page->size(); record_id++) {
			Record record = page->get_record(record_id);
            uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            partitions[bucket_id].add_right_rel_page(page_id);
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
	vector<uint> disk_pages; 
	for (Bucket& bucket : partitions) {
        unordered_map<uint, vector<Record>> hashtable;
        for (uint left_page_id : bucket.get_left_rel()) {
            Page* page = mem->mem_page(0);
            mem->loadFromDisk(disk, left_page_id, 0);
            for (uint i = 0; i < page->size(); ++i) { // each record in page
                Record r = page->get_record(i);
                hashtable[r.probe_hash()].push_back(r);
            }
        }

		for (uint right_page_id : bucket.get_right_rel()) {
			Page* page = mem->mem_page(1);
			mem->loadFromDisk(disk, right_page_id, 1);
			for (uint i = 0; i < page->size(); ++i) { // each record in page
				Record s = page->get_record(i);
				uint hash_value = s.probe_hash(); // get hash value for that record

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
