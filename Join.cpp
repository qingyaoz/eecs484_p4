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
	//reserve two pages for input and output
	Page* output_page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
	Page* input_page = mem->mem_page(MEM_SIZE_IN_PAGE - 2);

	vector<uint> disk_pages; 

	for (Bucket& bucket : partitions) {
        //unordered_map<uint, vector<Record>> hashtable;
		// unordered_map<uint, vector<bool>> matched; // record whether match

		//“smaller relation” is defined as the relation with the fewer total number of records.
		bool isLeftSmaller = bucket.num_left_rel_record <= bucket.num_right_rel_record;
		vector<uint> smaller_rel_pages;
		vector<uint> larger_rel_pages;
		if (isLeftSmaller) {
			smaller_rel_pages = bucket.get_left_rel();
			larger_rel_pages = bucket.get_right_rel();
		} else {
			smaller_rel_pages = bucket.get_right_rel();
			larger_rel_pages = bucket.get_left_rel();
		}

        for (uint page_id : smaller_rel_pages) {
			//Page* page = mem->mem_page(0);
			mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 2);
            for (uint i = 0; i < input_page->size(); ++i) { // each record in page
                Record r = input_page->get_record(i);
                //hashtable[r.probe_hash() % (MEM_SIZE_IN_PAGE - 2)].push_back(r);
				// matched[r.probe_hash()].push_back(false); // initialize with unmatch
				// add to mem
				mem->mem_page(r.probe_hash() % (MEM_SIZE_IN_PAGE - 2))->loadRecord(r);
            }
			//clean page
			mem->mem_page(MEM_SIZE_IN_PAGE - 2)->reset();
        }

        for (uint page_id : larger_rel_pages) {
			mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 2);
            for (uint i = 0; i < input_page->size(); ++i) { // each record in page
                Record r = input_page->get_record(i);
                uint hash = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
				// find match?
				Page* temp = mem->mem_page(hash);
				for (uint i = 0; i < temp->size(); i++) {
					if (temp->get_record(i) == r) {
						// found match
						if (output_page->full()) {
							// output page is full, need to flush to disk
							uint flush_idx = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
							disk_pages.push_back(flush_idx);
							output_page->reset();
						}
						//load the pair
						output_page->loadPair(temp->get_record(i), r);
					}
				}
				//mem->mem_page(r.probe_hash() % (MEM_SIZE_IN_PAGE - 2))->loadRecord(r);
            }
			//clean page
			mem->mem_page(MEM_SIZE_IN_PAGE - 2)->reset();
        }
		// for (uint right_page_id : bucket.get_right_rel()) {
		// 	Page* page = mem->mem_page(1);
		// 	mem->loadFromDisk(disk, right_page_id, 1);
		// 	for (uint i = 0; i < page->size(); ++i) { // each record in page
		// 		Record s = page->get_record(i);
		// 		uint hash_value = s.probe_hash() % (MEM_SIZE_IN_PAGE - 2); // get hash value for that record
		// 		if (hashtable.count(hash_value)) { // if the slot have records
		// 			for (Record& r : hashtable[hash_value]) { // iterate every records in the slot
		// 				if (r == s) { // test whether it equail to the current one
		// 					Page* output_page = mem->mem_page(2);
		// 					if (output_page->full()) {
		// 						uint disk_page_id = mem->flushToDisk(disk, 2); // send the full page with result to disk
		// 						disk_pages.push_back(disk_page_id);
		// 						output_page->reset();
		// 					}
		// 					output_page->loadPair(r, s);
		// 				}
		// 			}
		// 		}
		// 	}
		// }

		// Clear the hash table for the next partition
		for (uint i = 0; i < MEM_SIZE_IN_PAGE - 2; i++) {
			mem->mem_page(i)->reset();
		}
	}
	// Flush the last page if it's not empty.
	if (!output_page->empty()) {
		uint disk_page_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
		disk_pages.push_back(disk_page_id);
		output_page->reset();
	}

	return disk_pages;
}
