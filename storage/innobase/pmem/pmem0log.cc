/* 
 * Author; Trong-Dat Nguyen
 * MySQL Partitioned log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2018 VLDB Lab - Sungkyunkwan University
 * */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>                                                                      
#include <sys/time.h> //for struct timeval, gettimeofday()
#include <string.h>
#include <stdint.h> //for uint64_t
#include <math.h> //for log()
#include <assert.h>
#include <wchar.h>
#include <unistd.h> //for access()

#include "mtr0log.h" //for mlog_parse_initial_log_record()
#include "dyn0buf.h" // for mtr_buf_t

#include "my_pmem_common.h"
#include "my_pmemobj.h"

#include "os0file.h"

#if defined (UNIV_PMEMOBJ_PL)

#if defined (UNIV_PMEMOBJ_PL)
static FILE* debug_ptxl_file = fopen("pll_debug.txt","a");
static FILE* lock_overhead_file = fopen("ppl_lock_overhead.txt","a");
/*Part Log*/
static uint64_t PMEM_N_LOG_BUCKETS;
static uint64_t PMEM_N_BLOCKS_PER_BUCKET;

/*Log Buf*/
static uint64_t PMEM_LOG_BUF_SIZE;

/*Transaction Table*/
static uint64_t PMEM_TT_N_LINES;
static uint64_t PMEM_TT_N_ENTRIES_PER_LINE;
static uint64_t PMEM_TT_MAX_DIRTY_PAGES_PER_TX;


/*Flush Log*/
static double PMEM_LOG_BUF_FLUSH_PCT;
static uint64_t PMEM_LOG_FLUSHER_WAKE_THRESHOLD=5;
static uint64_t PMEM_LOG_REDOER_WAKE_THRESHOLD=30;
static uint64_t PMEM_N_LOG_FLUSH_THREADS=32;

/*Log files*/
//static uint64_t PMEM_LOG_FILE_SIZE=4*1024; //in 4-KB pages (16MB)
static uint64_t PMEM_LOG_FILE_SIZE=16*1024; //in 4-KB pages (64MB)
//static uint64_t PMEM_N_LOG_FILES_PER_BUCKET=2;
static uint64_t PMEM_N_LOG_FILES_PER_BUCKET=1;


#endif //UNIV_PMEMOBJ_PL
//////////////// NEW PMEM PARTITION LOG /////////////

/////////////////// PER-TX LOGGING /////////////////
void
pm_wrapper_tx_log_alloc_or_open(
		PMEM_WRAPPER*	pmw,
		uint64_t		n_buckets,
		uint64_t		n_blocks_per_bucket,
		uint64_t		block_size){


	PMEM_N_LOG_BUCKETS = n_buckets;
	PMEM_N_BLOCKS_PER_BUCKET = n_blocks_per_bucket;
	
	//Case 1: Alocate new buffer in PMEM
	if (!pmw->ptxl) {
		pmw->ptxl = alloc_pmem_tx_part_log(pmw->pop,
				PMEM_N_LOG_BUCKETS,
				PMEM_N_BLOCKS_PER_BUCKET,
				block_size);

		if (pmw->ptxl == NULL){
			printf("PMEMOBJ_ERROR: error when allocate buffer in pm_wrapper_log_alloc_or_open()\n");
			exit(0);
		}

		printf("\n=================================\n Footprint of TX part-log:\n");
		printf("Log area size \t\t\t %f (MB) \n", (n_buckets * n_blocks_per_bucket * block_size * 1.0)/(1024*1024) );
		printf("TX-Log metadata size \t %f (MB)\n", (pmw->ptxl->pmem_tx_log_size * 1.0)/(1024*1024));
		printf("DPT size \t\t\t %f (MB)\n", (pmw->ptxl->pmem_dpt_size * 1.0) / (1024*1024));
		printf("Total allocated \t\t %f (MB)\n", (pmw->ptxl->pmem_alloc_size * 1.0)/ (1024*1024));
		printf(" =================================\n");
	}
	else {
		//Case 2: Reused a buffer in PMEM
		printf("!!!!!!! [PMEMOBJ_INFO]: the server restart from a crash but the log buffer is persist\n");

		//We need to re-align the p_align
		byte* p;
		p = static_cast<byte*> (pmemobj_direct(pmw->ptxl->data));
		assert(p);
		pmw->ptxl->p_align = static_cast<byte*> (ut_align(p, block_size));
	}


	pmw->ptxl->deb_file = fopen("part_log_debug.txt","a");
}

/*
 * Allocate per-tx logs and DPT
 * */
PMEM_TX_PART_LOG* alloc_pmem_tx_part_log(
		PMEMobjpool*		pop,
		uint64_t			n_buckets,
		uint64_t			n_blocks_per_bucket,
		uint64_t			block_size) {

	char* p;
	size_t align_size;
	uint64_t n;
	

	uint64_t size = n_buckets * n_blocks_per_bucket * block_size;

	TOID(PMEM_TX_PART_LOG) pl; 

	POBJ_ZNEW(pop, &pl, PMEM_TX_PART_LOG);
	PMEM_TX_PART_LOG* ptxl = D_RW(pl);

	ptxl->pmem_alloc_size = sizeof(PMEM_TX_PART_LOG);

	//(1) Allocate and alignment for the log data
	//align sizes to a pow of 2
	assert(ut_is_2pow(block_size));
	align_size = ut_uint64_align_up(size, block_size);

	ptxl->size = align_size;

	ptxl->n_buckets = n_buckets;
	ptxl->n_blocks_per_bucket = n_blocks_per_bucket;
	ptxl->block_size = block_size;

	//(2) dirty page table
	__init_dpt(pop, ptxl, MAX_DPT_LINES, MAX_DPT_ENTRIES_PER_LINE);

	ptxl->pmem_alloc_size += ptxl->pmem_dpt_size;

	ptxl->is_new = true;
	ptxl->data = pm_pop_alloc_bytes(pop, align_size);

	ptxl->pmem_alloc_size += align_size;

	//align the pmem address for DIRECT_IO
	p = static_cast<char*> (pmemobj_direct(ptxl->data));
	assert(p);

	ptxl->p_align = static_cast<byte*> (ut_align(p, block_size));
	pmemobj_persist(pop, ptxl->p_align, sizeof(*ptxl->p_align));

	if (OID_IS_NULL(ptxl->data)){
		return NULL;
	}

	//(3) init the buckets
	pm_tx_part_log_bucket_init(pop,
		   	ptxl,
		   	n_buckets,
			n_blocks_per_bucket,
			block_size);

	ptxl->pmem_alloc_size += ptxl->pmem_tx_log_size;

	pmemobj_persist(pop, ptxl, sizeof(*ptxl));
	return ptxl;
}
/*
 * Init DPT with n hashed lines, k entries per line
 * k is the load factor 
 * */
void 
__init_dpt(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*		ptxl,
		uint64_t n,
		uint64_t k) {
	
	uint64_t i, j, i_temp;	
	PMEM_DPT* pdpt;
	PMEM_DPT_HASHED_LINE *pline;
	PMEM_DPT_ENTRY* pe;

	POBJ_ZNEW(pop, &ptxl->dpt, PMEM_DPT);
	pdpt = D_RW(ptxl->dpt);
	assert (pdpt);
	
	ptxl->pmem_dpt_size = sizeof(PMEM_DPT);

	//allocate the buckets (hashed lines)
	pdpt->n_buckets = n;
	pdpt->n_entries_per_bucket = k;

	POBJ_ALLOC(pop,
				&pdpt->buckets,
				TOID(PMEM_DPT_HASHED_LINE),
				sizeof(TOID(PMEM_DPT_HASHED_LINE)) * n,
				NULL,
				NULL);
	ptxl->pmem_dpt_size += sizeof(TOID(PMEM_DPT_HASHED_LINE)) * n;

	if (TOID_IS_NULL(pdpt->buckets)) {
		fprintf(stderr, "POBJ_ALLOC\n");
	}
	//for each hash line
	for (i = 0; i < n; i++) {
		POBJ_ZNEW(pop,
				&D_RW(pdpt->buckets)[i],
				PMEM_DPT_HASHED_LINE);

		ptxl->pmem_dpt_size += sizeof(PMEM_DPT_HASHED_LINE);
		if (TOID_IS_NULL(D_RW(pdpt->buckets)[i])) {
			fprintf(stderr, "POBJ_ALLOC\n");
		}
		pline = D_RW(D_RW(pdpt->buckets)[i]);

		pline->hashed_id = i;
		pline->n_entries = k;
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
		pline->n_free = k;
		pline->n_idle = 0;
#endif		
		//Allocate the entries
		POBJ_ALLOC(pop,
				&pline->arr,
				TOID(PMEM_DPT_ENTRY),
				sizeof(TOID(PMEM_DPT_ENTRY)) * k,
				NULL,
				NULL);
		ptxl->pmem_dpt_size += sizeof(TOID(PMEM_DPT_ENTRY))* k;
		//for each entry in the line
		for (j = 0; j < k; j++) {
			POBJ_ZNEW(pop,
					&D_RW(pline->arr)[j],
					PMEM_DPT_ENTRY);
			ptxl->pmem_dpt_size += sizeof(PMEM_DPT_ENTRY);
			if (TOID_IS_NULL(D_RW(pline->arr)[j])) {
				fprintf(stderr, "POBJ_ZNEW\n");
			}
			pe = D_RW(D_RW(pline->arr)[j]);

			pe->is_free = true;
			pe->key = 0;
			pe->eid = i * k + j;
			pe->count = 0;
			pe->pageLSN = 0;
			POBJ_ALLOC(pop,
					&pe->tx_idx_arr,
					int64_t,
					sizeof(int64_t) * MAX_TX_PER_PAGE,
					NULL,
					NULL);
			ptxl->pmem_dpt_size += sizeof(int64_t) * MAX_TX_PER_PAGE;
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
			pe->n_reused = 0;
			pe->max_txref_size = 0;
#endif
			for (i_temp = 0; i_temp < MAX_TX_PER_PAGE; i_temp++){
				D_RW(pe->tx_idx_arr)[i_temp] = -1;
			}
			pe->n_tx_idx = 0;
		} //end for each entry
	}// end for each line
}

void 
pm_tx_part_log_bucket_init(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*		pl,
		uint64_t			n_buckets,
		uint64_t			n_blocks_per_bucket,
		uint64_t			block_size) {

}

/*
 * Write REDO log records from mtr's heap to PMEM in partitioned-log   
 *
 * If the current transaction has its first log record, the block_id is -1
 *
 * Return the index of the log_block to write on
 * */
int64_t
pm_ptxl_write(
			PMEMobjpool*		pop,
			PMEM_TX_PART_LOG*	ptxl,
			uint64_t			tid,
			byte*				log_src,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			LSN_arr,
			uint64_t*			space_arr,
			uint64_t*			page_arr,
			int64_t				block_id)
{
	ulint hashed;
	ulint i, j;

	uint64_t n, k;
	uint64_t bucket_id, local_id;
	int64_t ret;
	uint64_t n_try;

	//handle DPT
	uint64_t page_no;
	uint64_t space_no;	
	uint64_t key;	
	uint64_t LSN;	
	uint64_t eid;

	byte* pdata;

	TOID(PMEM_TX_LOG_HASHED_LINE) line;
	PMEM_TX_LOG_HASHED_LINE* pline;

	TOID(PMEM_TX_LOG_BLOCK) log_block;
	PMEM_TX_LOG_BLOCK*	plog_block;

	PMEM_DPT* pdpt;

	n = ptxl->n_buckets;
	k = ptxl->n_blocks_per_bucket;
	if (block_id == -1) {
		//Case A: A first log write of this transaction
		//Search the right log block to write
		//(1) Hash the tid
		PMEM_LOG_HASH_KEY(hashed, tid, n);
		assert (hashed < n);

		TOID_ASSIGN(line, (D_RW(ptxl->buckets)[hashed]).oid);
		pline = D_RW(line);
		assert(pline);
		assert(pline->n_blocks == k);

		//(2) Search for the free log block to write on
		//lock the hash line
		pmemobj_rwlock_wrlock(pop, &pline->lock);

		n_try = pline->n_blocks;
		//Choose the starting point as the hashed value, to avoid contention
		PMEM_LOG_HASH_KEY(i, tid, pline->n_blocks);
		while (i < pline->n_blocks){
			TOID_ASSIGN (log_block, (D_RW(pline->arr)[i]).oid);
			plog_block = D_RW(log_block);
			if (plog_block->state == PMEM_FREE_LOG_BLOCK){
				plog_block->state = PMEM_ACTIVE_LOG_BLOCK;
				plog_block->tid = tid;
				ret = hashed * k + i;
				break;	
			}	
			//jump a litte far to avoid contention
			i = (i + JUMP_STEP) % pline->n_blocks;
			n_try--;
			if (n_try == 0){
				printf("===> PMEM ERROR, there is no empty log block to write, allocate more to solve at bucket %zu \n", hashed);
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
				__print_tx_blocks_state(debug_ptxl_file, ptxl);
#endif
				assert(0);
			}
			//try again
		}
		//unlock the hash line
		pmemobj_rwlock_unlock(pop, &pline->lock);
	}
	else {
		//Case B: Get the log block from the input id
		ret = block_id;

		bucket_id = block_id / k;
		local_id = block_id % k;
		TOID_ASSIGN(line, (D_RW(ptxl->buckets)[bucket_id]).oid);
		pline = D_RW(line);
		assert(pline);
		TOID_ASSIGN (log_block, (D_RW(pline->arr)[local_id]).oid);
		plog_block = D_RW(log_block);
		assert(plog_block->tid == tid);
	}

	// Now the plog_block point to the log block to write on and the ret is the log_block_id
	
	pmemobj_rwlock_wrlock(pop, &plog_block->lock);

	//Check the capacity, current version is fix size
	if (plog_block->cur_off + size > ptxl->block_size) {
		printf("PMEM_LOG_ERROR: The log block %zu is not enough space, cur size %zu write_size %zu block_size %zu \n", ret, plog_block->cur_off, size, ptxl->block_size);
		assert(0);
	}
	// (1) Append log record
	pdata = ptxl->p_align;
	//CACHELINE_SIZE is 64B
	if (size <= CACHELINE_SIZE){
		//Do not need a transaction for atomicity
			pmemobj_memcpy_persist(
				pop, 
				pdata + plog_block->pmemaddr + plog_block->cur_off,
				log_src,
				size);

	}
	else {
		TX_BEGIN(pop) {
			TX_MEMCPY(pdata + plog_block->pmemaddr + plog_block->cur_off,
					log_src, size);

		}TX_ONABORT {
		}TX_END
	}
	plog_block->cur_off += size;
	plog_block->n_log_recs += n_recs;
	
	//Part 2: handle DPT
	
	// for each input log record
	for (i = 0; i < n_recs; i++) {
		//key = key_arr[i];
		LSN = LSN_arr[i];
		space_no = space_arr[i];
		page_no = page_arr[i];

		key = (space_no << 20) + space_no + page_no;
		assert (key == key_arr[i]);

		
		if (plog_block->n_dp_entries >= PMEM_TT_MAX_DIRTY_PAGES_PER_TX){
			printf("PMEM ERROR, in pm_ptxl_write(), n_dp_entries reach the max capacity %zu, change your setting \n", PMEM_TT_MAX_DIRTY_PAGES_PER_TX);
			assert(0);
		}

		//find the corresponding pageref with this log rec
		PMEM_PAGE_REF* pref;
		for (j = 0; j < plog_block->n_dp_entries; j++) {
			pref = D_RW(D_RW(plog_block->dp_array)[j]);

			if (pref->key == key){
				//pagref already exist
				break;
			}
		}	

		if (j < plog_block->n_dp_entries){
			//pageref already exist, update LSN
			pref->pageLSN = LSN;
		}
		else{
			//new pageref
			pdpt = D_RW(ptxl->dpt);
			assert (pdpt);

			//update the entry in DPT 
			//tdnguyen test
			//eid = 1;
			eid = __update_dpt_entry_on_write_log(pop, ptxl, plog_block->bid, key);

			//update metadata in this log block
			plog_block->count++;

			assert (j == plog_block->n_dp_entries);
			pref = D_RW(D_RW(plog_block->dp_array)[j]);

			pref->key = key;
			pref->idx = eid;
			pref->pageLSN = LSN;

			plog_block->n_dp_entries++;
		}

		//uint64_t dpt_ret = __handle_dpt_on_write_log(pop, ptxl, plog_block, key, LSN);

	}//end for each input log record

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
	//Collect statistic information
	if (block_id == -1){
		plog_block->start_time = ut_time_ms();
		//plog_block->all_n_reused++;
	}
	if (size <= CACHELINE_SIZE)
		plog_block->n_small_log_recs++;
	//Compute info for trx-lifetime 	
	STAT_CAL_AVG(
			plog_block->avg_log_rec_size,
			plog_block->n_log_recs,
			plog_block->avg_log_rec_size,
			size);

	if (size < plog_block->min_log_rec_size)
		plog_block->min_log_rec_size = size;
	
	if (size > plog_block->max_log_rec_size)
		plog_block->max_log_rec_size = size;

#endif

	pmemobj_rwlock_unlock(pop, &plog_block->lock);
	return ret;
}


/* If key is not exist: add new key 
 * Otherwise: update (increase count 1)
 *
 * At the first time added: set count to 1
 * When a transaction commit/abort decrease counter 1
 * pop (in): The pmemobjpop
 * pdpt (in): The pointer to DPT
 * bid (in): block log id that call this function
 * key (in): fold of space_id and page_no
 *
 * Return the index of the entry in DPT
 *
 * */
int64_t
__update_dpt_entry_on_write_log(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*	ptxl,
		uint64_t			bid,
		uint64_t			key) {

	ulint hashed;
	uint32_t n, k, i, j;
	int64_t free_idx;

	TOID(PMEM_DPT_HASHED_LINE) line;
	PMEM_DPT_HASHED_LINE* pline;

	TOID(PMEM_DPT_ENTRY) e;
	PMEM_DPT_ENTRY* pe;

	PMEM_DPT*			pdpt = D_RW(ptxl->dpt);

	n = pdpt->n_buckets;
	k = pdpt->n_entries_per_bucket;
	
	//(1) Get the hashed line
	PMEM_LOG_HASH_KEY(hashed, key, n);
	assert (hashed < n);
	
	TOID_ASSIGN(line, (D_RW(pdpt->buckets)[hashed]).oid);
	pline = D_RW(line);
	assert(pline);
	assert(pline->n_entries == k);

	/*(2) Sequential scan for the entry 
	 * if the entry is exist -> increase the counter
	 * otherwise: add new 
	 */
	//don't lock the hash line, slow performance
	//pmemobj_rwlock_wrlock(pop, &pline->lock);
	
	// Expensive O(k)	
	for (i = 0; i < k; i++) {
		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);

		TOID_ASSIGN(e, (D_RW(pline->arr)[i]).oid);
		pe = D_RW(e);
		assert(pe != NULL);
		if (!pe->is_free){
			if (pe->key == key){
				// Case A: the entry is existed, now we increase counter and add the tid to the txref list
				//
				//pmemobj_rwlock_wrlock(pop, &pe->lock);
				//increase the count
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
				if (pe->count == 0){
					//idle -> busy
					pline->n_idle--;
				}
#endif
				pe->count++;
				//search the first free txref to write on
				for (j = 0; j < pe->n_tx_idx; j++){
					if (D_RW(pe->tx_idx_arr)[j] == -1){
						break;
					}
				}
				/*Note that we don't update pe->pageLSN*/
				D_RW(pe->tx_idx_arr)[j] = bid;
				//increase the size
				if (j == pe->n_tx_idx)
					pe->n_tx_idx++;

				if (pe->n_tx_idx >= MAX_TX_PER_PAGE){
					printf("PMEM_ERROR in __update_DPT_entry_on_write_log() not enough txref in page entry \n");
					printf("print out debug info \n");
					for (j = 0; j < pe->n_tx_idx; j++){
						printf("txref #%zu ref to bid %zu\n", j, D_RW(pe->tx_idx_arr)[j]);
					}
					assert(0);
				}
				//pmemobj_rwlock_unlock(pop, &pline->lock);
				assert(pe->eid == (hashed * k + i));

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				//pmemobj_rwlock_unlock(pop, &pe->lock);
				return pe->eid;
			}
		}
		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
	}
	//we must seach for the free entry from the beginning because during the time this thread travel the previous for loop, some entries may be free
	
	free_idx = -1;
	//search for a free entry
	for (i = 0; i < k; i++) {
		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		TOID_ASSIGN(e, (D_RW(pline->arr)[i]).oid);
		pe = D_RW(e);
		assert(pe != NULL);
		if (pe->is_free){
			//Case B: new entry
			//pmemobj_rwlock_wrlock(pop, &pe->lock);

			free_idx = i;
			if (pe->n_tx_idx > 0){
				printf("PMEM_ERROR cur n_tx_idx %zu of a free entry should be 0, pe->count %zu , logical error \n", pe->n_tx_idx, pe->count);
				//pmemobj_rwlock_unlock(pop, &pe->lock);
				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				assert(0);
			}
			pe->is_free = false;
			pe->key = key;
			pe->count++;

			assert(pe->n_tx_idx == 0);
			/*Note that pe->pageLSN = 0; */
			D_RW(pe->tx_idx_arr)[pe->n_tx_idx] = bid;
			pe->n_tx_idx++;

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
			pline->n_free--;
#endif
			//pmemobj_rwlock_unlock(pop, &pe->lock);
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			return pe->eid;
		}
		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		//next
	} //end search for a free entry

	if (free_idx == -1) {
		//Now we optional wait for a little or simply treat as error
		printf("PMEM_ERROR in __update_dpt_entry_on_write_log(), line %zu no free entry\n", hashed);

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
		//We print the DPT to debug
		__print_DPT(pdpt, debug_ptxl_file);
		__print_tx_blocks_state(debug_ptxl_file, ptxl);
#endif
		assert(0);
		return PMEM_ERROR;
	}
}

/*
 * High level function called when transaction commit
 * pop (in): 
 * ptxl (in):
 * tid (in): transaction id
 * bid (in): block id, saved in the transaction
 * */
void
pm_ptxl_commit(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*	ptxl,
		uint64_t			tid,
		int64_t				bid)
{
	ulint hashed;
	uint64_t n, k, i;
	uint64_t bucket_id, local_id;
	bool is_reclaim;

	TOID(PMEM_TX_LOG_HASHED_LINE) line;
	PMEM_TX_LOG_HASHED_LINE* pline;

	TOID(PMEM_TX_LOG_BLOCK) log_block;
	PMEM_TX_LOG_BLOCK*	plog_block;

	PMEM_DPT* pdpt = D_RW(ptxl->dpt);
	PMEM_PAGE_REF* pref;

	n = ptxl->n_buckets;
	k = ptxl->n_blocks_per_bucket;
	
	// (1) Get the log block by block id
	bucket_id = bid / k;
	local_id = bid % k;

	TOID_ASSIGN(line, (D_RW(ptxl->buckets)[bucket_id]).oid);
	pline = D_RW(line);
	assert(pline);
	TOID_ASSIGN (log_block, (D_RW(pline->arr)[local_id]).oid);
	plog_block = D_RW(log_block);
	assert(plog_block);

	if (plog_block->tid != tid){
		printf("PMEM_TX_PART_LOG error in pm_ptxl_set_log_block_state(), block id %zu != tid %zu\n ", plog_block->tid, tid);
		assert(0);
	}

	pmemobj_rwlock_wrlock(pop, &plog_block->lock);
	
	//goto test_skip2;

	//(2) for each pageref in the log block
	for (i = 0; i < plog_block->n_dp_entries; i++) {
			pref = D_RW(D_RW(plog_block->dp_array)[i]);
			if (pref->idx >= 0){
				//update the corresponding DPT entry and try to reclaim it
				bool is_reclaim = __update_dpt_entry_on_commit(
						pop, pdpt, pref);
				if (is_reclaim){
					//invalid the page ref
					pref->key = 0;
					pref->idx = -1;
					pref->pageLSN = 0;
				}
			}

	} // end for each pageref in the log block	

//test_skip2:	
	// (3) update metadata
	plog_block->state = PMEM_COMMIT_LOG_BLOCK;
	// (4) Check for reclaim

	if (plog_block->count <= 0){
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)

		printf("+++ reset LOGBLOCK from COMMIT, bid %zu\n", plog_block->bid);
		fprintf(debug_ptxl_file, "+++ reset LOGBLOCK from COMMIT, bid %zu\n", plog_block->bid);
#endif
		__reset_tx_log_block(plog_block);
	}
//	test
//	__reset_tx_log_block(plog_block);

	pmemobj_rwlock_unlock(pop, &plog_block->lock);
}

/*
 * Reset the tx log block to reused in the next time
 * The caller must acquire the lock on this log block before reseting
 * */
void 
__reset_tx_log_block(PMEM_TX_LOG_BLOCK* plog_block)
{
	uint64_t i;
	PMEM_PAGE_REF* pref;

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
	plog_block->all_n_reused++;

	if (plog_block->all_max_log_buf_size < plog_block->cur_off)
		plog_block->all_max_log_buf_size = plog_block->cur_off;

	if(plog_block->all_max_n_pagerefs < plog_block->n_dp_entries)
		plog_block->all_max_n_pagerefs = plog_block->n_dp_entries;

#endif

	plog_block->tid = 0;
	plog_block->cur_off = 0;
	plog_block->n_log_recs = 0;
	plog_block->state = PMEM_FREE_LOG_BLOCK;
	plog_block->count = 0;

	//reset array of dirty pages but not deallocate
	for (i = 0; i < plog_block->n_dp_entries; i++){
		pref = D_RW(D_RW(plog_block->dp_array)[i]);
		pref->key = 0;
		pref->idx = -1;
		pref->pageLSN = 0;
	}
	
	plog_block->n_dp_entries = 0;
}

/* update the DPT when tx commit
 * called by pm_ptxl_commit()
 * decrease the counter 1 and try to reclaim the DPT entry
 *
 * return 
 * true : if we reclaim the entry
 * false: otherwise
 * */
bool
__update_dpt_entry_on_commit(
		PMEMobjpool*		pop,
		PMEM_DPT*			pdpt,
		PMEM_PAGE_REF* pref) {

	ulint hashed;
	uint32_t n, k, i;
	uint64_t line_id, local_id;

	assert(pref != NULL);
	assert(pref->idx >= 0);

	uint64_t key = pref->key;

	TOID(PMEM_DPT_HASHED_LINE) line;
	PMEM_DPT_HASHED_LINE* pline;

	TOID(PMEM_DPT_ENTRY) e;
	PMEM_DPT_ENTRY* pe;

	n = pdpt->n_buckets;
	k = pdpt->n_entries_per_bucket;
	
	//Get the entry by eid without hashing
	line_id = pref->idx / k;
	local_id = pref->idx % k;
	
	pline = D_RW(D_RW(pdpt->buckets)[line_id]);
	pe = D_RW(D_RW(pline->arr)[local_id]);

	assert(pe != NULL);
	assert(pe->is_free == false);

	if (pe->key != pref->key){
		printf("PMEM_ERROR in __update_dpt_entry_on_commit(), pe->key %zu != pref->key %zu\n", pe->key, pref->key);
		assert(0);
	}


	pmemobj_rwlock_wrlock(pop, &pe->lock);
	if (pe->count <= 0){
		printf("PMEM_ERROR in __update_dpt_entry_on_commit(), entry count is already zero, cannot reduce more. This is logical error!!!\n");
		pmemobj_rwlock_unlock(pop, &pe->lock);
		assert(0);
		return false;
	}
	//decrease the count
	pe->count--;

	//Reclaim the entry if:
	//(1) there is no active transaction access on this page (pe->count == 0), AND
	//(2) the pmem page has newer or equal version than the pmem log
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
	if (pe->count <= 0){
		//busy -> idle
		pline->n_idle++;
	}
#endif
	if (pe->count <= 0 &&
		pref->pageLSN <= pe->pageLSN){
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
		printf("+++ reset DPT entry from COMMIT, eid %zu\n", pe->eid);
		fprintf(debug_ptxl_file, "+++ reset DPT entry from COMMIT, eid %zu\n", pe->eid);
#endif
		__reset_DPT_entry(pe);
		pmemobj_rwlock_unlock(pop, &pe->lock);
		return true;
	}
	else {
		pmemobj_rwlock_unlock(pop, &pe->lock);
		return false;
	}
}

/* reset (reclaim) a DPT entry 
 * The caller must acquired the entry lock before reseting
 * */
void 
__reset_DPT_entry(PMEM_DPT_ENTRY* pe) 
{
	uint64_t i;


#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
	pe->n_reused++;
	if (pe->max_txref_size < pe->n_tx_idx)
		pe->max_txref_size = pe->n_tx_idx;
#endif

	pe->is_free = true;
	pe->key = 0;
	pe->count = 0;
	pe->pageLSN = 0;

	for (i = 0; i < pe->n_tx_idx; i++){
		D_RW(pe->tx_idx_arr)[i] = -1;
	}	
	pe->n_tx_idx = 0;
}

/*
 * Called when the buffer pool flush page to PB-NVM
 *
 * key (in): the fold of space_id and page_no
 * pageLSN (in): pageLSN in the header of the flushed page
 * */
void 
pm_ptxl_on_flush_page(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*	ptxl,
		uint64_t			key,
		uint64_t			pageLSN)
{
	ulint hashed;
	uint32_t n, k, i, j;

	int64_t free_idx;

	TOID(PMEM_DPT_HASHED_LINE) line;
	PMEM_DPT_HASHED_LINE* pline;

	TOID(PMEM_DPT_ENTRY) e;
	PMEM_DPT_ENTRY* pe;

	PMEM_DPT* pdpt = D_RW(ptxl->dpt);

	n = pdpt->n_buckets;
	k = pdpt->n_entries_per_bucket;

	//(1) Get the hashed line
	PMEM_LOG_HASH_KEY(hashed, key, n);
	assert (hashed < n);
	
	TOID_ASSIGN(line, (D_RW(pdpt->buckets)[hashed]).oid);
	pline = D_RW(line);
	assert(pline);
	assert(pline->n_entries == k);
	
	//(2) Get the entry in the hashed line
	//for each entry in the hashline
	for (i = 0; i < k; i++) {
		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		TOID_ASSIGN(e, (D_RW(pline->arr)[i]).oid);
		pe = D_RW(e);
		assert(pe != NULL);
		if (!pe->is_free){
			if (pe->key == key){
				//pmemobj_rwlock_wrlock(pop, &pe->lock);
				//for each tids in entry
				for (j = 0; j < pe->n_tx_idx; j++){
					int64_t tx_idx = D_RW(pe->tx_idx_arr)[j];
					//only reclaim valid txref
					if (tx_idx >= 0){
						bool is_reclaim = __check_and_reclaim_tx_log_block(
								pop,
								ptxl,
								tx_idx);
						if (is_reclaim){
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
					printf("+++ reset LOGBLOCK from flush, bid %zu\n", tx_idx);
					fprintf(debug_ptxl_file, "+++ reset LOGBLOCK from flush, bid %zu\n", tx_idx);
#endif
							D_RW(pe->tx_idx_arr)[j] = -1;
						}
					}
				} //end for each tids in the entry

				pe->pageLSN = pageLSN;
				//Check to reclaim this entry
				if (pe->count <= 0){
					//since pe->pageLSN now is the largest LSN in the page, we don't check the LSN condition anymore
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
//					printf("+++ reset DPT from flush, entry eid %zu count %zu\n", pe->eid, pe->count);
//					fprintf(debug_ptxl_file, "+++ reset DPT from flush, entry eid %zu count %zu\n", pe->eid, pe->count);
#endif
					__reset_DPT_entry(pe);
				}
				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				//pmemobj_rwlock_unlock(pop, &pe->lock);
				return;
			}
		}
		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		//next
	}//end for entry in the hashed line
	
	/*similar to __update_dpt_entry_on_write_log() we should searching for the free entry from the beginning*/

	free_idx = -1;
	//search for a free entry
	for (i = 0; i < k; i++) {
		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		TOID_ASSIGN(e, (D_RW(pline->arr)[i]).oid);
		pe = D_RW(e);
		assert(pe != NULL);
		if (pe->is_free){
			//pmemobj_rwlock_wrlock(pop, &pe->lock);
			free_idx = i;

			pe->is_free = false;
			pe->key = key;
			assert(pe->n_tx_idx == 0);	
			/*Note that still pe->count = 0; */
			assert(pe->count == 0);
			pe->pageLSN = pageLSN;

			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			//pmemobj_rwlock_unlock(pop, &pe->lock);
			return;
		}
		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		
	} //end search for a free entry

	if (free_idx == -1){
		//Now we optional wait for a little or simply treat as error
		printf("PMEM_ERROR in pm_ptxl_on_flush_page(), no free entry on line %zu\n", hashed);
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
		//We print the DPT to debug
		__print_DPT(pdpt, debug_ptxl_file);
		__print_tx_blocks_state(debug_ptxl_file, ptxl);
#endif
		assert(0);
	}
}

/*
 * Check and reclaim a log block with input block id
 * pm_ptxl_on_flush_page() call this function for each txref in the DPT entry
 * pop (in):
 * ptxl (in):
 * block_id (in): block id of the tx log block
 *
 * Return:
 * true: if reclaim the log block
 * false: otherwise
 * */
bool
__check_and_reclaim_tx_log_block(
		PMEMobjpool*		pop,
		PMEM_TX_PART_LOG*	ptxl,
		int64_t			block_id)
{
	uint64_t n, k;
	uint64_t bucket_id, local_id;

	PMEM_TX_LOG_HASHED_LINE* pline;
	PMEM_TX_LOG_BLOCK*	plog_block;

	assert(block_id >= 0);

	n = ptxl->n_buckets;
	k = ptxl->n_blocks_per_bucket;

	bucket_id = block_id / k;
	local_id = block_id % k;

	pline = D_RW(D_RW(ptxl->buckets)[bucket_id]);
	plog_block = D_RW(D_RW(pline->arr)[local_id]);
	
	//Reduce the dirty page counter and reset the logblock if the counter le 0
	pmemobj_rwlock_wrlock(pop, &plog_block->lock);

	assert(plog_block->bid == block_id);

	plog_block->count--;

	if (plog_block->state == PMEM_COMMIT_LOG_BLOCK) {
		if (plog_block->count <= 0) {
			__reset_tx_log_block(plog_block);
			pmemobj_rwlock_unlock(pop, &plog_block->lock);
			return true;
		}
		else {
			pmemobj_rwlock_unlock(pop, &plog_block->lock);
			return false;
		}
	}
	else{
		pmemobj_rwlock_unlock(pop, &plog_block->lock);
		return false;
	}
}

/*
 * Call on propagation
 * Check an entry with given key
 * If that entry is an IDLE => set free and return true
 * Otherwise: return false
 * */
bool
pm_ptxl_check_and_reset_dpt_entry(
		PMEMobjpool*		pop,
		PMEM_DPT*			pdpt,
		uint64_t			key) {

	ulint hashed;
	uint32_t n, k, i;

	TOID(PMEM_DPT_HASHED_LINE) line;
	PMEM_DPT_HASHED_LINE* pline;

	TOID(PMEM_DPT_ENTRY) e;
	PMEM_DPT_ENTRY* pe;

	n = pdpt->n_buckets;
	k = pdpt->n_entries_per_bucket;

	//(1) Get the hashed line
	PMEM_LOG_HASH_KEY(hashed, key, n);
	assert (hashed < n);
	
	TOID_ASSIGN(line, (D_RW(pdpt->buckets)[hashed]).oid);
	pline = D_RW(line);
	assert(pline);
	assert(pline->n_entries == k);

	/*(2) Sequential scan for the entry 
	 * if the entry is exist -> decrease the counter
	 * otherwise: ERROR
	 */
	//lock the hash line
	pmemobj_rwlock_wrlock(pop, &pline->lock);

	for (i = 0; i < k; i++) {
		TOID_ASSIGN(e, (D_RW(pline->arr)[i]).oid);
		pe = D_RW(e);
		assert(pe != NULL);
		if (!pe->is_free &&
			pe->key == key &&
			pe->count == 0){
					//reset the entry
					pe->is_free = true;
					pe->key = 0;
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
					//idle --> free
					pline->n_free++;
					pline->n_idle--;
#endif

					pmemobj_rwlock_unlock(pop, &pline->lock);
					return true;
		}
	}
	pmemobj_rwlock_unlock(pop, &pline->lock);
	return false;
}

////////////////// PERPAGE LOGGING ///////////////

PMEM_PAGE_PART_LOG* 
pm_pop_get_ppl (PMEMobjpool* pop)
{
	TOID(PMEM_PAGE_PART_LOG) pl;
	PMEM_PAGE_PART_LOG* ppl;

	pl = POBJ_FIRST(pop, PMEM_PAGE_PART_LOG);
	ppl = D_RW(pl);
	if (ppl == NULL){
		printf("PMEM ERROR in pm_pop_get_ppl(), ppl is NULL \n");
		assert(0);
	}

	return ppl;	
}

/*
 * Allocate part page log and its components
 * Read config variable from my.cnf 
 * */
void
pm_wrapper_page_log_alloc_or_open(
		PMEM_WRAPPER*	pmw
		) 
{
	
	// Get const variable from parameter and config value
	uint64_t n_log_bufs;
	uint64_t n_free_log_bufs;

	PMEM_N_LOG_BUCKETS = srv_ppl_n_log_buckets;
	PMEM_N_BLOCKS_PER_BUCKET = srv_ppl_blocks_per_bucket;
	PMEM_LOG_BUF_SIZE = srv_ppl_log_buf_size;
	PMEM_TT_N_LINES = srv_ppl_tt_n_lines;
	PMEM_TT_N_ENTRIES_PER_LINE = srv_ppl_tt_entries_per_line;
	PMEM_TT_MAX_DIRTY_PAGES_PER_TX = srv_ppl_tt_pages_per_tx;
/*Flush Log*/
	PMEM_LOG_BUF_FLUSH_PCT = srv_ppl_log_buf_flush_pct;
	PMEM_LOG_FLUSHER_WAKE_THRESHOLD = srv_ppl_log_flusher_wake_threshold;
	PMEM_N_LOG_FLUSH_THREADS = srv_ppl_n_log_flush_threads;
/*Log files*/
	PMEM_LOG_FILE_SIZE = srv_ppl_log_file_size; //in 4-KB pages (64MB)
	PMEM_N_LOG_FILES_PER_BUCKET = srv_ppl_log_files_per_bucket;
	
	n_free_log_bufs = PMEM_N_LOG_BUCKETS / 4;
	n_log_bufs = PMEM_N_LOG_BUCKETS + n_free_log_bufs;

	/* Part 1: NVDIMM structures*/	

	if (!pmw->ppl) {
		pmw->ppl = alloc_pmem_page_part_log(
				pmw->pop,
				PMEM_N_LOG_BUCKETS,
				PMEM_N_BLOCKS_PER_BUCKET,
				n_log_bufs,
				PMEM_LOG_BUF_SIZE);

		if (pmw->ppl == NULL){
			printf("PMEMOBJ_ERROR: error when allocate buffer in pm_wrapper_page_log_alloc_or_open()\n");
			exit(0);
		}
		printf("\n=================================\n Footprint of PAGE part-log:\n");
		printf("Log area %zu x %zu (B) = \t\t %f (MB) \n", n_log_bufs, PMEM_LOG_BUF_SIZE, (n_log_bufs * PMEM_LOG_BUF_SIZE * 1.0)/(1024*1024) );
		printf("PAGE-Log metadata: %zu x %zu = \t %f (MB)\n", PMEM_N_LOG_BUCKETS, PMEM_N_BLOCKS_PER_BUCKET, (pmw->ppl->pmem_page_log_size * 1.0)/(1024*1024));
		printf("TT metadata, %zu x %zu x %zu = \t\t %f (MB)\n", PMEM_TT_N_LINES, PMEM_TT_N_ENTRIES_PER_LINE, PMEM_TT_MAX_DIRTY_PAGES_PER_TX, (pmw->ppl->pmem_tt_size * 1.0) / (1024*1024));
		printf("Total NVDIMM allocated = \t\t %f (MB)\n", (pmw->ppl->pmem_alloc_size * 1.0)/ (1024*1024));
		float log_file_size_MB = PMEM_LOG_FILE_SIZE * 4 * 1024 * 1.0 / (1024 * 1024);
		printf("Log files %zu x %f (MB) = \t %f (MB)\n", PMEM_N_LOG_BUCKETS, log_file_size_MB, (PMEM_N_LOG_BUCKETS * log_file_size_MB));
		printf(" =================================\n");
	}
	else {
		//Case 2: Reused a buffer in PMEM
		printf("!!!!!!! [PMEMOBJ_INFO]: the server restart from a crash but the per-page log buffers are persist\n");
		//We need to re-align the p_align
		byte* p;
		p = static_cast<byte*> (pmemobj_direct(pmw->ppl->data));
		assert(p);
		pmw->ppl->p_align = static_cast<byte*> (ut_align(p, PMEM_LOG_BUF_SIZE));

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
		//print the hashed line to check
		__print_page_log_hashed_lines(debug_ptxl_file, pmw->ppl);
#endif

	}
	/* Part 2: DRAM structures*/	

	// In any case (new alloc or reused) we need to allocate below objects

	// defined in my_pmemobj.h, implement in buf0flu.cc
	pmw->ppl->flusher = pm_log_flusher_init(PMEM_N_LOG_FLUSH_THREADS);

	pmw->ppl->free_log_pool_event = os_event_create("pm_free_log_pool_event");

	pmw->ppl->deb_file = fopen("part_log_debug.txt","a");
}

void
pm_wrapper_page_log_close(
		PMEM_WRAPPER*	pmw	)
{
	ulint i;
	fil_node_t* node;
	
	PMEM_PAGE_PART_LOG* ppl = pmw->ppl;
	
	//Free resource allocated in DRAM
	os_event_destroy(ppl->free_log_pool_event);
	pm_log_flusher_close(ppl->flusher);
	
	pm_close_and_free_log_files(pmw->ppl);	
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)	
//	__print_page_log_hashed_lines(debug_ptxl_file, pmw->ppl);
#endif

#if defined(UNIV_PMEMOBJ_PPL_STAT)
	__print_lock_overhead(lock_overhead_file, pmw->ppl);
#endif	
	//the resource allocated in NVDIMM are kept
}
/*
 * Allocate per-page logs and TT
 * */
PMEM_PAGE_PART_LOG* alloc_pmem_page_part_log(
		PMEMobjpool*	pop,
		uint64_t		n_buckets,
		uint64_t		n_blocks_per_bucket,
		uint64_t		n_log_bufs,
		uint64_t		log_buf_size) {

	char* p;
	size_t align_size;
	uint64_t n;
	uint64_t i;

	uint64_t log_buf_id;
	uint64_t log_buf_offset;

	assert(n_log_bufs > n_buckets);

	uint64_t size = n_log_bufs * log_buf_size;
	uint64_t n_free_log_bufs = n_log_bufs - n_buckets;

	TOID(PMEM_PAGE_PART_LOG) pl; 

	POBJ_ZNEW(pop, &pl, PMEM_PAGE_PART_LOG);
	PMEM_PAGE_PART_LOG* ppl = D_RW(pl);

	ppl->pmem_alloc_size = sizeof(PMEM_PAGE_PART_LOG);

	//(1) Allocate and alignment for the log data
	//align sizes to a pow of 2
	assert(ut_is_2pow(log_buf_size));
	align_size = ut_uint64_align_up(size, log_buf_size);

	ppl->size = align_size;
	ppl->log_buf_size = log_buf_size;
	ppl->n_log_bufs = n_log_bufs;
	ppl->n_buckets = n_buckets;
	ppl->n_blocks_per_bucket = n_blocks_per_bucket;

	ppl->n_log_files_per_bucket = PMEM_N_LOG_FILES_PER_BUCKET;
	ppl->log_file_size = PMEM_LOG_FILE_SIZE;

	ppl->is_new = true;
	ppl->data = pm_pop_alloc_bytes(pop, align_size);

	ppl->pmem_alloc_size += align_size;

	//align the pmem address for DIRECT_IO
	p = static_cast<char*> (pmemobj_direct(ppl->data));
	assert(p);

	ppl->p_align = static_cast<byte*> (ut_align(p, log_buf_size));
	pmemobj_persist(pop, ppl->p_align, sizeof(*ppl->p_align));

	if (OID_IS_NULL(ppl->data)){
		return NULL;
	}
	
	log_buf_id = 0;
	log_buf_offset = 0;

	//(2) init the buckets
	pm_page_part_log_bucket_init(
			pop,
		   	ppl,
		   	n_buckets,
			n_blocks_per_bucket,
			log_buf_size,
			log_buf_id,
			log_buf_offset
			);

	ppl->pmem_alloc_size += ppl->pmem_page_log_size;
	//(3) Free Pool
	__init_page_log_free_pool(
			pop,
			ppl,
			n_free_log_bufs,
			log_buf_size,
			log_buf_id,
			log_buf_offset);

	ppl->pmem_alloc_size += ppl->pmem_page_log_free_pool_size;
	
	//(4) transaction table
	__init_tt(
			pop,
		   	ppl,
		   	PMEM_TT_N_LINES,
		   	PMEM_TT_N_ENTRIES_PER_LINE,
			PMEM_TT_MAX_DIRTY_PAGES_PER_TX);

	ppl->pmem_alloc_size += ppl->pmem_tt_size;

	pmemobj_persist(pop, ppl, sizeof(*ppl));
	return ppl;
}

/*
 * Init TT with n hashed lines, k entries per line
 * k is the load factor 
 * */
void 
__init_tt(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t				n,
		uint64_t				k,
		uint64_t				pages_per_tx) {

	uint64_t i, j, i_temp;	
	PMEM_TT* ptt;
	PMEM_TT_HASHED_LINE *pline;
	PMEM_TT_ENTRY* pe;

	POBJ_ZNEW(pop, &ppl->tt, PMEM_TT);
	ptt = D_RW(ppl->tt);
	assert (ptt);
	
	ppl->pmem_tt_size = sizeof(PMEM_TT);

	//allocate the buckets (hashed lines)
	ptt->n_buckets = n;
	ptt->n_entries_per_bucket = k;
	ptt->n_pref_per_entry = pages_per_tx;

	//the special bucket
	POBJ_ZNEW(pop,
			&ptt->spec_bucket,
			PMEM_TT_HASHED_LINE);

	ppl->pmem_tt_size += sizeof(PMEM_TT_HASHED_LINE);

	pline = D_RW(ptt->spec_bucket);

	//spec TT line has hashed_id equals n
	__init_tt_entry(pop, ppl, pline,
			n, k, pages_per_tx);

	//the remain buckets
	POBJ_ALLOC(pop,
				&ptt->buckets,
				TOID(PMEM_TT_HASHED_LINE),
				sizeof(TOID(PMEM_TT_HASHED_LINE)) * n,
				NULL,
				NULL);

	ppl->pmem_tt_size += sizeof(TOID(PMEM_TT_HASHED_LINE)) * n;

	if (TOID_IS_NULL(ptt->buckets)) {
		fprintf(stderr, "POBJ_ALLOC\n");
	}
	
	//for each hash line
	for (i = 0; i < n; i++) {
		POBJ_ZNEW(pop,
				&D_RW(ptt->buckets)[i],
				PMEM_TT_HASHED_LINE);

		ppl->pmem_tt_size += sizeof(PMEM_TT_HASHED_LINE);

		if (TOID_IS_NULL(D_RW(ptt->buckets)[i])) {
			fprintf(stderr, "POBJ_ALLOC\n");
		}

		pline = D_RW(D_RW(ptt->buckets)[i]);
		
		__init_tt_entry(pop, ppl, pline,
				i, k, pages_per_tx);

	}// end for each line
}

void __init_tt_entry(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_TT_HASHED_LINE*	pline,
		uint64_t hashed_id,
		uint64_t n_entries,
		uint64_t pages_per_tx)
{
		uint64_t i, j, k, i_temp;
		PMEM_TT_ENTRY* pe;

		i = hashed_id;
		k = n_entries;

		pline->hashed_id = hashed_id;
		pline->n_entries = n_entries;

		//Allocate the entries
		POBJ_ALLOC(pop,
				&pline->arr,
				TOID(PMEM_TT_ENTRY),
				sizeof(TOID(PMEM_TT_ENTRY)) * n_entries,
				NULL,
				NULL);

		ppl->pmem_tt_size += sizeof(TOID(PMEM_TT_ENTRY))* n_entries;
		//for each entry in the line
		for (j = 0; j < k; j++) {
			POBJ_ZNEW(pop,
					&D_RW(pline->arr)[j],
					PMEM_TT_ENTRY);

			ppl->pmem_tt_size += sizeof(PMEM_TT_ENTRY);

			if (TOID_IS_NULL(D_RW(pline->arr)[j])) {
				fprintf(stderr, "POBJ_ZNEW\n");
			}

			pe = D_RW(D_RW(pline->arr)[j]);

			pe->state = PMEM_TX_FREE;
			pe->tid = 0;
			pe->eid = i * k + j;

			//dp bid array
			POBJ_ALLOC(pop,
					&pe->dp_arr,
					TOID(PMEM_PAGE_REF),
					sizeof(TOID(PMEM_PAGE_REF)) * pages_per_tx,
					NULL,
					NULL);

			ppl->pmem_tt_size += sizeof(TOID(PMEM_PAGE_REF)) * pages_per_tx;

			for (i_temp = 0; i_temp < pages_per_tx; i_temp++){
				POBJ_ZNEW(pop,
					&D_RW(pe->dp_arr)[i_temp],
				PMEM_PAGE_REF);	

				D_RW(D_RW(pe->dp_arr)[i_temp])->key = 0;
				D_RW(D_RW(pe->dp_arr)[i_temp])->idx = -1;
				D_RW(D_RW(pe->dp_arr)[i_temp])->pageLSN = 0;
			}

			pe->n_dp_entries = 0;
			pe->max_dp_entries = pages_per_tx;
			ppl->pmem_tt_size += sizeof(PMEM_PAGE_REF) * pages_per_tx;
		} //end for each entry
}

void __reset_TT_entry(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_TT_ENTRY* pe)
{
	uint64_t i;
	PMEM_PAGE_REF* pref;

	PMEM_TT* ptt = D_RW(ppl->tt);

	pe->tid = 0;
	pe->state = PMEM_TX_FREE;

	//TODO: what happend for pe that is realloc?
	// (pe->max_dp_entries > ptt->n_pref_per_entry)
	
	for (i = 0; i < pe->n_dp_entries; i++) {
		pref = D_RW(D_RW(pe->dp_arr)[i]);
		pref->key = 0;
		pref->idx = -1;
		pref->pageLSN = 0;
	}

	pe->n_dp_entries = 0;
}

/*
 * reallocate a full pe
 * pop <in>
 * ppl <in>
 * pe <in/out>
 * new_size <in>
 * */
void __realloc_TT_entry(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_TT_ENTRY*			pe,
		uint64_t				new_size)
{
	uint64_t i;
	
	//we only reallocate up
	assert (pe->max_dp_entries < new_size);

	POBJ_REALLOC(pop,
			&pe->dp_arr,
			TOID(PMEM_PAGE_REF),
			sizeof(TOID(PMEM_PAGE_REF)) * new_size);

	if (pe->max_dp_entries < new_size){
		
		for (i = pe->max_dp_entries; i < new_size; i++){
			POBJ_ZNEW(pop,
					&D_RW(pe->dp_arr)[i],
					PMEM_PAGE_REF);	

			D_RW(D_RW(pe->dp_arr)[i])->key = 0;
			D_RW(D_RW(pe->dp_arr)[i])->idx = -1;
			D_RW(D_RW(pe->dp_arr)[i])->pageLSN = 0;
		}

		ppl->pmem_tt_size += sizeof(PMEM_PAGE_REF) * (new_size - pe->max_dp_entries);
		pe->max_dp_entries = new_size;


	}
}

void 
pm_page_part_log_bucket_init(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		pl,
		uint64_t				n_buckets,
		uint64_t				n_blocks_per_bucket,
		uint64_t				log_buf_size,
		uint64_t				&log_buf_id,
		uint64_t				&log_buf_offset) {

	uint64_t i, j, n, k;
	uint64_t cur_bucket;
	size_t offset;

	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	offset = 0;
	cur_bucket = 0;

	n = n_buckets;
	k = n_blocks_per_bucket;

	//allocate the pointer array buckets
	POBJ_ALLOC(pop,
			&pl->buckets,
			TOID(PMEM_PAGE_LOG_HASHED_LINE),
			sizeof(TOID(PMEM_PAGE_LOG_HASHED_LINE)) * n,
			NULL,
			NULL);
	if (TOID_IS_NULL(pl->buckets)) {
		fprintf(stderr, "POBJ_ALLOC\n");
	}

	pl->pmem_page_log_size = sizeof(TOID(PMEM_PAGE_LOG_HASHED_LINE)) * n;

	//for each hashed line
	for (i = 0; i < n; i++) {
		POBJ_ZNEW(pop,
				&D_RW(pl->buckets)[i],
				PMEM_PAGE_LOG_HASHED_LINE);
		if (TOID_IS_NULL(D_RW(pl->buckets)[i])) {
			fprintf(stderr, "POBJ_ZNEW\n");
		}

		pl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_HASHED_LINE);
		pline = D_RW(D_RW(pl->buckets)[i]);

		pline->hashed_id = i;
		pline->n_blocks = k;
		pline->diskaddr = 0;
		pline->write_diskaddr = 0;

		TOID_ASSIGN(pline->flush_logbuf, OID_NULL);
#if defined(UNIV_PMEMOBJ_PPL_STAT)
		pline->log_write_lock_wait_time = 0;
		pline->n_log_write = 0;
		pline->log_flush_lock_wait_time = 0;
		pline->n_log_flush = 0;
#endif	

		//Log buf, after the alloca call, log_buf_id and log_buf_offset increase
		POBJ_ZNEW(pop, &pline->logbuf, PMEM_PAGE_LOG_BUF);
		pl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_BUF);

		PMEM_PAGE_LOG_BUF* plogbuf = D_RW(pline->logbuf);

		plogbuf->pmemaddr = log_buf_offset;
		log_buf_offset += log_buf_size;

		plogbuf->id = log_buf_id;

		//save hashed_id 
		plogbuf->hashed_id = i;
		log_buf_id++;

		plogbuf->state = PMEM_LOG_BUF_FREE;
		plogbuf->size = log_buf_size;
		//plogbuf->cur_off = 0;
		plogbuf->cur_off = PMEM_LOG_BUF_HEADER_SIZE;
		plogbuf->self = (pline->logbuf).oid;
		plogbuf->check = PMEM_AIO_CHECK;


		//Allocate the log blocks
		POBJ_ALLOC(pop,
				&pline->arr,
				TOID(PMEM_PAGE_LOG_BLOCK),
				sizeof(TOID(PMEM_PAGE_LOG_BLOCK)) * k,
				NULL,
				NULL);
		pl->pmem_page_log_size += sizeof(TOID(PMEM_PAGE_LOG_BLOCK)) * k;
		//for each log block
		for (j = 0; j < k; j++) {
			POBJ_ZNEW(pop,
					&D_RW(pline->arr)[j],
					PMEM_PAGE_LOG_BLOCK);
			if (TOID_IS_NULL(D_RW(pline->arr)[j])) {
				fprintf(stderr, "POBJ_ZNEW\n");
			}
			pl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_BLOCK);
			plog_block = D_RW(D_RW(pline->arr)[j]);

			plog_block->is_free = true;
			plog_block->bid = i * k + j;
			plog_block->key = 0;
			plog_block->count = 0;

			//plog_block->cur_size = 0;
			//plog_block->n_log_recs = 0;

			plog_block->pageLSN = 0;
			plog_block->lastLSN = 0;
			plog_block->start_off = 0;
			plog_block->start_diskaddr = 0;
		}//end for each log block
	}//end for each hashed line
}

/*
 * Allocate the free pool of log bufs
 * */
void 
__init_page_log_free_pool(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t				n_free_log_bufs,
		uint64_t				log_buf_size,
		uint64_t				&log_buf_id,
		uint64_t				&log_buf_offset
		)
{
	uint64_t i;
	PMEM_PAGE_LOG_FREE_POOL* pfreepool;
	TOID(PMEM_PAGE_LOG_BUF) logbuf;
	PMEM_PAGE_LOG_BUF* plogbuf;

	
	POBJ_ZNEW(pop, &ppl->free_pool, PMEM_PAGE_LOG_FREE_POOL);
	if (TOID_IS_NULL(ppl->free_pool)){
		fprintf(stderr, "POBJ_ALLOC\n");
	}	

	ppl->pmem_page_log_free_pool_size = sizeof(PMEM_PAGE_LOG_FREE_POOL);

	pfreepool = D_RW(ppl->free_pool);
	pfreepool->max_bufs = n_free_log_bufs;

	pfreepool->cur_free_bufs = 0;
	for (i = 0; i < n_free_log_bufs; i++) {

		//alloc the log buf
		POBJ_ZNEW(pop, &logbuf, PMEM_PAGE_LOG_BUF);
		ppl->pmem_page_log_free_pool_size += sizeof(PMEM_PAGE_LOG_BUF);

		plogbuf = D_RW(logbuf);

		plogbuf->pmemaddr = log_buf_offset;
		log_buf_offset += log_buf_size;

		plogbuf->id = log_buf_id;
		log_buf_id++;

		plogbuf->hashed_id = -1;

		plogbuf->state = PMEM_LOG_BUF_FREE;
		plogbuf->size = log_buf_size;
		//plogbuf->cur_off = 0;
		plogbuf->cur_off = PMEM_LOG_BUF_HEADER_SIZE;
		plogbuf->self = logbuf.oid;
		plogbuf->check = PMEM_AIO_CHECK;

		//insert to the free pool list
		POBJ_LIST_INSERT_HEAD(pop, &pfreepool->head, logbuf, list_entries); 
		pfreepool->cur_free_bufs++;

	}

	pmemobj_persist(pop, &ppl->free_pool, sizeof(ppl->free_pool));
}

/*
 * Write REDO log records from mtr's heap to PMEM in partitioned-log   
 *
 * Return the entry id of TT  write on
 * */
int64_t
pm_ppl_write(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			tid,
			byte*				log_src,
			//mtr_buf_t*			dyn_buf,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			LSN_arr,
			uint64_t*			size_arr,
			int64_t				entry_id)
{
	int64_t ret;

	ulint hashed;
	ulint i, j;

	uint64_t n, k;
	uint64_t bucket_id, local_id;
	uint64_t n_try;


	int64_t block_id, new_block_id;

	PMEM_TT* ptt;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;

	TOID(PMEM_TT_ENTRY) entry;
	PMEM_TT_ENTRY* pe;

	
	//Starting from TT is easier and faster
	ptt = D_RW(ppl->tt);

	assert(ptt != NULL);
	assert(n_recs > 0);

	n = ptt->n_buckets;
	k = ptt->n_entries_per_bucket;
	
	if (entry_id == -2){
		ret = entry_id;

		//the special write with trx 0
		local_id = 0; //the local_id always is 0

		pline = D_RW(ptt->spec_bucket);

		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[local_id])->lock);

		TOID_ASSIGN (entry, (D_RW(pline->arr)[local_id]).oid);
		pe = D_RW(entry);
		if (pe->state == PMEM_TX_FREE){
			pe->state = PMEM_TX_ACTIVE;
			pe->tid = tid;

			__handle_pm_ppl_write_by_entry(
					pop,
					ppl,
					tid,
					log_src,
					//dyn_buf,
					size,
					n_recs,
					key_arr,
					LSN_arr,
					size_arr,
					pe,
					true);
		} else {
			assert(pe->state == PMEM_TX_ACTIVE);
			assert(pe->tid == tid);

		__handle_pm_ppl_write_by_entry(
				pop,
				ppl,
				tid,
				log_src,
				//dyn_buf,
				size,
				n_recs,
				key_arr,
				LSN_arr,
				size_arr,
				pe,
				false);
		}
		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[local_id])->lock);
		return ret;
	} //end special case
	else if (entry_id == -1) {
		//printf("BEGIN case A add log for tid %zu at entry %zu n_recs %zu\n", tid,  entry_id, n_recs);
		//Case A: A first log write of this transaction
		//Search the right entry to write
		//(1) Hash the tid
		PMEM_LOG_HASH_KEY(hashed, tid, n);
		assert (hashed < n);

		TOID_ASSIGN(line, (D_RW(ptt->buckets)[hashed]).oid);
		pline = D_RW(line);
		assert(pline);
		assert(pline->n_entries == k);

		//(2) Search for the free entry to write on
		//lock the hash line
		//pmemobj_rwlock_wrlock(pop, &pline->lock);

		n_try = pline->n_entries;
		//Choose the starting point as the hashed value, to avoid contention
		
		PMEM_LOG_HASH_KEY(i, tid, pline->n_entries);

		while (i < pline->n_entries){
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			TOID_ASSIGN (entry, (D_RW(pline->arr)[i]).oid);
			pe = D_RW(entry);
			if (pe->state == PMEM_TX_FREE){
				//found a free entry to write on
				pe->state = PMEM_TX_ACTIVE;
				pe->tid = tid;
				ret = hashed * k + i;
				
				__handle_pm_ppl_write_by_entry(
						pop,
						ppl,
						tid,
						log_src,
						//dyn_buf,
						size,
						n_recs,
						key_arr,
						LSN_arr,
						size_arr,
						pe,
						true);

				//handle update log
				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
	//printf("END case A add log for tid %zu at entry %zu n_recs %zu ret %zu \n", tid,  entry_id, n_recs, ret);
				return ret;
			}	
			n_try--;
			if (n_try == 0){
				printf("===> PMEM ERROR, in pm_ppl_write() there is no empty entry to write, allocate more to solve at bucket %zu \n", hashed);
				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
				__print_TT(debug_ptxl_file, ptt);
				__print_page_blocks_state(debug_ptxl_file, ppl);
#endif

				assert(0);
			}

			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			//jump a litte far to avoid contention
			i = (i + JUMP_STEP) % pline->n_entries;
			//try again
		}
		//unlock the hash line
		//pmemobj_rwlock_unlock(pop, &pline->lock);
	}//end Case A
	else {
		//Case B: Get the entry from the input id
		ret = entry_id;

		bucket_id = entry_id / k;
		local_id = entry_id % k;

		//printf("BEGIN case B add log for tid %zu at entry %zu n_recs %zu bucket_id %zu local_id %zu \n", tid,  entry_id, n_recs, bucket_id, local_id);

		TOID_ASSIGN(line, (D_RW(ptt->buckets)[bucket_id]).oid);
		pline = D_RW(line);
		assert(pline);
		
		//pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[bucket_id])->lock);
		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[local_id])->lock);
		TOID_ASSIGN (entry, (D_RW(pline->arr)[local_id]).oid);
		pe = D_RW(entry);

		assert(pe->state == PMEM_TX_ACTIVE);
		assert(pe->tid == tid);

		__handle_pm_ppl_write_by_entry(
				pop,
				ppl,
				tid,
				log_src,
				//dyn_buf,
				size,
				n_recs,
				key_arr,
				LSN_arr,
				size_arr,
				pe,
				false);

		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[bucket_id])->lock);
		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[local_id])->lock);

	//printf("END case B add log for tid %zu at entry %zu n_recs %zu bucket_id %zu local_id %zu \n", tid,  entry_id, n_recs, bucket_id, local_id);
		return ret;
	}//end case B
}

PMEM_PAGE_LOG_BLOCK*
__get_log_block_by_id(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			bid)
{
	uint64_t n, k;
	uint64_t bucket_id, local_id;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;


	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	bucket_id = bid / k;
	local_id = bid % k;

	TOID_ASSIGN(line, (D_RW(ppl->buckets)[bucket_id]).oid);
	pline = D_RW(line);
	TOID_ASSIGN (log_block, (D_RW(pline->arr)[local_id]).oid);
	plog_block = D_RW(log_block);

	return plog_block;
}


/*
 * Sub-rountine to handle write log
 * Called by pm_ppl_write(), the caller has already acquired the lock on the entry
 * Parameters are similar to pm_ppl_write() excep
 * pe (in/out): 
 * pe->count and pageref may change
 * */

void __handle_pm_ppl_write_by_entry(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			tid,
			byte*				log_src,
			//mtr_buf_t*			dyn_buf,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			LSN_arr,
			uint64_t*			size_arr,
			PMEM_TT_ENTRY*		pe,
			bool				is_new)
{

	ulint i, j;

	uint64_t key;	
	uint64_t LSN;	// log record LSN
	uint64_t rec_size;// log record size
	uint64_t cur_off;
	uint64_t eid;

	int64_t blodk_id;

	byte* end_ptr;

	uint64_t block_len;
	uint64_t sum_len;

	PMEM_PAGE_REF* pref;

	i = 0;
	cur_off = 0;

	end_ptr = log_src + size;

	while (cur_off < size){
		// retrieve info
		key = key_arr[i];
		LSN = LSN_arr[i];
		rec_size = size_arr[i];
		//check size
		mlog_id_t type;
		ulint space, page_no;
		
		byte* temp = mlog_parse_initial_log_record(
				log_src + cur_off, end_ptr, &type, &space, &page_no);

		uint16_t check_size = mach_read_from_2(temp);

		assert (check_size == rec_size);
		assert (type < MLOG_BIGGEST_TYPE);

		//find the corresponding pageref with this log rec
		for (j = 0; j < pe->n_dp_entries; j++) {
			pref = D_RW(D_RW(pe->dp_arr)[j]);
			if (pref->key == key){
				//pageref already exist
				break;
			}
		} //end for each pageref in entry

		if (j < pe->n_dp_entries){
			//pageref already exist, we don't increase count
			//Note: Because pm_ppl_flush() may reset a plogblock even though it's count > 0, check for out-of-date
			PMEM_PAGE_LOG_BLOCK* plog_block =
				__get_log_block_by_id(pop, ppl, pref->idx);
			if (!plog_block->is_free &&
					plog_block->key == key){
				pref->pageLSN = LSN;

				//update log block			
				__update_page_log_block_on_write(
						pop,
						ppl,
						log_src,
						cur_off,
						rec_size,
						key,
						LSN,
						pe->eid,
						pref->idx);
			}
			else{
				//pref is out-of-date, the plog_block is either reset now or the key is not match (another trx has occupied the reset plog_block)

				pref->idx = -1;

				uint64_t new_bid = __update_page_log_block_on_write(
						pop,
						ppl,
						log_src,
						cur_off,
						rec_size,
						key,
						LSN,
						pe->eid,
						-1);

				//update pref
				pref->key = key;
				pref->idx = new_bid;
				pref->pageLSN = LSN;
			}
		}
		else {
			// new pageref, increase the count
			//find the first free pageref from the beginning to reuse the reclaim index
			for (j = 0; j < pe->n_dp_entries; j++) {
				pref = D_RW(D_RW(pe->dp_arr)[j]);
				if (pref->idx == -1){
					//found
					break;
				}
			}

			if (j == pe->n_dp_entries){

				//pe is full
				if (pe->n_dp_entries == pe->max_dp_entries){
					uint64_t old_size = pe->max_dp_entries;
					//Realloc double size
					__realloc_TT_entry(pop, ppl, pe, pe->max_dp_entries * 2);

					mlog_id_t type;
					type = (mlog_id_t)((ulint)*(log_src + cur_off) & ~MLOG_SINGLE_REC_FLAG);

					printf("\n\nREALLOC a PMEM_TT_ENTRY from %zu to %zu (eid %zu tid %zu log type %zu) \n", old_size, pe->max_dp_entries, pe->eid, pe->tid, type);
					//assert(0);
				}

				pref = D_RW(D_RW(pe->dp_arr)[j]);
				pe->n_dp_entries++;
			}

			//update log block
			uint64_t new_bid = __update_page_log_block_on_write(
					pop,
					ppl,
					log_src,
					cur_off,
					rec_size,
					key,
					LSN,
					pe->eid,
					-1);

			pref->key = key;
			pref->idx = new_bid;
			pref->pageLSN = LSN;
		}

		cur_off += rec_size;
		i++;
	} //end while
	
	assert(i == n_recs);
}

void __handle_pm_ppl_write_by_entry_old(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			tid,
			//byte*				log_src,
			mtr_buf_t*			dyn_buf,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			LSN_arr,
			uint64_t*			size_arr,
			PMEM_TT_ENTRY*		pe,
			bool				is_new)
{

	ulint i, j;

	uint64_t key;	
	uint64_t LSN;	// log record LSN
	uint64_t rec_size;// log record size
	uint64_t cur_off;
	uint64_t eid;

	int64_t blodk_id;

	byte* log_src;

	PMEM_PAGE_REF* pref;
	// Now the pe point to the entry to write on and the ret is the entry id

	// for each input log record
	cur_off = 0;
	for (i = 0; i < n_recs; i++) {
		key = key_arr[i];
		LSN = LSN_arr[i];
		if (i == n_recs - 1){
			rec_size = size - size_arr[i];
		}
		else{
			rec_size = size_arr[i + 1] - size_arr[i];
		}
		
		//find the corresponding pageref with this log rec
		for (j = 0; j < pe->n_dp_entries; j++) {
			pref = D_RW(D_RW(pe->dp_arr)[j]);
			if (pref->key == key){
				//pageref already exist
				break;
			}
		} //end for each pageref in entry

		if (j < pe->n_dp_entries){
			//pageref already exist, we don't increase count
			//Note: Because pm_ppl_flush() may reset a plogblock even though it's count > 0, check for out-of-date
			PMEM_PAGE_LOG_BLOCK* plog_block =
				__get_log_block_by_id(pop, ppl, pref->idx);
			if (!plog_block->is_free &&
					plog_block->key == key){
				pref->pageLSN = LSN;

				//update log block			
				__update_page_log_block_on_write(
						pop,
						ppl,
						log_src,
						cur_off,
						rec_size,
						key,
						LSN,
						pe->eid,
						pref->idx);
			}
			else{
				//pref is out-of-date, the plog_block is either reset now or the key is not match (another trx has occupied the reset plog_block)

				pref->idx = -1;

				uint64_t new_bid = __update_page_log_block_on_write(
						pop,
						ppl,
						log_src,
						cur_off,
						rec_size,
						key,
						LSN,
						pe->eid,
						-1);

				//update pref
				pref->key = key;
				pref->idx = new_bid;
				pref->pageLSN = LSN;
			}
		}
		else {
			// new pageref, increase the count
			//find the first free pageref from the beginning to reuse the reclaim index
			for (j = 0; j < pe->n_dp_entries; j++) {
				pref = D_RW(D_RW(pe->dp_arr)[j]);
				if (pref->idx == -1){
					//found
					break;
				}
			}

			if (j == pe->n_dp_entries){

				//pe is full
				if (pe->n_dp_entries == pe->max_dp_entries){
					uint64_t old_size = pe->max_dp_entries;
					//Realloc double size
					__realloc_TT_entry(pop, ppl, pe, pe->max_dp_entries * 2);

					mlog_id_t type;
					type = (mlog_id_t)((ulint)*log_src & ~MLOG_SINGLE_REC_FLAG);

					printf("\n\nREALLOC a PMEM_TT_ENTRY from %zu to %zu (eid %zu tid %zu log type %zu) \n", old_size, pe->max_dp_entries, pe->eid, pe->tid, type);
					//assert(0);
				}

				pref = D_RW(D_RW(pe->dp_arr)[j]);
				pe->n_dp_entries++;
			}

			//update log block
			uint64_t new_bid = __update_page_log_block_on_write(
					pop,
					ppl,
					log_src,
					cur_off,
					rec_size,
					key,
					LSN,
					pe->eid,
					-1);
			
			pref->key = key;
			pref->idx = new_bid;
			pref->pageLSN = LSN;
		}

		cur_off += rec_size;
		//next log record
	}//end for each input log record
}

/*
 * Write a log record on per-page partitioned log
 * log_src (in): pointer to the beginning of array log records in mini-transaction's heap
 * cur_off (in): start offset of current log record
 * rec_size (in): size of current log record
 * key (in): fold of page_no and space 
 * LSN (in): log record's LSN
 * eid (in): entry id of the TT entry
 * bid (in):
 * -1: no ref
 *  Otherwise: reference to the log block to write 
 *
 *  Return: the bid of the block written
 * */
int64_t
__update_page_log_block_on_write(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			byte*				log_src,
			uint64_t			cur_off,
			uint64_t			rec_size,
			uint64_t			key,
			uint64_t			LSN,
			int64_t				eid,
			int64_t				block_id)
{

	int64_t ret;

	ulint hashed;
	ulint i, j;

	uint64_t n, k;
	uint64_t bucket_id, local_id;
	int64_t n_try;

	int64_t idx;

	byte* pdata = ppl->p_align;
	byte* log_des;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	PMEM_PAGE_LOG_BUF* plogbuf;
	
	////tdnguyen test, parset the log rec
	//mlog_id_t type;
	//ulint space;
	//ulint page_no;

	//mlog_parse_initial_log_record(log_src, log_src + rec_size, &type, &space, &page_no);	


	//if (type == MLOG_UNDO_INSERT){
	//	printf("==> PMEM_TEST Add REDO log rec of UNDO page (space %zu, page_no %zu)\n ", space, page_no);
	//}
	////end tdnguyen test
	
	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	if (block_id == -1) {
		//Case A: there is no help from fast access, the log block may exist or not. Search from the beginning
		PMEM_LOG_HASH_KEY(hashed, key, n);
		assert (hashed < n);

		TOID_ASSIGN(line, (D_RW(ppl->buckets)[hashed]).oid);
		pline = D_RW(line);
		assert(pline);
		assert(pline->n_blocks == k);

		//(1) search for the exist log block O(k)
		n_try = k;

		PMEM_LOG_HASH_KEY(i, key, k);

		while (n_try > 0){
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			plog_block = D_RW(D_RW(pline->arr)[i]);
			if (!plog_block->is_free &&
				plog_block->key == key){
				//Case A1: exist block
				ret = plog_block->bid;
				
				//(2) append log
				__pm_write_log_buf(
						pop,
						ppl,
						//pline,
						hashed,
						log_src,
						cur_off,
						rec_size,
						LSN,
						plog_block,
						false);

				//plog_block->n_log_recs++;

				//(3) update metadata
				plog_block->lastLSN = LSN;
				//inc number of active tx on this page			
				plog_block->count++;

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				return ret;
			}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);

			n_try--;
			i = (i + 1) % k;
			//next log block
		}//end search for exist log block

		//Case A2: If you reach here, then the log block is not existed, write the new one. During the scan, some log block may be reclaimed, search from the begin

		//search for a free block to write on O(k)
		n_try = k;
		PMEM_LOG_HASH_KEY(i, key, k);

		while( n_try > 0) {
		//for (i = 0; i < k; i++){}
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			plog_block = D_RW(D_RW(pline->arr)[i]);
			if (plog_block->is_free){
				//found, write data on the new block
				ret = plog_block->bid;

				plog_block->is_free = false;
				plog_block->key = key;

				//(2) append log
				__pm_write_log_buf(
						pop,
						ppl,
						//pline,
						hashed,
						log_src,
						cur_off,
						rec_size,
						LSN,
						plog_block,
						true);

				//plog_block->n_log_recs++;

				//(3) update metadata
				/*firstLSN is updated only here*/
				plog_block->lastLSN = LSN;

				plog_block->count = 1;

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);

				return ret;
			}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);

			n_try--;
			i = (i + 1) % k;
			//next
		}//end search for a free block to write on

		//If you reach here, then there is no free block
		printf("PMEM_ERROR in __update_page_log_block_on_write(), no free log block on hashed %zu\n", hashed);
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
		__print_page_blocks_state(debug_ptxl_file, ppl);
#endif
		assert(0);
		return ret;
	}//end CASE A
	else {
		//Case B: fast access mode, similar to case A1
		ret = block_id;

		bucket_id = block_id / k;
		local_id = block_id % k;
		TOID_ASSIGN(line, (D_RW(ppl->buckets)[bucket_id]).oid);
		pline = D_RW(line);
		assert(pline);
		TOID_ASSIGN (log_block, (D_RW(pline->arr)[local_id]).oid);
		plog_block = D_RW(log_block);
		assert(plog_block->key == key);
		assert(!plog_block->is_free);

		pmemobj_rwlock_wrlock(pop, &plog_block->lock);
		//(2) append log
		__pm_write_log_buf(
				pop,
				ppl,
				//pline,
				bucket_id,
				log_src,
				cur_off,
				rec_size,
				LSN,
				plog_block,
				false);
		//plog_block->n_log_recs++;

		//(3) update metadata
		plog_block->lastLSN = LSN;

		//because block_id != -1, the txref must exist, no need to update tx_idx_arr, n_tx_idx, and count
		pmemobj_rwlock_unlock(pop, &plog_block->lock);
		return ret;
	} //end case B
}
/*
 *Write a log rec to a log buf
 ppl (in):pointer to the part-log to get the free pool
 pline (in): pointer to the hashed line
 log_src (in): pointer to the log rec
 src_off (in): offset on the log_src of the log rec
 plog_block (in): pointer to the log block
 is_first_write (in): true if it is the first write on the log_block
 * */
static inline void
__pm_write_log_buf(
			PMEMobjpool*				pop,
			PMEM_PAGE_PART_LOG*			ppl,
			//PMEM_PAGE_LOG_HASHED_LINE*	pline,
			uint64_t					hashed_id,
			byte*						log_src,
			uint64_t					src_off,
			uint64_t					rec_size,
			uint64_t					rec_lsn,
			PMEM_PAGE_LOG_BLOCK*		plog_block,
			bool						is_first_write)
{

	byte* log_des;
	byte* pdata;

	PMEM_PAGE_LOG_HASHED_LINE*	pline;
	PMEM_PAGE_LOG_FREE_POOL* pfreepool;
	PMEM_PAGE_LOG_BUF* plogbuf;

#if defined(UNIV_PMEMOBJ_PPL_STAT)
	uint64_t start_time, end_time;
#endif	

#if defined(UNIV_PMEMOBJ_PPL_STAT)
	start_time = ut_time_us(NULL);
#endif	

retry:

	pline = D_RW(D_RW(ppl->buckets)[hashed_id]);	
	pmemobj_rwlock_wrlock(pop, &pline->lock);
	
#if defined(UNIV_PMEMOBJ_PPL_STAT)
	end_time = ut_time_us(NULL);
	pline->log_write_lock_wait_time += (end_time - start_time);
	pline->n_log_write++;
#endif
	//when a logbuf is full, we switch the pline->logbuf not the line, so it's not require to check full and goto as in PB-NVM (see pm_buf_write_with_flusher())
		
	plogbuf = D_RW(pline->logbuf);
	if (plogbuf->state == PMEM_LOG_BUF_IN_FLUSH){
		pmemobj_rwlock_unlock(pop, &pline->lock);
		goto retry;
	}	

	////////////////////////////////////////////
	// (1) Handle full log buf
	// /////////////////////////////////////////
	if (plogbuf->cur_off + rec_size > plogbuf->size) {

get_free_buf:
		// (1.1) Get a free log buf
		pfreepool = D_RW(ppl->free_pool);

#if defined(UNIV_PMEMOBJ_PPL_STAT)
	start_time = ut_time_us(NULL);
#endif	
		pmemobj_rwlock_wrlock(pop, &pfreepool->lock);

#if defined(UNIV_PMEMOBJ_PPL_STAT)
	end_time = ut_time_us(NULL);
	pline->log_flush_lock_wait_time += (end_time - start_time);
	pline->n_log_flush++;

#endif	
		TOID(PMEM_PAGE_LOG_BUF) free_buf = POBJ_LIST_FIRST (&pfreepool->head);
		if (pfreepool->cur_free_bufs == 0 || 
				TOID_IS_NULL(free_buf)){

			pmemobj_rwlock_unlock(pop, &pfreepool->lock);
			os_event_wait(ppl->free_log_pool_event);
			goto get_free_buf;
		}
		POBJ_LIST_REMOVE(pop, &pfreepool->head, free_buf, list_entries);
		pfreepool->cur_free_bufs--;
		
		os_event_reset(ppl->free_log_pool_event);
		pmemobj_rwlock_unlock(pop, &pfreepool->lock);
		
		// (1.2) switch the free log buf with the full log buf

		// save the flushing log buf for recovery
		TOID_ASSIGN(pline->flush_logbuf, pline->logbuf.oid);
		//asign new one
		TOID_ASSIGN(pline->logbuf, free_buf.oid);
		D_RW(free_buf)->hashed_id = pline->hashed_id; 
		
		//reserve 4 bytes for log buffer's header
		assert(D_RW(free_buf)->cur_off == PMEM_LOG_BUF_HEADER_SIZE);
		
		//save the diskaddr should be written
		plogbuf->diskaddr = pline->diskaddr;

		//move the diskaddr on the line ahead, the written size should be aligned with 512B for DIRECT_IO works
		//pline->diskaddr += end_off;
		pline->diskaddr += plogbuf->size;

		// (1.3)write log rec on new buf
		log_des = ppl->p_align + D_RW(free_buf)->pmemaddr + D_RW(free_buf)->cur_off;

		if (is_first_write){
			//Note that we save the offset and diskaddr for the first write of this block
			plog_block->start_off = D_RW(free_buf)->cur_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;
		}

		__pm_write_log_rec_low(pop,
				log_des,
				log_src + src_off,
				rec_size);

		D_RW(free_buf)->cur_off += rec_size;
		D_RW(free_buf)->state = PMEM_LOG_BUF_IN_USED;

		// (1.4) write acutal log buffer's size to the header
		byte* header = ppl->p_align + plogbuf->pmemaddr + 0;
		mach_write_to_4(header, plogbuf->cur_off);
		//fill zero the un-used len
		uint64_t dif_len = plogbuf->size - plogbuf->cur_off;
		if (dif_len > 0) {
			memset(header + plogbuf->cur_off, 0, dif_len);
		}

		// (1.5) assign a pointer in the flusher to the full log buf, this function return immediately 
		pm_log_buf_assign_flusher(ppl, plogbuf);

	} //end handle full log buf
	else {	//Regular case
		// (1) Write log rec to log buf
		if (is_first_write){
			//Note that we save the offset and diskaddr for the first write of this block
			plog_block->start_off = plogbuf->cur_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;
		}

		log_des = ppl->p_align + plogbuf->pmemaddr + plogbuf->cur_off;

		__pm_write_log_rec_low(pop,
				log_des,
				log_src + src_off,
				rec_size);
		if (plogbuf->cur_off == PMEM_LOG_BUF_HEADER_SIZE)		{
			//this is the first write on this logbuf
			plogbuf->state = PMEM_LOG_BUF_IN_USED;
		}
		plogbuf->cur_off += rec_size;
	}
	pmemobj_rwlock_unlock(pop, &pline->lock);

}

/*
 * Called by __pm_write_log_buf() when a logbuf is full
 * Assign the pointer in the flusher to the full logbuf
 * Trigger the worker thread if the number of full logbuf reaches a threshold
 * */
void
pm_log_buf_assign_flusher(
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_PAGE_LOG_BUF*	plogbuf) 
{
	
	PMEM_LOG_FLUSHER* flusher = ppl->flusher;

assign_worker:
	mutex_enter(&flusher->mutex);

	if (flusher->n_requested == flusher->size) {
		//all requested slot is full)
		printf("PMEM_INFO: all log_buf reqs are booked, sleep and wait \n");
		mutex_exit(&flusher->mutex);
		os_event_wait(flusher->is_log_req_full);	
		goto assign_worker;	
	}

	//find an idle thread to assign flushing task
	int64_t n_try = flusher->size;
	while (n_try > 0) {
		if (flusher->flush_list_arr[flusher->tail] == NULL) {
			//found
			flusher->flush_list_arr[flusher->tail] = plogbuf;
			plogbuf->state = PMEM_LOG_BUF_IN_FLUSH;

			++flusher->n_requested;
			//delay calling flush up to a threshold
			if (flusher->n_requested >= PMEM_LOG_FLUSHER_WAKE_THRESHOLD) {
				/*trigger the flusher 
				 * pm_log_flusher_worker() -> pm_log_batch_aio()
				 * */
				os_event_set(flusher->is_log_req_not_empty);
				//see pm_flusher_worker() --> pm_buf_flush_list()
			}

			if (flusher->n_requested >= flusher->size) {
				os_event_reset(flusher->is_log_req_full);
			}
			//for the next 
			flusher->tail = (flusher->tail + 1) % flusher->size;
			break;
		}
		//circled increase
		flusher->tail = (flusher->tail + 1) % flusher->size;
		n_try--;
	} //end while 
	//check
	if (n_try == 0) {
		/*This imply an logical error 
		 * */
		printf("PMEM_ERROR in pm_log_buf_assign_flusher() requested/size = %zu /%zu / %zu\n", flusher->n_requested, flusher->size);
		mutex_exit(&flusher->mutex);
		assert (n_try);
	}

	mutex_exit(&flusher->mutex);
}

/*
 * log flusher worker call back
 */
void 
pm_log_flush_log_buf(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_BUF*		plogbuf) 
{
	uint64_t i;

	uint64_t min_diskaddr;
	uint64_t min_off;

	PMEM_PAGE_LOG_FREE_POOL* pfree_pool;
	TOID(PMEM_PAGE_LOG_BUF) logbuf;

	assert(plogbuf);
	TOID_ASSIGN(logbuf, plogbuf->self);

	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_PAGE_LOG_BLOCK* plog_block;

	pline = D_RW(D_RW(ppl->buckets)[plogbuf->hashed_id]);

	/*(1) Pre-flush*/
	//this code just for test, simulate the checkpoint
	bool is_first = true;

	for (i = 0; i < ppl->n_blocks_per_bucket; i++){
		plog_block = D_RW(D_RW(pline->arr)[i]);

		if (!plog_block->is_free){
			if (is_first){
				is_first = false;
				min_off = plog_block->start_off;
				continue;
			}

			if (min_diskaddr > plog_block->start_diskaddr)
				min_diskaddr = plog_block->start_diskaddr;
			if (min_off > plog_block->start_off)
				min_off = plog_block->start_off;
		}
	}

	pline->recv_diskaddr = min_diskaddr;
	pline->recv_off = min_off;

#if defined (UNIV_WRITE_LOG_ON_NVM)
	/*(2) memcpy and reset */
	//skip write
	
	//handle finish
	pm_handle_finished_log_buf(
		pop, ppl, plogbuf);	
#else
	/*(2) Flush	*/
	plogbuf->state = PMEM_LOG_BUF_IN_FLUSH;
	pm_log_fil_io(pop, ppl, plogbuf);
#endif
}

/*
 * Call back function from fil_aio_wait() in fil0fil.cc
 * */
void
pm_handle_finished_log_buf(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		fil_node_t*				node,
		PMEM_PAGE_LOG_BUF*		plogbuf)
{
	TOID(PMEM_PAGE_LOG_BUF) logbuf;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	PMEM_PAGE_LOG_FREE_POOL* pfree_pool;

	assert(plogbuf);
	TOID_ASSIGN(logbuf, plogbuf->self);

	//printf("pm_handle_finished_log_buf id %zu hashed %zu\n", plogbuf->id, plogbuf->hashed_id);

	pline = D_RW(D_RW(ppl->buckets)[plogbuf->hashed_id]);
	assert(pline);

	/*(3) Post-flush */

	//flush file
	assert(node->is_open);
	os_file_flush(node->handle);

	/*advance the write_diskaddr
	 * write_diskaddr may smaller than diskaddr multiple logbuf's sizes, i.e. 
	 * write_diskaddr + k*plogbuf->size == diskaddr
	 * 0 <= k <= n
	 * */
	//pline->write_diskaddr = pline->diskaddr;
	pline->write_diskaddr += plogbuf->size;

	//reset the log buf
	plogbuf->state = PMEM_LOG_BUF_FREE;
	//plogbuf->cur_off = 0;
	plogbuf->cur_off = PMEM_LOG_BUF_HEADER_SIZE;
	plogbuf->hashed_id = -1;
	plogbuf->diskaddr = 123; //dummy offset 
	//reset the ref in line
	TOID_ASSIGN(pline->flush_logbuf, OID_NULL);

	//put back to the free pool
	pfree_pool = D_RW(ppl->free_pool);
	pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

	POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, logbuf, list_entries);
	pfree_pool->cur_free_bufs++;

	//wakeup who is waitting for free_pool available
	os_event_set(ppl->free_log_pool_event);

	pmemobj_rwlock_unlock(pop, &pfree_pool->lock);
}

/*
 * Called by pm_ppl_redo in recovery
 * Assign the pointer in the redoer to a need-REDO hashed line
 * Trigger the worker thread if the number of assigned pointers reach a threshold
 * */
void
pm_recv_assign_redoer(
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_PAGE_LOG_HASHED_LINE*	pline) 
{
	
	PMEM_LOG_REDOER* redoer = ppl->redoer;

assign_worker:
	mutex_enter(&redoer->mutex);

	if (redoer->n_requested == redoer->size) {
		//all requested slot is full)
		printf("PMEM_INFO: all redoers are booked, sleep and wait \n");
		mutex_exit(&redoer->mutex);
		os_event_wait(redoer->is_log_req_full);	
		goto assign_worker;	
	}

	//find an idle thread to assign task
	int64_t n_try = redoer->size;

	while (n_try > 0) {
		if (redoer->hashed_line_arr[redoer->tail] == NULL) {
			//found
			redoer->hashed_line_arr[redoer->tail] = pline;
			++redoer->n_requested;
			//delay calling REDO up to a threshold
			if (redoer->n_requested >= PMEM_LOG_REDOER_WAKE_THRESHOLD) {
				os_event_set(redoer->is_log_req_not_empty);
				//see pm_redoer_worker() --> pm_ppl_redo_line()
			}

			if (redoer->n_requested >= redoer->size) {
				os_event_reset(redoer->is_log_req_full);
			}
			//for the next 
			redoer->tail = (redoer->tail + 1) % redoer->size;
			break;
		}
		//circled increase
		redoer->tail = (redoer->tail + 1) % redoer->size;
		n_try--;
	} //end while 
	//check
	if (n_try == 0) {
		/*This imply an logical error 
		 * */
		printf("PMEM_ERROR in pm_log_buf_assign_redoer() requested/size = %zu /%zu / %zu\n", redoer->n_requested, redoer->size);

		mutex_exit(&redoer->mutex);
		assert (n_try);
	}

	mutex_exit(&redoer->mutex);
}

/*
 * Write log rec from transaction's heap to NVDIMM
 * */
static inline void
__pm_write_log_rec_low(
			PMEMobjpool*			pop,
			byte*					log_des,
			byte*					log_src,
			uint64_t				rec_size)
{
	if (rec_size <= CACHELINE_SIZE){
		//We need persistent copy, Do not need a transaction for atomicity
			pmemobj_memcpy_persist(
				pop, 
				log_des,
				log_src,
				rec_size);

	}
	else{
		TX_BEGIN(pop) {
			TX_MEMCPY(log_des, log_src, rec_size);
		}TX_ONABORT {
		}TX_END
	}
}


/*
 * High level function called when transaction commit
 * Unlike per-tx logging, when a transaction commit we remove the TT entry 
 * pop (in): 
 * ppl (in):
 * tid (in): transaction id
 * bid (in): block id, saved in the transaction
 * */
void
pm_ppl_commit(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t				tid,
		int64_t					eid) 
{
//return;

	ulint hashed;
	uint64_t n, k, i;
	uint64_t bucket_id, local_id;
	bool is_reclaim;

	PMEM_TT* ptt;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;

	TOID(PMEM_TT_ENTRY) entry;
	PMEM_TT_ENTRY* pe;
	PMEM_PAGE_REF* pref;

	assert(eid != -1);
	assert(tid);

	ptt = D_RW(ppl->tt);
	assert(ptt != NULL);

	n = ptt->n_buckets;
	k = ptt->n_entries_per_bucket;

	bucket_id = eid / k;
	local_id = eid % k;
	TOID_ASSIGN(line, (D_RW(ptt->buckets)[bucket_id]).oid);
	pline = D_RW(line);
	assert(pline);
	TOID_ASSIGN (entry, (D_RW(pline->arr)[local_id]).oid);
	pe = D_RW(entry);

	pmemobj_rwlock_wrlock(pop, &pe->lock);

	//(2) for each pageref in the entry	
	for (i = 0; i < pe->n_dp_entries; i++) {
		pref = D_RW(D_RW(pe->dp_arr)[i]);
		if (pref->idx >= 0){

			//Double-check for out-of-date log_block
			PMEM_PAGE_LOG_BLOCK* plog_block =
				__get_log_block_by_id(pop, ppl, pref->idx);
			if (plog_block->is_free || plog_block->key == 0){
				pref->idx = -1;
			}
			else {
				//update the corresponding log block and try to reclaim it
				__update_page_log_block_on_commit(
						pop, ppl, pref, pe->eid);
			}
		}

	} //end for each pageref in the entry

	//we reset the TT entry on commit 
	__reset_TT_entry(pop, ppl, pe);

	pmemobj_rwlock_unlock(pop, &pe->lock);
}


/*
 * Update the page log block on commit
 * Called by pm_ppl_commit()
 *
 * bid (in): the block id help to find the right log block
 * eid (in): the eid of the transaction, use this to invalid txref in the log block because the TT entry will be reclaim soon
 * */
bool
__update_page_log_block_on_commit(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG* ppl,
		PMEM_PAGE_REF*		pref,
		int64_t				eid)
{
	ulint hashed;
	uint32_t n, k, i;
	uint64_t bucket_id, local_id;
	
	assert(pref != NULL);
	assert(pref->idx >= 0);

	int64_t bid = pref->idx;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK* plog_block;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;
	
	//(1) Get the log block by block id
	
	assert(bid >= 0);

	bucket_id = bid / k;
	local_id = bid % k;

	TOID_ASSIGN(line, (D_RW(ppl->buckets)[bucket_id]).oid);
	pline = D_RW(line);
	assert(pline);
	TOID_ASSIGN (log_block, (D_RW(pline->arr)[local_id]).oid);
	plog_block = D_RW(log_block);
	assert(plog_block);
	

	if (plog_block->bid != bid){
		printf("error in __update_page_log_block_on_commit plog_block->bid %zu diff. with bid %zu \n ", plog_block->bid, bid);
		assert(0);
	}

	if (plog_block->is_free){
		// If we reclaim a block when flush page without checking the count variable (true in InnoDB), then this case may happen
		// this block is free due to flush page, do nothing
		return true;
	}	

	pmemobj_rwlock_wrlock(pop, &plog_block->lock);
	if (plog_block->count <= 0){
		printf("PMEM_ERROR in __update_page_log_block_on_commit(), count is already zero, cannot reduce more. This is logical error!!!\n");
		pmemobj_rwlock_unlock(pop, &plog_block->lock);
		assert(0);
		return false;
	}

	//(1) Invalid the txref
	//for (i = 0; i < plog_block->n_tx_idx; i++){
	//	if (D_RW(plog_block->tx_idx_arr)[i] == eid){
	//		D_RW(plog_block->tx_idx_arr)[i] = -1;
	//		break;
	//	}
	//}
	//if (i >= plog_block->n_tx_idx){
	//	//no txref
	//	printf("PMEM ERROR, no txref with eid %zu in log block %zu of bucket %zu, logical error!\n", eid, plog_block->bid, bucket_id);
	//	assert(0);
	//}

	//(2) Reclaim the log block
	plog_block->count--;
	if (plog_block->count <= 0 &&
		plog_block->lastLSN <= plog_block->pageLSN)
	{
	//	printf("====> reclaim page block %zu key %zu on commit\n", plog_block->bid, plog_block->key);

		__reset_page_log_block(plog_block);
		pmemobj_rwlock_unlock(pop, &plog_block->lock);
		return true;
	}
	else{
		pmemobj_rwlock_unlock(pop, &plog_block->lock);
		return false;
	}

	
}

/* reset (reclaim) a page log block
 * The caller must acquired the entry lock before reseting
 * */
void 
__reset_page_log_block(PMEM_PAGE_LOG_BLOCK* plog_block) 
{

	uint64_t i;
	
	plog_block->is_free = true;
	plog_block->key = 0;
	plog_block->count = 0;

	//plog_block->cur_size = 0;
	//plog_block->n_log_recs = 0;

	plog_block->pageLSN = 0;
	plog_block->firstLSN = 0;
	plog_block->lastLSN = 0;
	plog_block->start_off = 0;
	plog_block->start_diskaddr = 0;
}

/*
 * Called when the buffer pool flush page to PB-NVM
 *
 * key (in): the fold of space_id and page_no
 * pageLSN (in): pageLSN in the header of the flushed page
 * */
void 
pm_ppl_flush_page(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		uint64_t			space,
		uint64_t			page_no,
		uint64_t			key,
		uint64_t			pageLSN) 
{
	ulint hashed;
	uint32_t n, k, i, j;

	int64_t free_idx;
	int64_t n_try;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	PMEM_TT* ptt = D_RW(ppl->tt);

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	//(1) Start from the per-page log block
	
	PMEM_LOG_HASH_KEY(hashed, key, n);
	assert (hashed < n);

	TOID_ASSIGN(line, (D_RW(ppl->buckets)[hashed]).oid);
	pline = D_RW(line);
	assert(pline);
	assert(pline->n_blocks == k);
	
	//find the log block by hashing key O(k)
	n_try = k;
	PMEM_LOG_HASH_KEY(i, key, k);
	while (n_try > 0){
		//for (i = 0; i < k; i++){}
		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		plog_block = D_RW(D_RW(pline->arr)[i]);

		if (!plog_block->is_free &&
				plog_block->key == key){
			// Case A: found
			//(1) no need to check and reclaim the corresponding entries in TT

			//update the pageLSN on flush page
			plog_block->pageLSN = pageLSN;

			//we no longer assert here, new log recs are written on page during its flushing time
			//assert(plog_block->lastLSN <= pageLSN);
	
			/*Note 1: In InnoDB, changes of UNDO page has already captured in REDO log, we don't need to check the count variable to equal to reclaim. However, in other storage engine, count variable may needed
			 * Note 2: Checkpoint in PPL is naturally done by this reclaim. By reclaiming a block of flush page, the low_watermark is increased.
			 * */
			// (2) Check to reclaim this log block
			//printf("pm_ppl_flush_page (%zu, %zu) key %zu\n bid %zu count %zu", space, page_no, key, plog_block->bid, plog_block->count);
			//if (plog_block->count <= 0){
				if (plog_block->lastLSN <= pageLSN){
					__reset_page_log_block(plog_block);
				}
			//}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			return;

		}//end found the right log block

		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
		n_try--;
		i = (i + 1) % k;
		//next log block
	}//end find the log blcok by hashing key

	//if you reach here, then this flushed page doesn't have any transaction modified 
	//it may be the DBMS's metadata page
	
	//Now we skip this case
	return;
/*	
	//Case B: new log block on flush
	// We start searching from the beginning for a free block to write on
	for (i = 0; i < k; i++){
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			plog_block = D_RW(D_RW(pline->arr)[i]);

			if (plog_block->is_free){
				//found 
				plog_block->is_free = false;
				plog_block->key = key;
				plog_block->pageLSN = pageLSN;

				assert(plog_block->count == 0);
				assert(plog_block->n_tx_idx == 0);

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				return;
			}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			//next
	}
	// No free log block
	// TODO: reclaim log block by flushing committed log block to log files	
		pmemobj_rwlock_wrlock(pop, &pline->lock);
		printf("PMEM_ERROR in pm_ppl_flush_page(), no free log block on hashed %zu\n", hashed);
		__print_TT(debug_ptxl_file, ptt);
		__print_page_blocks_state(debug_ptxl_file, ppl);
		pmemobj_rwlock_unlock(pop, &pline->lock);
		assert(0);
		return;	
*/
}

//////////// RECOVERY ////////////////
//see pm_ppl_recovery() in storage/innobase/log/log0recv.cc

/////////// END RECOVERY ////////////

/*
 * Init a log group in DRAM
 * Borrow the code from InnoDB 
 * */
PMEM_LOG_GROUP*
pm_log_group_init(
/*===========*/
	uint64_t	id,			/*!< in: group id */
	uint64_t	n_files,		/*!< in: number of log files */
	uint64_t	file_size,		/*!< in: log file size in bytes */
	uint64_t	space_id)		/*!< in: space id of the file space
					which contains the log files of this
					group */
{
	uint64_t i;
	PMEM_LOG_GROUP*	group;
	
	group = static_cast<PMEM_LOG_GROUP*>(ut_malloc_nokey(sizeof(PMEM_LOG_GROUP)));

	group->id = id;
	group->n_files = n_files;
	group->format = LOG_HEADER_FORMAT_CURRENT;
	group->file_size = file_size;
	group->space_id = space_id;
	group->state = LOG_GROUP_OK;
	group->lsn = LOG_START_LSN;
	group->lsn_offset = LOG_FILE_HDR_SIZE;

	group->file_header_bufs_ptr = static_cast<byte**>(
		ut_zalloc_nokey(sizeof(byte*) * n_files));

	group->file_header_bufs = static_cast<byte**>(
		ut_zalloc_nokey(sizeof(byte**) * n_files));

	for (i = 0; i < n_files; i++) {
		group->file_header_bufs_ptr[i] = static_cast<byte*>(
			ut_zalloc_nokey(LOG_FILE_HDR_SIZE
					+ OS_FILE_LOG_BLOCK_SIZE));

		group->file_header_bufs[i] = static_cast<byte*>(
			ut_align(group->file_header_bufs_ptr[i],
				 OS_FILE_LOG_BLOCK_SIZE));
	}

	return group;
}

void 
pm_log_group_free(
		PMEM_LOG_GROUP* group)
{
	uint64_t n, i;

	n	= group->n_files;
	
	for (i = 0; i < n; i++){
		ut_free(group->file_header_bufs_ptr[i]);
		//ut_free(group->file_header_bufs[i]);

	}
	ut_free (group->file_header_bufs_ptr);
	ut_free (group->file_header_bufs);
	ut_free (group);
}

PMEM_PAGE_LOG_HASHED_LINE*
pm_ppl_get_line_from_key(
	PMEMobjpool*                pop,
	PMEM_PAGE_PART_LOG*         ppl,
	uint64_t					key)
{
	uint32_t n;
	ulint hashed;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	n = ppl->n_buckets;

	PMEM_LOG_HASH_KEY(hashed, key, n);
	assert (hashed < n);

	TOID_ASSIGN(line, (D_RW(ppl->buckets)[hashed]).oid);
	pline = D_RW(line);
	assert(pline);

	return pline;
}
//////////////////// STATISTIC FUNCTIONS//////

#if defined(UNIV_PMEMOBJ_PPL_STAT)
void
__print_lock_overhead(FILE* f,
		PMEM_PAGE_PART_LOG* ppl){
	
	uint32_t n, k, i, j;
	uint64_t max_log_write_lock_wait_time = 0;
	uint64_t max_log_flush_lock_wait_time = 0;

	double avg_log_write_lock_wait_time = 0;
	double avg_log_flush_lock_wait_time = 0;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

		if (pline->log_write_lock_wait_time > max_log_write_lock_wait_time){
			max_log_write_lock_wait_time = pline->log_write_lock_wait_time;
		}

	    avg_log_write_lock_wait_time += pline->log_write_lock_wait_time * 1.0 / pline->n_log_write;

		if (pline->log_flush_lock_wait_time > max_log_flush_lock_wait_time){
			max_log_flush_lock_wait_time = pline->log_flush_lock_wait_time;
		}
		
	    avg_log_flush_lock_wait_time += pline->log_flush_lock_wait_time * 1.0 / pline->n_log_flush;

	} //end for

	avg_log_write_lock_wait_time = avg_log_write_lock_wait_time / n;
	avg_log_flush_lock_wait_time = avg_log_flush_lock_wait_time / n;

	fprintf(f, "max_log_write_wait(us) avg_log_write_wait(us) max_log_flush_wait(us) avg_log_flush_wait \t %zu \t %f \t %zu \t %f\n", 
			max_log_write_lock_wait_time,
			avg_log_write_lock_wait_time,
			max_log_flush_lock_wait_time,
			avg_log_flush_lock_wait_time);
	printf("max_log_write_wait(us) avg_log_write_wait(us) max_log_flush_wait(us) avg_log_flush_wait \t %zu \t %f \t %zu \t %f\n", 
			max_log_write_lock_wait_time,
			avg_log_write_lock_wait_time,
			max_log_flush_lock_wait_time,
			avg_log_flush_lock_wait_time);
}
#endif
////////////END STATISTIC  FUNCTION

////////////////// END PERPAGE LOGGING ///////////////





//////////////////// STATISTIC FUNCTION ////////////////
#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
/*
 *Print the dirty page table
 Trace number of entries with zero counter but not free 
 * */
void
__print_DPT(
		PMEM_DPT*			pdpt,
		FILE* f)
{
	uint32_t n, k, i, j;
	uint64_t total_free_entries = 0;
	uint64_t total_idle_entries = 0;
	uint64_t line_max_txref = 0;
	uint64_t all_max_txref = 0;

	TOID(PMEM_DPT_HASHED_LINE) line;
	PMEM_DPT_HASHED_LINE* pline;

	TOID(PMEM_DPT_ENTRY) e;
	PMEM_DPT_ENTRY* pe;

	n = pdpt->n_buckets;
	k = pdpt->n_entries_per_bucket;

	fprintf(f, "\n============ Print Idle Entries in DPT ======= \n"); 
	//printf("\n============ Print Idle Entries in DPT ======= \n"); 
	for (i = 0; i < pdpt->n_buckets; i++) {
		pline = D_RW(D_RW(pdpt->buckets)[i]);
		fprintf(f, "DPT line %zu n_free %zu n_idle %zu load factor %zu\n", pline->hashed_id, pline->n_free, pline->n_idle, pline->n_entries);
		//printf("line %zu n_free %zu n_idle %zu load factor %zu\n", pline->hashed_id, pline->n_free, pline->n_idle, pline->n_entries);
		total_free_entries += pline->n_free;
		for (j = 0; j < pline->n_entries; j ++){
			pe =  D_RW(D_RW(pline->arr)[j]);
			assert(pe != NULL);
			if (line_max_txref < pe->max_txref_size)
				line_max_txref = pe->max_txref_size;
		}

		if (all_max_txref < line_max_txref){
			all_max_txref = line_max_txref;
		}

	}
	fprintf(f, "Total free entries in DPT:\t%zu\n", total_free_entries);
	fprintf(f, "max txref size in DPT:\t%zu\n", all_max_txref);
	//printf("Total free entries in DPT:\t%zu\n", total_free_entries);
	fprintf(f, "============ End Idle Entries in DPT ======= \n"); 
	//printf("============ End Idle Entries in DPT ======= \n"); 
}

/*print number of active transaction in the TT*/
void
__print_TT(FILE* f, 	PMEM_TT* ptt)
{
	uint32_t n, k, i, j;

	uint64_t n_free;
	uint64_t n_active;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;

	TOID(PMEM_TT_ENTRY) e;
	PMEM_TT_ENTRY* pe;

	n = ptt->n_buckets;
	k = ptt->n_entries_per_bucket;
	
	fprintf(f, "\n============ Print n_free, n_active in TT ======= \n"); 
	for (i = 0; i < n; i++){
		pline = D_RW(D_RW(ptt->buckets)[i]);

		n_free = n_active = 0;
		for (j = 0; j < k; j++){
			pe =  D_RW(D_RW(pline->arr)[j]);
			if (pe->state == PMEM_TX_FREE){
				n_free++;
			}
			else {
				assert(pe->state == PMEM_TX_ACTIVE);
				n_active++;
			}
		}

		fprintf(f, "TT line %zu n_free %zu n_active %zu load factor %zu\n", i, n_free, n_active, pline->n_entries);
	}
	fprintf(f, "\n============ end TT ======= \n"); 

}
/*
 * Consolidate info from trx-life info into whole time info
 * */
void
ptxl_consolidate_stat_info(PMEM_TX_LOG_BLOCK*	plog_block)
{
	uint64_t n = plog_block->all_n_reused;
	
	float					all_avg_small_log_recs;
	float					all_avg_log_rec_size;
	uint64_t				all_min_log_rec_size;
	uint64_t				all_max_log_rec_size;
	float					avg_block_lifetime;

	STAT_CAL_AVG(
			plog_block->all_avg_small_log_recs,
			n,
			plog_block->all_avg_small_log_recs,
			plog_block->n_small_log_recs);

	STAT_CAL_AVG(
			plog_block->all_avg_log_rec_size,
			n,
			plog_block->all_avg_log_rec_size,
			plog_block->avg_log_rec_size);

	STAT_CAL_AVG(
			plog_block->all_avg_block_lifetime,
			n,
			plog_block->all_avg_block_lifetime,
			plog_block->lifetime);

	if (plog_block->min_log_rec_size  < 
			plog_block->all_min_log_rec_size)
		plog_block->all_min_log_rec_size = 
			plog_block->min_log_rec_size;

	if (plog_block->max_log_rec_size  > 
			plog_block->all_max_log_rec_size)
		plog_block->all_max_log_rec_size = 
			plog_block->max_log_rec_size;
}
/*
 * print out free tx log block for debug
 * */
void 
__print_tx_blocks_state(FILE* f,
		PMEM_TX_PART_LOG* ptxl)
{

	uint64_t i, j, n, k;
	uint64_t bucket_id, local_id;

	uint64_t n_free;
	uint64_t n_active;
	uint64_t n_commit;
	uint64_t n_reclaimable;

	TOID(PMEM_TX_LOG_HASHED_LINE) line;
	PMEM_TX_LOG_HASHED_LINE* pline;

	TOID(PMEM_TX_LOG_BLOCK) log_block;
	PMEM_TX_LOG_BLOCK*	plog_block;

	n = ptxl->n_buckets;
	k = ptxl->n_blocks_per_bucket;

	fprintf(f, "======================================\n");
	printf("======================================\n");
	printf("Count state of each line for debugging\n");
	fprintf(f, "Count state of each line for debugging\n");
	//for each bucket
	for (i = 0; i < n; i++) {
		TOID_ASSIGN(line, (D_RW(ptxl->buckets)[i]).oid);
		pline = D_RW(line);
		assert(pline);
		//for each log block in the bucket
		n_free = n_active = n_commit = n_reclaimable = 0;
		for (j = 0; j < k; j++){
			TOID_ASSIGN (log_block, (D_RW(pline->arr)[j]).oid);
			plog_block = D_RW(log_block);
			if (plog_block->state == PMEM_FREE_LOG_BLOCK){
				n_free++;
			}
			else if (plog_block->state == PMEM_COMMIT_LOG_BLOCK) {
				if (plog_block->count <= 0){
					n_reclaimable++;
				}
				
				n_commit++;
			}
			else if (plog_block->state == PMEM_ACTIVE_LOG_BLOCK){
				n_active++;
			}
			else {
				printf("PMEM_ERROR: state %zu is not in defined list\n", plog_block->state);
				assert(0);
			}

		}//end for each log block
		printf("line %zu:  n_free %zu n_active %zu n_commit %zu n_reclaimable %zu \n", i, n_free, n_active, n_commit, n_reclaimable);
		fprintf(f, "line %zu:  n_free %zu n_active %zu n_commit %zu n_reclaimable %zu\n", i, n_free, n_active, n_commit, n_reclaimable);
	} //end for each bucket
	printf("======================================\n");
	fprintf(f, "======================================\n");
}

/*Print hahsed lines  for debugging*/
void 
__print_page_log_hashed_lines(
		FILE* f,
		PMEM_PAGE_PART_LOG* ppl)
{
	uint64_t i, j, n, k;
	uint64_t n_free;
	uint64_t n_flushed;
	uint64_t n_active;// number of log block that has at least one active tx on per bucket
	uint64_t max_log_size;

	uint64_t min_start_off;
	uint64_t min_start_diskaddr;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	fprintf(f, "==================Hashed lines info===============\n");
	printf("======================================\n");
	max_log_size = 0;

	for (i = 0; i < n; i++){
		pline = D_RW(D_RW(ppl->buckets)[i]);
		
		n_free = 0;
		n_flushed = 0;
		n_active = 0;
		min_start_off = ULONG_MAX;
		min_start_diskaddr = ULONG_MAX;

		//For each log block
		for (j = 0; j < k; j++) {
			plog_block = D_RW(D_RW(pline->arr)[j]);
			if (plog_block->is_free){
				n_free++;
			}
			else {
				if (min_start_off > plog_block->start_off)
					min_start_off = plog_block->start_off;
				if (min_start_diskaddr > plog_block->start_diskaddr)
					min_start_diskaddr = plog_block->start_diskaddr;
				
				if (plog_block->count > 0)
					n_active++;
				if (plog_block->pageLSN <= plog_block->lastLSN)
					n_flushed++;
			}
		} //end for each log block
		printf( "line %zu diskaddr %zu write_diskaddr %zu buf_cur_off %zu recv_diskaddr %zu recv_off %zu min_start_diskaddr %zu min_start_off %zu n_free %zu n_active %zu n_flushed %zu \n",
				i,
				pline->diskaddr,
				pline->write_diskaddr,
				D_RW(pline->logbuf)->cur_off,
				pline->recv_diskaddr,
				pline->recv_off,
				min_start_diskaddr,
				min_start_off,
				n_free,
				n_active,
				n_flushed
		  	  );	
		fprintf(f, "line %zu diskaddr %zu write_diskaddr %zu buf_cur_off %zu recv_diskaddr %zu recv_off %zu min_start_diskaddr %zu min_start_off %zu n_free %zu n_active %zu n_flushed %zu \n",
				i,
				pline->diskaddr,
				pline->write_diskaddr,
				D_RW(pline->logbuf)->cur_off,
				pline->recv_diskaddr,
				pline->recv_off,
				min_start_diskaddr,
				min_start_off,
				n_free,
				n_active,
				n_flushed
		  	  );	
		if (max_log_size < pline->diskaddr + D_RW(pline->logbuf)->cur_off)
			max_log_size = pline->diskaddr + D_RW(pline->logbuf)->cur_off;
	}
	printf("max log size %zu \n", max_log_size);
	fprintf(f, "max log size %zu \n", max_log_size);

	fprintf(f, "======================================\n");
	printf("======================================\n");
}
/*Print out page block states for debugging*/
void 
__print_page_blocks_state(
		FILE* f,
		PMEM_PAGE_PART_LOG* ppl)
{
	uint64_t i, j, n, k;
	uint64_t n_free;
	uint64_t n_flushed;
	uint64_t n_active;// number of log block that has at least one active tx on per bucket
	uint64_t actual_size;
	uint64_t max_size;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	fprintf(f, "======================================\n");
	printf("======================================\n");

	fprintf(f, "======================================\n");
	printf("======================================\n");
}

inline bool
__is_page_log_block_reclaimable(
		PMEM_PAGE_LOG_BLOCK* plog_block)
{
	return (plog_block->count ==0 &&
			plog_block->pageLSN >= plog_block->lastLSN);
}
/*
 * Print stat info for whole PPL
 * */
void 
ptxl_print_all_stat_info (FILE* f, PMEM_TX_PART_LOG* ptxl) 
{
	uint64_t i, j, n, k;
	uint64_t bucket_id, local_id;
	float					all_avg_small_log_recs = 0;
	float					all_avg_log_rec_size = 0;
	uint64_t				all_min_log_rec_size = ULONG_MAX;
	uint64_t				all_max_log_rec_size = 0;
	float					all_avg_block_lifetime = 0;

	uint64_t				all_max_log_buf_size = 0;
	uint64_t				all_max_n_pagerefs = 0;

	//Dirty page table
	uint64_t total_free_entries = 0;

	TOID(PMEM_TX_LOG_HASHED_LINE) line;
	PMEM_TX_LOG_HASHED_LINE* pline;

	TOID(PMEM_TX_LOG_BLOCK) log_block;
	PMEM_TX_LOG_BLOCK*	plog_block;

	n = ptxl->n_buckets;
	k = ptxl->n_blocks_per_bucket;

	fprintf(f, "\n============ BEGIN sum up info ======= \n"); 
	//scan through all log blocks
	//for each bucket
	for (i = 0; i < n; i++) {
		TOID_ASSIGN(line, (D_RW(ptxl->buckets)[i]).oid);
		pline = D_RW(line);
		assert(pline);
		//for each log block in the bucket
		for (j = 0; j < k; j++){
			TOID_ASSIGN (log_block, (D_RW(pline->arr)[j]).oid);
			plog_block = D_RW(log_block);
			
			// Calculate for total overall info
			STAT_CAL_AVG(
			all_avg_small_log_recs,
			i*n+j,
			all_avg_small_log_recs,
			plog_block->all_avg_small_log_recs);

			STAT_CAL_AVG(
			all_avg_log_rec_size,
			i*n+j,
			all_avg_log_rec_size,
			plog_block->all_avg_log_rec_size);

			if (plog_block->all_min_log_rec_size <
					all_min_log_rec_size)
				all_min_log_rec_size = plog_block->all_min_log_rec_size;
			if (plog_block->all_max_log_rec_size > 
					all_max_log_rec_size)
				all_max_log_rec_size = plog_block->all_max_log_rec_size;
			
			if (all_max_log_buf_size < plog_block->all_max_log_buf_size)
			all_max_log_buf_size = plog_block->all_max_log_buf_size;
			if (all_max_n_pagerefs < plog_block->all_max_n_pagerefs)
				all_max_n_pagerefs = plog_block->all_max_n_pagerefs;

			STAT_CAL_AVG(
			all_avg_block_lifetime,
			i*n+j,
			all_avg_block_lifetime,
			plog_block->all_avg_block_lifetime);
			
			// Print per-block info to file
			if (plog_block->all_n_reused > 0){
				ptxl_print_log_block_stat_info(f, plog_block);
			}

		} // end for each log block
	} // end for each bucket

	fprintf(f, "\n============ END sum up info ======= \n"); 

	//Dirty page table statistic info
	PMEM_DPT* pdpt = D_RW(ptxl->dpt);
	PMEM_DPT_HASHED_LINE* pdpt_line;
	
	__print_DPT(pdpt, f);

	printf("====== Overall ptxl statistic info ========\n");
	
	printf("Max log buf size:\t\t%zu\n", all_max_log_buf_size);
	printf("Max n pagerefs: \t\t%zu\n", all_max_n_pagerefs);
	printf("AVG no small log recs:\t\t%zu\n", all_avg_small_log_recs);
	printf("AVG log rec size (B):\t\t%zu\n", all_avg_log_rec_size);
	printf("min log rec size (B):\t\t%zu\n", all_min_log_rec_size);
	printf("max log rec size (B):\t\t%zu\n", all_max_log_rec_size);
	printf("AVG trx lifetime (ms):\t\t%zu\n", all_avg_block_lifetime);
	printf("====== End overall ptxl statistic info ========\n");
}
/*
 * Print stat info for a log block
 * */
void 
ptxl_print_log_block_stat_info (FILE* f, PMEM_TX_LOG_BLOCK* plog_block) 
{
	//<block-life info> <trx-life info>
	fprintf(f, "%zu %zu %.2f %.2f %zu %zu %.2f	%zu %zu %zu %zu %zu  %zu %zu %zu %zu \n", 
			plog_block->bid,
			plog_block->all_n_reused,
			plog_block->all_avg_small_log_recs,
			plog_block->all_avg_log_rec_size,
			plog_block->all_min_log_rec_size,
			plog_block->all_max_log_rec_size,
			plog_block->all_avg_block_lifetime,
			////// trx-lifetime
			plog_block->tid,
			plog_block->cur_off,
			//plog_block->n_log_recs,
			plog_block->count,
			plog_block->state,
			plog_block->n_small_log_recs,
			plog_block->avg_log_rec_size,
			plog_block->min_log_rec_size,
			plog_block->max_log_rec_size,
			plog_block->lifetime
			);
}
#endif //UNIV_PMEMOBJ_PART_PL_STAT


#endif //UNIV_PMEMOBJ_PL
