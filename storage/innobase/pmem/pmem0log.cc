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
//static FILE* debug_ptxl_file = fopen("pll_debug.txt","a");
static FILE* lock_overhead_file = fopen("ppl_lock_overhead.txt","a");
/*Part Log*/
static uint64_t PMEM_N_LOG_BUCKETS;
static uint64_t PMEM_N_BLOCKS_PER_BUCKET;

/*Log Buf*/
static uint64_t PMEM_LOG_BUF_SIZE;

static double PMEM_CKPT_THRESHOLD = 0.7;
static double PMEM_CKPT_MAX_OFFSET;

/*Transaction Table*/
static uint64_t PMEM_TT_N_LINES;
static uint64_t PMEM_TT_N_ENTRIES_PER_LINE;
static uint64_t PMEM_TT_MAX_DIRTY_PAGES_PER_TX;


/*Flush Log*/
static double PMEM_LOG_BUF_FLUSH_PCT;
static uint64_t PMEM_LOG_FLUSHER_WAKE_THRESHOLD=5;
static uint64_t PMEM_LOG_REDOER_WAKE_THRESHOLD=30;
static uint64_t PMEM_N_LOG_FLUSH_THREADS=32;

/*n spaces*/
static uint64_t PMEM_N_SPACES=256;

/*Log files*/
//static uint64_t PMEM_LOG_FILE_SIZE=4*1024; //in 4-KB pages (16MB)
static uint64_t PMEM_LOG_FILE_SIZE=16*1024; //in 4-KB pages (64MB)
//static uint64_t PMEM_N_LOG_FILES_PER_BUCKET=2;
static uint64_t PMEM_N_LOG_FILES_PER_BUCKET=1;

//temp buf size in TT PE
//static uint64_t PMEM_MINI_BUF_SIZE=2048;
static uint64_t PMEM_MINI_BUF_SIZE=8192;

static uint64_t PMEM_DUMMY_EID = pm_ppl_create_entry_id(PMEM_EID_NEW, 0, 0);

/*BIT ARRARY*/
static bool USE_BIT_ARRAY = true;
//static bool USE_BIT_ARRAY = false;
static uint64_t BIT_BLOCK_SIZE = sizeof(long long);

/*call pmemobj_persist() at every log record write*/
static bool PERSIST_AT_WRITE = true;

#endif //UNIV_PMEMOBJ_PL
//////////////// NEW PMEM PARTITION LOG /////////////

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
	
	/*we choose the number of buckets is the prime number that is slightly greater than the input*/
	PMEM_N_LOG_BUCKETS = ut_find_prime(srv_ppl_n_log_buckets);
	PMEM_N_BLOCKS_PER_BUCKET = ut_find_prime(srv_ppl_blocks_per_bucket);
	
	/*the log buf size should align with 512B*/
	PMEM_LOG_BUF_SIZE = srv_ppl_log_buf_size;

	PMEM_TT_N_LINES = ut_find_prime(srv_ppl_tt_n_lines);
	PMEM_TT_N_ENTRIES_PER_LINE = ut_find_prime(srv_ppl_tt_entries_per_line);
	PMEM_TT_MAX_DIRTY_PAGES_PER_TX = srv_ppl_tt_pages_per_tx;

/*Log files*/
	PMEM_LOG_FILE_SIZE = srv_ppl_log_file_size; //in 4-KB pages (64MB)
	PMEM_N_LOG_FILES_PER_BUCKET = srv_ppl_log_files_per_bucket;

/*Checkpoint*/
	PMEM_CKPT_THRESHOLD = srv_ppl_ckpt_threshold;
	PMEM_CKPT_MAX_OFFSET = (PMEM_LOG_FILE_SIZE * UNIV_PAGE_SIZE) * 1.0 * PMEM_CKPT_THRESHOLD;

/*Flush Log*/
	PMEM_LOG_BUF_FLUSH_PCT = srv_ppl_log_buf_flush_pct;
	PMEM_LOG_FLUSHER_WAKE_THRESHOLD = srv_ppl_log_flusher_wake_threshold;
	PMEM_N_LOG_FLUSH_THREADS = srv_ppl_n_log_flush_threads;
	


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
		//we create hash after finish REDO phase 1
	}
	/* Part 2: DRAM structures*/	

	// In any case (new alloc or reused) we need to allocate below objects
	
	/*init the per-line std::map */
	pm_ppl_init_in_mem(pmw->pop, pmw->ppl);

	/* defined in my_pmemobj.h, implement in buf0flu.cc */
	pmw->ppl->flusher = pm_log_flusher_init(PMEM_N_LOG_FLUSH_THREADS, FLUSHER_LOG_BUF);

	pmw->ppl->free_log_pool_event = os_event_create("pm_free_log_pool_event");
	pmw->ppl->redoing_done_event = os_event_create("pm_is_redoing_done_event");
	
	pm_page_part_log_hash_create(pmw->pop, pmw->ppl);

	pmw->ppl->deb_file = fopen("part_log_debug.txt","a");
}

void
pm_wrapper_page_log_close(
		PMEM_WRAPPER*	pmw	)
{
	PMEM_PAGE_PART_LOG* ppl = pmw->ppl;
	
	//Free resource allocated in DRAM
	pm_ppl_free_in_mem(pmw->pop, pmw->ppl);

	pm_log_flusher_close(ppl->flusher);
	os_event_destroy(ppl->free_log_pool_event);
	os_event_destroy(ppl->redoing_done_event);

	pm_page_part_log_hash_free(pmw->pop, pmw->ppl);

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
 * Reset data structures in PPL after recovery finished
 * */
void 
pm_ppl_reset_all(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl)
{

	//(1) Reset the log blocks
	pm_page_part_log_bucket_reset(pop, ppl);

	/*we don't use TT in this implementation*/
	//__reset_tt(pop, ppl);
}

/**/
void 
pm_page_part_log_bucket_reset(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl)
{
	uint64_t i, j, n, k;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_PAGE_LOG_BLOCK*	plog_block;
	int ret;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

        //(1) for each block in the line
        for (j = 0; j < pline->max_blocks; j++){
            plog_block = D_RW(D_RW(pline->arr)[j]);
			__reset_page_log_block(plog_block);
        } //end for each block in the line
		
		// (2) now realloc the pline->arr in need
		if (pline->max_blocks > k){
			ret = POBJ_REALLOC(pop,
					&pline->arr,
					TOID(PMEM_PAGE_LOG_BLOCK),
					sizeof(TOID(PMEM_PAGE_LOG_BLOCK)) * k);
			if (ret == -1){
				printf("PMEM_ERROR in __realloc_page_log_block_line\n");
				fprintf(stderr, "POBJ_REALLOC\n");
				assert(0);
			}
		}
		// (3) reset the pline
		pline->n_blocks = 0;
		pline->max_blocks = k;
		pline->diskaddr = 0;
		pline->write_diskaddr = 0;
		
		pline->ckpt_lsn = 0;
		pline->oldest_block_off = UINT32_MAX;
		pline->is_req_checkpoint = false;
		
		// (4) logbuf
		
		PMEM_PAGE_LOG_BUF* plogbuf = D_RW(pline->logbuf);
		plogbuf->pmemaddr = 0;
		plogbuf->state = PMEM_LOG_BUF_FREE;
		plogbuf->cur_off = PMEM_LOG_BUF_HEADER_SIZE;
		plogbuf->n_recs = 0;

		TOID_ASSIGN(plogbuf->next, OID_NULL);
		TOID_ASSIGN(plogbuf->prev, OID_NULL);
		TOID_ASSIGN(pline->tail_logbuf, (pline->logbuf).oid);

    } //end for each line

	pm_page_part_log_hash_free(pop, ppl);
	pm_page_part_log_hash_create(pop, ppl);
}

void
__reset_tt(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl)
{

	ulint i, j;
	ulint n, k;
	
	PMEM_TT* ptt;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;

	TOID(PMEM_TT_ENTRY) entry;
	PMEM_TT_ENTRY* pe;

	ptt = D_RW(ppl->tt);

	n = ptt->n_buckets;
	k = ptt->n_entries_per_bucket;

	//for each bucket
	for (i = 0; i < n; i++){
		pline = D_RW(D_RW(ptt->buckets)[i]);
		//for each entry in the bucket
		for (j = 0; j < k; j++){
			pe =  D_RW(D_RW(pline->arr)[j]);
			__reset_TT_entry(pop, ppl, pe);
		}
	}
}

/*
 * Allocate per-page logs and TT
 * @param[in] pop
 * @param[in] n_buckets - number of buckets in the PPL
 * @paramp[in] n_blocks_per_bucket
 * @param[in] n_log_bufs - total number of log buf
 * @param[in] log_buf_size - default log buffer size
 * */
PMEM_PAGE_PART_LOG* alloc_pmem_page_part_log(
		PMEMobjpool*	pop,
		uint64_t		n_buckets,
		uint64_t		n_blocks_per_bucket,
		uint64_t		n_log_bufs,
		uint64_t		log_buf_size) {

	char* p;
	size_t align_size;

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

	/*space array*/
	//POBJ_ALLOC(pop,
	//		&ppl->space_arr,
	//		TOID(PMEM_SPACE),
	//		sizeof(TOID(PMEM_SPACE)) * PMEM_N_SPACES,
	//		NULL,
	//		NULL);
	//ppl->n_spaces = 0;
	//for (i = 0; i < PMEM_N_SPACES; i++) {
	//	POBJ_ZNEW(pop,
	//			&D_RW(ppl->space_arr)[i],
	//			PMEM_SPACE);
	//	PMEM_SPACE* space = D_RW(D_RW(ppl->space_arr)[i]);
	//	space->space_no = UINT32_MAX;
	//}

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
	ppl->pmem_alloc_size += ppl->pmem_mini_free_pool_size;

	//(4) Free Pool
	__init_page_log_free_pool(
			pop,
			ppl,
			n_free_log_bufs,
			log_buf_size,
			log_buf_id,
			log_buf_offset);

	ppl->pmem_alloc_size += ppl->pmem_page_log_free_pool_size;
	
	/* (4) transaction table */
	/* We don't use TT in this implementation*/
	/*
	__init_tt(
			pop,
		   	ppl,
		   	PMEM_TT_N_LINES,
		   	PMEM_TT_N_ENTRIES_PER_LINE,
			PMEM_TT_MAX_DIRTY_PAGES_PER_TX);

	ppl->pmem_alloc_size += ppl->pmem_tt_size;
	*/
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

	uint64_t i;	
	PMEM_TT* ptt;
	PMEM_TT_HASHED_LINE *pline;

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

/*
 * Init entries in a TT line
 * */
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
		pline->n_entries = 0;
		pline->max_entries = n_entries;

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
			pe->n_mbuf_reqs = 0;
			pe->is_commit = false;
			//pe->eid = i * k + j;
			pe->eid = pm_ppl_create_entry_id(PMEM_EID_NEW, hashed_id, j);


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
				//D_RW(D_RW(pe->dp_arr)[i_temp])->idx = -1;
				D_RW(D_RW(pe->dp_arr)[i_temp])->idx = PMEM_DUMMY_EID;
				D_RW(D_RW(pe->dp_arr)[i_temp])->pageLSN = 0;
			}

			pe->n_dp_entries = 0;
			pe->max_dp_entries = pages_per_tx;
			ppl->pmem_tt_size += sizeof(PMEM_PAGE_REF) * pages_per_tx;

			// mini-buffer
			POBJ_ZNEW(pop, &pe->mbuf, PMEM_MINI_BUF);
			ppl->pmem_tt_size += sizeof(PMEM_MINI_BUF);
			
			PMEM_MINI_BUF* pmbuf = D_RW(pe->mbuf);

			POBJ_ALLOC(pop, &pmbuf->buf, char, sizeof(char)  * PMEM_MINI_BUF_SIZE, NULL, NULL); 
			pmbuf->cur_buf_size = 0;
			pmbuf->max_buf_size = PMEM_MINI_BUF_SIZE;
			pmbuf->is_new_write = true;
			pmbuf->entry_oid = (D_RW(pline->arr)[j]).oid;
			pmbuf->self = (pe->mbuf).oid;
			pmbuf->id = i * PMEM_TT_N_ENTRIES_PER_LINE  + j; //only assign id here
			pmbuf->start_time = 0;
		} //end for each entry
}

void __reset_TT_entry(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_TT_ENTRY* pe)
{
	uint64_t i;
	PMEM_PAGE_REF* pref;

	//PMEM_TT* ptt = D_RW(ppl->tt);

	pe->tid = 0;
	pe->n_mbuf_reqs = 0;
	pe->state = PMEM_TX_FREE;

	//TODO: what happend for pe that is realloc?
	// (pe->max_dp_entries > ptt->n_pref_per_entry)
	
	//for (i = 0; i < pe->n_dp_entries; i++) 
	for (i = 0; i < pe->max_dp_entries; i++) 
	{
		pref = D_RW(D_RW(pe->dp_arr)[i]);
		pref->key = 0;
		//pref->idx = -1;
		pref->idx = PMEM_DUMMY_EID;
		pref->pageLSN = 0;
	}

	pe->n_dp_entries = 0;
	pe->is_commit = false;

	//temp buffer
	PMEM_MINI_BUF* pmbuf = D_RW(pe->mbuf);
	
//	if (pmbuf->max_buf_size > PMEM_MINI_BUF_SIZE){
//		POBJ_REALLOC(pop, &pmbuf->buf, char, sizeof(char)  * PMEM_MINI_BUF_SIZE); 
//		pmbuf->max_buf_size = PMEM_MINI_BUF_SIZE;
//	}

	pmbuf->cur_buf_size = 0;
	pmbuf->is_new_write = true;
	pmbuf->entry_oid = OID_NULL;
	pmbuf->start_time = 0;
}

void __realloc_page_log_block_line(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE*	pline,
		uint64_t				new_size)
{

	uint64_t i;
	
	int ret;

	PMEM_PAGE_LOG_BLOCK*	plog_block;
	
	printf("PMEM_INFO __realloc_page_log_block_line() line  %d from %zu blocks to %zu blocks\n ", pline->hashed_id, pline->max_blocks, new_size);

	ret = POBJ_REALLOC(pop,
			&pline->arr,
			TOID(PMEM_PAGE_LOG_BLOCK),
			sizeof(TOID(PMEM_PAGE_LOG_BLOCK)) * new_size);
	if (ret == -1){
		printf("PMEM_ERROR in __realloc_page_log_block_line\n");
		fprintf(stderr, "POBJ_REALLOC\n");
		assert(0);
	}

	if (pline->max_blocks < new_size){
		ppl->pmem_page_log_size += sizeof(TOID(PMEM_PAGE_LOG_BLOCK)) * (new_size - pline->max_blocks);
		//for each log block
		for (i = pline->max_blocks; i < new_size; i++) {
			POBJ_ZNEW(pop,
					&D_RW(pline->arr)[i],
					PMEM_PAGE_LOG_BLOCK);
			if (TOID_IS_NULL(D_RW(pline->arr)[i])) {
				fprintf(stderr, "POBJ_ZNEW\n");
			}
			ppl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_BLOCK);
			plog_block = D_RW(D_RW(pline->arr)[i]);

			plog_block->is_free = true;
			plog_block->state = PMEM_FREE_BLOCK;
			//plog_block->bid = i * k + j;
			//plog_block->bid = PMEM_DUMMY_EID;
			plog_block->bid = 
				pm_ppl_create_entry_id(PMEM_EID_NEW, pline->hashed_id, i) ;
			plog_block->id = i;

			plog_block->key = 0;
			plog_block->count = 0;

			//plog_block->cur_size = 0;
			//plog_block->n_log_recs = 0;

			plog_block->pageLSN = 0;
			plog_block->lastLSN = 0;
			plog_block->start_off = 0;
			plog_block->start_diskaddr = 0;
		}//end for each log block

		pline->max_blocks = new_size;
	}
}
/*
 *Reallocate (extend) a TT line
 * */
void __realloc_TT_line(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_TT_HASHED_LINE*	pline,
		uint64_t				new_size)
{
	uint64_t i;
	uint64_t i_temp;
	uint64_t pages_per_tx;

	int ret;

	PMEM_TT_ENTRY* pe;
	PMEM_TT* ptt;
		
	ptt = D_RW(ppl->tt);
	pages_per_tx = ptt->n_pref_per_entry;
	//we only reallocate up
	assert (pline->max_entries < new_size);

	printf("PMEM_INFO __realloc_TT_line() line  %zu from %zu entries to %zu entries\n ", pline->hashed_id, pline->max_entries, new_size);

	ret = POBJ_REALLOC(pop,
			&pline->arr,
			TOID(PMEM_TT_ENTRY),
			sizeof(TOID(PMEM_TT_ENTRY)) * new_size);
	
	if (ret == -1){
		printf("PMEM_ERROR in __realloc_TT_line() line %zu\n", pline->hashed_id);
		fprintf(stderr, "POBJ_REALLOC\n");
		assert(0);
	}

	if (pline->max_entries < new_size){
		//init for new allocated TT entry	
		for (i = pline->max_entries; i < new_size; i++){
			POBJ_ZNEW(pop,
					&D_RW(pline->arr)[i],
					PMEM_TT_ENTRY);
			ppl->pmem_tt_size += sizeof(PMEM_TT_ENTRY);

			if (TOID_IS_NULL(D_RW(pline->arr)[i])) {
				fprintf(stderr, "POBJ_ZNEW\n");
			}

			pe = D_RW(D_RW(pline->arr)[i]);
			assert(pe != NULL);

			pe->state = PMEM_TX_FREE;
			pe->tid = 0;
			//pe->eid = i;
			pe->eid = pm_ppl_create_entry_id(PMEM_EID_NEW, pline->hashed_id, i);

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
				//D_RW(D_RW(pe->dp_arr)[i_temp])->idx = -1;
				D_RW(D_RW(pe->dp_arr)[i_temp])->idx = PMEM_DUMMY_EID;
				D_RW(D_RW(pe->dp_arr)[i_temp])->pageLSN = 0;
			}

			pe->n_dp_entries = 0;
			pe->max_dp_entries = pages_per_tx;
			ppl->pmem_tt_size += sizeof(PMEM_PAGE_REF) * pages_per_tx;

		} //end for

		pline->max_entries = new_size;
	}
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
			//D_RW(D_RW(pe->dp_arr)[i])->idx = -1;
			D_RW(D_RW(pe->dp_arr)[i])->idx = PMEM_DUMMY_EID;
			D_RW(D_RW(pe->dp_arr)[i])->pageLSN = 0;
		}

		ppl->pmem_tt_size += sizeof(PMEM_PAGE_REF) * (new_size - pe->max_dp_entries);
		pe->max_dp_entries = new_size;


	}
}

/*
 * Init in-mem data structures and variables 
 * Used for either new PPL or reused
 * Called from pm_wrapper_page_log_alloc_or_open()
 * */
void 
pm_ppl_init_in_mem(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl)
{
	uint64_t n, i;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	char sbuf[256];

	ppl->ckpt_lsn = 0;	
	ppl->max_oldest_lsn = 0;
	ppl->min_oldest_lsn = ULONG_MAX;

	n = ppl->n_buckets;	
	/*per-line in-mem data structures*/
	for (i = 0; i < n; i++){
		pline = D_RW(D_RW(ppl->buckets)[i]);
		
		pline->is_flushing = false;		
		/*Note that each pline must have distinct os event*/
		sprintf(sbuf,"pm_line_log_flush_event%zu", i);
		pline->log_flush_event = os_event_create(sbuf);
		/*the map*/
		pline->offset_map = new std::map<uint64_t, uint32_t>();
	}
}

/*Free allocated in-mem data structures*/
void 
pm_ppl_free_in_mem(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl)
{
	uint64_t n, i;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	n = ppl->n_buckets;	

	/*per-line in-mem data structures*/
	for (i = 0; i < n; i++){
		pline = D_RW(D_RW(ppl->buckets)[i]);
		
		/*os events*/
		os_event_destroy(pline->log_flush_event);

		/*the map*/
		free(pline->offset_map);
	}
}

void 
pm_page_part_log_bucket_init(
		PMEMobjpool*			pop,
		PMEM_PAGE_PART_LOG*		ppl,
		uint64_t				n_buckets,
		uint64_t				n_blocks_per_bucket,
		uint64_t				log_buf_size,
		uint64_t				&log_buf_id,
		uint64_t				&log_buf_offset) 
{

	uint64_t i, j, n, k;

	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_PAGE_LOG_BLOCK*	plog_block;
	
	//we need extra one bucket for all pages of space 0
	//n = n_buckets;
	n = n_buckets;
	k = n_blocks_per_bucket;

	//allocate the pointer array buckets
	POBJ_ALLOC(pop,
			&ppl->buckets,
			TOID(PMEM_PAGE_LOG_HASHED_LINE),
			sizeof(TOID(PMEM_PAGE_LOG_HASHED_LINE)) * n,
			NULL,
			NULL);
	if (TOID_IS_NULL(ppl->buckets)) {
		fprintf(stderr, "POBJ_ALLOC\n");
	}

	ppl->pmem_page_log_size = sizeof(TOID(PMEM_PAGE_LOG_HASHED_LINE)) * n;

	/*for each hashed line*/
	for (i = 0; i < n; i++) {
		POBJ_ZNEW(pop,
				&D_RW(ppl->buckets)[i],
				PMEM_PAGE_LOG_HASHED_LINE);
		if (TOID_IS_NULL(D_RW(ppl->buckets)[i])) {
			fprintf(stderr, "POBJ_ZNEW\n");
		}

		ppl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_HASHED_LINE);
		pline = D_RW(D_RW(ppl->buckets)[i]);

		pline->hashed_id = i;
		pline->n_blocks = 0;
		pline->max_blocks = k;
		pline->diskaddr = 0;
		pline->write_diskaddr = 0;
		
		pline->ckpt_lsn = 0;
		pline->oldest_block_off = UINT32_MAX;
		pline->is_req_checkpoint = false;

#if defined(UNIV_PMEMOBJ_PPL_STAT)
		pline->log_write_lock_wait_time = 0;
		pline->n_log_write = 0;
		pline->log_flush_lock_wait_time = 0;
		pline->n_log_flush = 0;
#endif	

		/*Log buf, after the alloca call, log_buf_id and log_buf_offset increase */
		POBJ_ZNEW(pop, &pline->logbuf, PMEM_PAGE_LOG_BUF);
		ppl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_BUF);

		PMEM_PAGE_LOG_BUF* plogbuf = D_RW(pline->logbuf);

		plogbuf->pmemaddr = log_buf_offset;
		log_buf_offset += log_buf_size;

		plogbuf->id = log_buf_id;

		/*save hashed_id */
		plogbuf->hashed_id = i;
		log_buf_id++;

		plogbuf->state = PMEM_LOG_BUF_FREE;
		plogbuf->size = log_buf_size;
		//plogbuf->cur_off = 0;
		plogbuf->cur_off = PMEM_LOG_BUF_HEADER_SIZE;
		plogbuf->n_recs = 0;
		plogbuf->self = (pline->logbuf).oid;
		plogbuf->check = PMEM_AIO_CHECK;
		
		/*assign pointers for logbufs*/
		TOID_ASSIGN(plogbuf->next, OID_NULL);
		TOID_ASSIGN(plogbuf->prev, OID_NULL);
		TOID_ASSIGN(pline->tail_logbuf, (pline->logbuf).oid);

		/*Allocate the log blocks */
		POBJ_ALLOC(pop,
				&pline->arr,
				TOID(PMEM_PAGE_LOG_BLOCK),
				sizeof(TOID(PMEM_PAGE_LOG_BLOCK)) * k,
				NULL,
				NULL);
		ppl->pmem_page_log_size += sizeof(TOID(PMEM_PAGE_LOG_BLOCK)) * k;
		/* for each log block */
		for (j = 0; j < k; j++) {
			POBJ_ZNEW(pop,
					&D_RW(pline->arr)[j],
					PMEM_PAGE_LOG_BLOCK);
			if (TOID_IS_NULL(D_RW(pline->arr)[j])) {
				fprintf(stderr, "POBJ_ZNEW\n");
			}
			ppl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_BLOCK);
			plog_block = D_RW(D_RW(pline->arr)[j]);
			
			plog_block->state = PMEM_FREE_BLOCK;
			plog_block->is_free = true;
			plog_block->bid = 
				pm_ppl_create_entry_id(PMEM_EID_NEW, i, j) ;
			plog_block->id = j;
			plog_block->key = 0;
			plog_block->count = 0;

			//plog_block->cur_size = 0;
			//plog_block->n_log_recs = 0;

			plog_block->pageLSN = 0;
			plog_block->lastLSN = 0;
			plog_block->start_off = 0;
			plog_block->start_diskaddr = 0;
			
			plog_block->first_rec_found = false;
			plog_block->first_rec_size = 0;
			plog_block->first_rec_type = (mlog_id_t) 0;

		}//end for each log block

		/*bit array*/
		pline->n_bit_blocks = (k - 1) / sizeof(long long) + 1;
		pline->bit_arr = (long long*) calloc(pline->n_bit_blocks, sizeof(long long));

	}//end for each hashed line
}
////////////// BIT ARRAY //////////////////////

void
pm_bit_set(
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		long long*	arr,
		size_t		block_size,
		uint64_t	bit_i)
{
	int block_i = bit_i / block_size;
	int pos = bit_i % block_size;
	unsigned int flag = 1;

	flag = flag << pos;
	arr[block_i] |= flag;

	//printf("|||||=> pm_bit_SET pline %zu index %zu block_bit %zu pos %zu \n", pline->hashed_id, bit_i, block_i, pos);
}

void
pm_bit_clear(
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		long long*	arr,
		size_t		block_size,
		uint64_t	bit_i)
{
	int block_i = bit_i / block_size;
	int pos = bit_i % block_size;
	unsigned int flag = 1;

	flag = ~(flag << pos);
	arr[block_i] &= flag;

	//printf("|||||=> pm_bit_clear pline %zu index %zu block_bit %zu pos %zu \n", pline->hashed_id, bit_i, block_i, pos);
}
/*
 * Search the first free bit in the bit array (from the right)
 * @param[in] bit_arr
 * @param[in] n_bit_blocks
 * @param[in] block_size size of a block in bits
 * */
int32_t 
pm_search_first_free_slot(
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		long long*	bit_arr,
		uint16_t	n_bit_blocks,
		uint16_t	block_size)
{
	uint32_t ret;	
	uint16_t block_i;
	long long val;
	long long not_val;
	int pos;
	
	ret = -1;	
	/*search from the right most of the array*/
	//for (block_i = n_bit_blocks - 1; block_i >= 0; block_i--) 
	for (block_i = 0; block_i < n_bit_blocks ; block_i++) 
	{
		val = bit_arr[block_i];
		if (__builtin_popcount(val) == block_size){
			/*all bits in this block are 1, move next*/
			continue;
		}

		/* use built-in funciton of GCC, return 1 + index (based 0) of the first set bit of a long long from the right */
		not_val = ~val;
		pos = __builtin_ffsll(not_val);
		//ret	= block_i * block_size + (block_size - pos);
		ret	= block_i * block_size + (pos - 1);
		//printf("||~~~||=> pm_bit_search pline %zu ret_i %zu block_i %zu pos %zu\n", pline->hashed_id, ret, block_i, pos);
		return ret;
	}

	/*not found*/
	return ret;

}

///////////////////////////////////////////////
//////////// InnoDB SPACE HANDLE //////////////////

void
pm_ppl_add_space(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		char* name,
		uint32_t space_no
		)
{
	uint16_t i;
	PMEM_SPACE* pm_space;
	
	pmemobj_rwlock_wrlock(pop, &ppl->ckpt_lock);

	for (i = 0; i < ppl->n_spaces; i++){
		pm_space = D_RW(D_RW(ppl->space_arr)[i]);

		if (pm_space->space_no == space_no ||
				strstr(pm_space->name, name)){
			/*exist space*/
			pmemobj_rwlock_unlock(pop, &ppl->ckpt_lock);
			return;
		}
	}
	/*Add new*/
	if (ppl->n_spaces == PMEM_N_SPACES){
		pmemobj_rwlock_unlock(pop, &ppl->ckpt_lock);
		printf("PMEM_ERROR: we reach the maximum number of spaces %zu!!! \n", PMEM_N_SPACES);
		assert(0);
	}

	pm_space = D_RW(D_RW(ppl->space_arr)[i]);

	pm_space->space_no = space_no;
	strcpy(pm_space->name, name);

	ppl->n_spaces++;
	
	printf("===> pm_ppl_add_space() name %s id %u\n", name, space_no);
	pmemobj_rwlock_unlock(pop, &ppl->ckpt_lock);
}
/////////////// HASH TABLE ///////////////////
/* Init hash table for each line in PPL
 *Must called after pm_page_part_log_bucket_init
 * */
void
pm_page_part_log_hash_create(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl)
{
	uint64_t i, n, k;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		pline->addr_hash = hash_create(k);
	}
}

void
pm_page_part_log_hash_free(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl)
{
	uint64_t i;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	for (i = 0; i < ppl->n_buckets; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		hash_table_free(pline->addr_hash);
	}
}


/*
 * Add key to hashtable if it is not in
 * The caller reponse for holding the pline->lock
 * @param[in] pop
 * @param[in] ppl
 * @param[in] pline
 * @param[in] key
 * @return pointer to the item in hashtable
 * */
plog_hash_t*
pm_ppl_hash_check_and_add(
		PMEMobjpool*				pop,
		PMEM_PAGE_PART_LOG*			ppl,
		PMEM_PAGE_LOG_HASHED_LINE*	pline,
		uint64_t					key	)
{
	uint64_t i;
	int64_t n_try;
	int64_t i_bit;

	PMEM_PAGE_LOG_BLOCK*	plog_block;
	plog_hash_t* item;

	item = pm_ppl_hash_get(pop, ppl, pline, key);
	i_bit = -2; //init value 

	if (item == NULL){
		/*key is not in hashtable.
		 * select a free plogbuf in page-partition for this key*/
		item = (plog_hash_t*) malloc(sizeof(plog_hash_t));
		item->key = key;
retry:
		if (USE_BIT_ARRAY) {
			/* (1) Method 1: search the free entry by hash the key then sequential search */
			PMEM_LOG_HASH_KEY(i, key, pline->max_blocks);
			plog_block = D_RW(D_RW(pline->arr)[i]);
			
			if (plog_block->is_free){
				/*we use the index suggested by hash value*/
			}
			else {
				/*ask the bit_arr to get the first free plogblock*/
				i_bit = pm_search_first_free_slot(pline, pline->bit_arr, pline->n_bit_blocks, BIT_BLOCK_SIZE);

				if (i_bit >= 0){
					i = i_bit;
				}
				else {
					/*all log_block is not free, reallocate the array*/
					i = 2 * pline->max_blocks; //invalid index
				}
			}

			if (plog_block->is_free || 
					(i_bit >= 0 && i < pline->max_blocks)){
				//pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				//pmemobj_rwlock_wrlock(pop, &pline->meta_lock);
				plog_block = D_RW(D_RW(pline->arr)[i]);
				assert(plog_block->is_free);
				
				pm_bit_set(pline, pline->bit_arr, BIT_BLOCK_SIZE, i);

				plog_block->is_free = false;
				plog_block->state = PMEM_IN_USED_BLOCK;
				plog_block->key = key;
				item->block_off = i;
				//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				//(2) Insert
				HASH_INSERT(plog_hash_t, addr_hash, pline->addr_hash, key, item);

				//pmemobj_rwlock_unlock(pop, &pline->meta_lock);
				return item;
			}

		} else {
			/* (1) Method 2: search the free entry by hash the key then sequential search */
			n_try = pline->max_blocks;
			PMEM_LOG_HASH_KEY(i, key, pline->max_blocks);
			while (n_try > 0){
				pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				plog_block = D_RW(D_RW(pline->arr)[i]);
				if (plog_block->is_free){
					//found
					plog_block->is_free = false;
					plog_block->state = PMEM_IN_USED_BLOCK;
					plog_block->key = key;
					item->block_off = i;
					pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
					//(2) Insert
					HASH_INSERT(plog_hash_t, addr_hash, pline->addr_hash, key, item);
					return item;
				}

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				n_try--;
				i = (i + 1) % pline->max_blocks;
			}
		} //end method 2

		//If you reach here, then there is no free block, extend
		__realloc_page_log_block_line(pop, ppl, pline, pline->max_blocks * 2);

		goto retry;
		
	}
	//item has already existed, do nothing
	return item;
}

plog_hash_t*
pm_ppl_hash_get(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		uint64_t			key	)
{
	plog_hash_t* item;
	ulint hashed;

	hashed = hash_calc_hash(key, pline->addr_hash);

	for (item = static_cast<plog_hash_t*> (
				HASH_GET_FIRST(pline->addr_hash, hashed));
		item != 0;
		item = static_cast<plog_hash_t*> (
				HASH_GET_NEXT(addr_hash, item))) {
		if (item->key == key){
			return item;
		}
	}

	return (NULL);
}

void
pm_ppl_hash_remove(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline,
		uint64_t			key
		) 
{
	plog_hash_t* item;

	item = pm_ppl_hash_get(pop, ppl, pline, key);

	if (item != NULL){
		HASH_DELETE(plog_hash_t, addr_hash, pline->addr_hash, key, item);
	}
}
///////////////END HASH TABLE ///////////////////


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
		plogbuf->n_recs = 0;
		plogbuf->self = logbuf.oid;
		plogbuf->check = PMEM_AIO_CHECK;
		TOID_ASSIGN(plogbuf->next, OID_NULL);
		TOID_ASSIGN(plogbuf->prev, OID_NULL);


		//insert to the free pool list
		POBJ_LIST_INSERT_HEAD(pop, &pfreepool->head, logbuf, list_entries); 
		pfreepool->cur_free_bufs++;

	}

	pmemobj_persist(pop, &ppl->free_pool, sizeof(ppl->free_pool));
}

PMEM_TT_ENTRY* 
pm_ppl_get_tt_entry_by_tid(
		PMEMobjpool*				pop,
		PMEM_PAGE_PART_LOG*			ppl,
		uint64_t tid)
{

	ulint i, j;
	ulint n, k;
	
	PMEM_TT* ptt;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;

	TOID(PMEM_TT_ENTRY) entry;
	PMEM_TT_ENTRY* pe;

	ptt = D_RW(ppl->tt);

	n = ptt->n_buckets;
	k = ptt->n_entries_per_bucket;
	//for each bucket
	for (i = 0; i < n; i++){
		pline = D_RW(D_RW(ptt->buckets)[i]);
		//for each entry in the bucket
		for (j = 0; j < k; j++){
			pe =  D_RW(D_RW(pline->arr)[j]);
			if (pe->state == PMEM_TX_ACTIVE
				&& pe->tid == tid){
				return pe;
			}
		}
	}

	return NULL;
}

/*
 * Find the maximum tid in TT
 * Used in recovery
 * */
void
pm_ppl_get_min_max_tid(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	ulint* min_tid,
	ulint* max_tid){

	ulint i, j;
	ulint n, k;
	
	PMEM_TT* ptt;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;

	TOID(PMEM_TT_ENTRY) entry;
	PMEM_TT_ENTRY* pe;

	ptt = D_RW(ppl->tt);

	n = ptt->n_buckets;
	k = ptt->n_entries_per_bucket;
	
	*min_tid = ULONG_MAX;
	*max_tid = 0;
	
	//for each bucket
	for (i = 0; i < n; i++){
		pline = D_RW(D_RW(ptt->buckets)[i]);
		//for each entry in the bucket
		for (j = 0; j < k; j++){
			pe =  D_RW(D_RW(pline->arr)[j]);
			if (pe->state == PMEM_TX_ACTIVE){
				if (*max_tid < pe->tid){
					*max_tid = pe->tid;
				}

				if (*min_tid > pe->tid){
					*min_tid = pe->tid;
				}
			}
		}
	}

}

/*
 *Create a entry id
 @param[in] type: 1-byte type PMEM_BLOCK_ID*
 @param[in] row: 3-bytes row_id
 @param[in] col: 4-bytes col_id
 @return: The 8-byte block id
 * */
uint64_t
pm_ppl_create_entry_id(
	   	uint16_t type,
	   	uint32_t row,
	   	uint32_t col)
{
	uint64_t ret = 0;
	byte* ptr = (byte*) &ret;
	
	mach_write_to_1(ptr, type);
	ptr++;

	mach_write_to_3(ptr, row);
	ptr += 3;

	mach_write_to_4(ptr, col);
	ptr += 4;

	return ret;
}

/*
 * Parse an entry_id created by pm_ppl_create_entry_id()
 * */
void
pm_ppl_parse_entry_id(
		uint64_t val,
	   	uint16_t* type,
	   	uint32_t* row,
	   	uint32_t* col)
{
	byte* ptr = (byte*)  &val;

	*type = mach_read_from_1(ptr);
	ptr++;

	*row = mach_read_from_3(ptr);
	ptr += 3;

	*col = mach_read_from_4(ptr);
	ptr += 4;
}

/*
 * Write a log rec to PPL
 * Called from mtr::execute()
 * Directly use __pm_write_log_buf as the old version
 * @param[in] pop
 * @param[in] ppl
 * @param[in] key
 * @param[in] log_src
 * @param[in] rec_size - rec size
 * */
//void
uint64_t
pm_ppl_write_rec(
			PMEMobjpool*		pop,
			PMEM_WRAPPER*		pmw,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			key,
			byte*				log_src,
			uint32_t			rec_size)
{
	uint32_t					n;
	PMEM_PAGE_LOG_HASHED_LINE*	pline;
	PMEM_PAGE_LOG_FREE_POOL*	pfreepool;

	TOID(PMEM_PAGE_LOG_BUF)		logbuf;
	PMEM_PAGE_LOG_BUF*			plogbuf;

	PMEM_PAGE_LOG_BLOCK*		plog_block;
	plog_hash_t*				item;

	ulint hashed;
	byte* log_des;
	byte* temp;
	
	mlog_id_t type;
	ulint space, page_no;
	uint16_t check_size;
	
	uint64_t rec_lsn;
	uint64_t write_off;
	uint64_t old_off;
	
#if defined(UNIV_PMEMOBJ_PPL_STAT)
	uint64_t start_time, end_time;
	uint64_t t1, t2;
#endif	

#if defined (UNIV_PMEM_SIM_LATENCY)
	uint64_t start_cycle, end_cycle;
#endif


	assert(rec_size > 0);

	temp = mlog_parse_initial_log_record(
			log_src, log_src + rec_size, &type, &space, &page_no);
	check_size = mach_read_from_2(temp);

	assert (check_size == rec_size);
	assert (type < MLOG_BIGGEST_TYPE);
	temp += 2;
	
	/*The last bucket is reserved for space 0*/	
	n = ppl->n_buckets;

	PMEM_LOG_HASH_KEY(hashed, key, n);

	assert(hashed < n);

retry:
	pline = D_RW(D_RW(ppl->buckets)[hashed]);
	TOID_ASSIGN(logbuf, (pline->logbuf).oid);
	plogbuf = D_RW(pline->logbuf);
	
#if defined(UNIV_PMEMOBJ_PPL_STAT)
	start_time = ut_time_us(NULL);
#endif	
	/*WARNING this lock may become bottle neck*/
	pmemobj_rwlock_wrlock(pop, &pline->lock);

#if defined(UNIV_PMEMOBJ_PPL_STAT)
	end_time = ut_time_us(NULL);
	pline->log_write_lock_wait_time += (end_time - start_time);
	pline->n_log_write++;
#endif

	//if (plogbuf->state == PMEM_LOG_BUF_IN_FLUSH)
	//if (pline->is_flushing)
	if (pline->is_flushing ||
		plogbuf->state == PMEM_LOG_BUF_IN_FLUSH)
	{
		pmemobj_rwlock_unlock(pop, &pline->lock);
		
		/*wait for a logbuf available*/
		os_event_wait(pline->log_flush_event);
		/*wake up, the plogbuf may changed, better to re-acquire it*/
		goto retry;
	}	

	////////////////////////////////////////////
	// (1) Handle full log buf (if any)
	// /////////////////////////////////////////
	if (plogbuf->cur_off + rec_size > plogbuf->size) {
		pline->is_flushing = true;
		os_event_reset(pline->log_flush_event);

get_free_buf:
		// (1.1) Get a free log buf
#if defined(UNIV_PMEMOBJ_PPL_STAT)
	t1 = ut_time_us(NULL);
#endif	
		pfreepool = D_RW(ppl->free_pool);
		pmemobj_rwlock_wrlock(pop, &pfreepool->lock);


		TOID(PMEM_PAGE_LOG_BUF) free_buf = POBJ_LIST_FIRST (&pfreepool->head);
		if (pfreepool->cur_free_bufs == 0 || 
				TOID_IS_NULL(free_buf)){
			//no empty free logbuf, wait for an available one
			pmemobj_rwlock_unlock(pop, &pfreepool->lock);
			os_event_wait(ppl->free_log_pool_event);
			goto get_free_buf;
		}
		POBJ_LIST_REMOVE(pop, &pfreepool->head, free_buf, list_entries);
		pfreepool->cur_free_bufs--;
		
		os_event_reset(ppl->free_log_pool_event);
		pmemobj_rwlock_unlock(pop, &pfreepool->lock);

		assert(D_RW(free_buf)->cur_off == PMEM_LOG_BUF_HEADER_SIZE);
		assert(D_RW(free_buf)->n_recs == 0);

		//test
		if ( D_RW(plogbuf->prev) != NULL){
			printf("PMEM_WARN: there is another in-flushing logbuf id %zu before this full logbuf %zu pline %zu\n", D_RW(plogbuf->prev)->id, plogbuf->id, plogbuf->hashed_id);
		}

		/* (1.2) insert free logbuf into the head*/
		//TOID_ASSIGN(D_RW(free_buf)->prev, pline->logbuf.oid);
		//TOID_ASSIGN(D_RW(pline->logbuf)->next, free_buf.oid);
		//TOID_ASSIGN(pline->logbuf, free_buf.oid);

		TOID_ASSIGN(D_RW(free_buf)->prev, logbuf.oid);
		TOID_ASSIGN(plogbuf->next, free_buf.oid);
		TOID_ASSIGN(pline->logbuf, free_buf.oid);
		
#if defined(UNIV_PMEMOBJ_PPL_STAT)
	t2 = ut_time_us(NULL);
	pline->log_flush_lock_wait_time += (t2 - t1);
	pline->n_log_flush++;
#endif	
		//move the diskaddr on the line ahead, the written size should be aligned with 512B for DIRECT_IO works
		pline->diskaddr += plogbuf->size;
#if defined (UNIV_PMEMOBJ_PERSIST)
		pmemobj_persist(pop, &pline->diskaddr, sizeof(pline->diskaddr));
#endif

		// (1.4) write log rec on new buf
		D_RW(free_buf)->hashed_id = pline->hashed_id; 
		
		log_des = ppl->p_align + D_RW(free_buf)->pmemaddr + D_RW(free_buf)->cur_off;

		/*assign LSN right before write rec*/
		rec_lsn = ut_time_us(NULL);	
		mach_write_to_8(temp, rec_lsn);

		pm_write_log_rec_low(pop,
				log_des,
				log_src,
				rec_size);
		D_RW(free_buf)->n_recs++;

		D_RW(free_buf)->state = PMEM_LOG_BUF_IN_USED;
		D_RW(free_buf)->diskaddr = pline->diskaddr;

		/*IMPORTANT: always update offset after updating plog_block*/
		old_off = D_RW(free_buf)->cur_off;
		D_RW(free_buf)->cur_off += rec_size;
		
#if defined (UNIV_PMEM_SIM_LATENCY)
		PMEM_DELAY(start_cycle, end_cycle, 11 * pmw->PMEM_SIM_CPU_CYCLES); 
#endif
		// (1.5) write the header. This header is needed when recovery,
		byte* header = ppl->p_align + plogbuf->pmemaddr + 0;
		byte* ptr = header;
		mach_write_to_4(ptr, plogbuf->cur_off);
		ptr += 4;

		mach_write_to_4(ptr, plogbuf->n_recs);
		ptr += 4;

		//fill zero the un-used len
		uint64_t dif_len = plogbuf->size - plogbuf->cur_off;
		if (dif_len > 0) {
			memset(header + plogbuf->cur_off, 0, dif_len);
		}
		/*wakeup who is waiting and early release the general lock*/	
		pline->is_flushing = false;
		os_event_set(pline->log_flush_event);

#if defined (UNIV_PMEM_SIM_LATENCY)
		PMEM_DELAY(start_cycle, end_cycle, 4 * pmw->PMEM_SIM_CPU_CYCLES); 
#endif
		/*we update metadata on plogblock and flushing logbuf in non-critical section*/

		/*(2) Get the plogblock*/	
		pmemobj_rwlock_wrlock(pop, &pline->meta_lock);
		item = pm_ppl_hash_check_and_add(pop, ppl, pline, key); 
		pmemobj_rwlock_unlock(pop, &pline->meta_lock);

		assert(item->block_off < pline->max_blocks);

		/*acquire lock on plogblock is not necessary
		 * plog_block->is_free is false after pm_ppl_hash_check_and_add()*/
		plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
		assert(plog_block);

		// (1.3) update plog_block
		if (plog_block->firstLSN == 0){
			//first write
			plog_block->start_off = old_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;

			plog_block->first_rec_size = rec_size;
			plog_block->first_rec_type = type;

#if defined (UNIV_PMEMOBJ_PERSIST)
			pmemobj_persist(pop, &plog_block->start_off, sizeof(plog_block->start_off));
			pmemobj_persist(pop, &plog_block->start_diskaddr, sizeof(plog_block->start_diskaddr));
			pmemobj_persist(pop, &plog_block->firstLSN, sizeof(plog_block->firstLSN));
			pmemobj_persist(pop, &plog_block->first_rec_size, sizeof(plog_block->first_rec_size));
			pmemobj_persist(pop, &plog_block->first_rec_type, sizeof(plog_block->first_rec_type));
#endif

#if defined (UNIV_PMEM_SIM_LATENCY)
		PMEM_DELAY(start_cycle, end_cycle, 5 * pmw->PMEM_SIM_CPU_CYCLES); 
#endif
			//update the oldest
			if (pline->oldest_block_off == UINT32_MAX) {
				pline->oldest_block_off = item->block_off;
#if defined (UNIV_PMEMOBJ_PERSIST)
				pmemobj_persist(pop, &pline->oldest_block_off, sizeof(pline->oldest_block_off));
#endif

#if defined (UNIV_PMEM_SIM_LATENCY)
				PMEM_DELAY(start_cycle, end_cycle, pmw->PMEM_SIM_CPU_CYCLES); 
#endif
			}
			//test
			/*insert the pair (offset, bid) into the set*/
			write_off = plog_block->start_diskaddr + plog_block->start_off;
			pline->offset_map->insert( std::make_pair(write_off, item->block_off));

		}
		plog_block->lastLSN = rec_lsn;
#if defined (UNIV_PMEM_SIM_LATENCY)
		PMEM_DELAY(start_cycle, end_cycle, pmw->PMEM_SIM_CPU_CYCLES); 
#endif

		/*persist the plogblock*/
#if defined (UNIV_PMEMOBJ_PERSIST)
		pmemobj_persist(pop, D_RW(free_buf), sizeof(PMEM_PAGE_LOG_BUF));
		pmemobj_persist(pop, plogbuf, sizeof(PMEM_PAGE_LOG_BUF));
#endif

		// (1.6) assign a pointer in the flusher to the full log buf, this function return immediately 
		pm_log_buf_assign_flusher(ppl, plogbuf);

		pmemobj_rwlock_unlock(pop, &pline->lock);
		/* end critical section */
		//pmemobj_rwlock_unlock(pop, &pline->lock);
		return rec_lsn;
	}//end handle full logbuf
	else {
		/*regular case, remember that this thread is holding the general lock now*/

		log_des = ppl->p_align + plogbuf->pmemaddr + plogbuf->cur_off;
		/*assign LSN right before write rec*/
		rec_lsn = ut_time_us(NULL);	
		mach_write_to_8(temp, rec_lsn);

		pm_write_log_rec_low(pop, log_des, log_src, rec_size);

		plogbuf->n_recs++;
		if (plogbuf->cur_off == PMEM_LOG_BUF_HEADER_SIZE)		  {
			//this is the first write on this logbuf
			plogbuf->state = PMEM_LOG_BUF_IN_USED;
		}

		old_off = plogbuf->cur_off;
		plogbuf->cur_off += rec_size;
		if (PERSIST_AT_WRITE){
			pmemobj_persist(pop, &plogbuf->cur_off, sizeof(plogbuf->cur_off));
		}

#if defined (UNIV_PMEM_SIM_LATENCY)
		PMEM_DELAY(start_cycle, end_cycle, 5 * pmw->PMEM_SIM_CPU_CYCLES); 
#endif
		if (!pline->is_req_checkpoint){
			/*comment this line to disable checkpoint (for debugging)*/
			pm_ppl_check_for_ckpt(pop, ppl, pline, plogbuf, rec_lsn);
		}
		/*early realease the general lock*/
		pmemobj_rwlock_unlock(pop, &pline->lock);

		/*(2) Get the plogblock*/	
		//test unblock
		pmemobj_rwlock_wrlock(pop, &pline->meta_lock);
		item = pm_ppl_hash_check_and_add(pop, ppl, pline, key); 
		pmemobj_rwlock_unlock(pop, &pline->meta_lock);

		assert(item->block_off < pline->max_blocks);

		/*acquire lock on plogblock is not necessary
		 * plog_block->is_free is false after pm_ppl_hash_check_and_add()*/
		plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
		assert(plog_block);

		//pmemobj_rwlock_wrlock(pop, &plog_block->lock);
		//update plog_block
		if (plog_block->firstLSN == 0){
			//first write
			//plog_block->start_off = plogbuf->cur_off;
			plog_block->start_off = old_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;

			plog_block->first_rec_size = rec_size;
			plog_block->first_rec_type = type;

#if defined (UNIV_PMEMOBJ_PERSIST)
			pmemobj_persist(pop, &plog_block->start_off, sizeof(plog_block->start_off));
			pmemobj_persist(pop, &plog_block->start_diskaddr, sizeof(plog_block->start_diskaddr));
			pmemobj_persist(pop, &plog_block->firstLSN, sizeof(plog_block->firstLSN));
			pmemobj_persist(pop, &plog_block->first_rec_size, sizeof(plog_block->first_rec_size));
			pmemobj_persist(pop, &plog_block->first_rec_type, sizeof(plog_block->first_rec_type));
#endif

#if defined (UNIV_PMEM_SIM_LATENCY)
		PMEM_DELAY(start_cycle, end_cycle, 5 * pmw->PMEM_SIM_CPU_CYCLES); 
#endif
			//update the oldest
			if (pline->oldest_block_off == UINT32_MAX) {
				pline->oldest_block_off = item->block_off;
#if defined (UNIV_PMEMOBJ_PERSIST)
				pmemobj_persist(pop, &pline->oldest_block_off, sizeof(pline->oldest_block_off));
#endif
#if defined (UNIV_PMEM_SIM_LATENCY)
				PMEM_DELAY(start_cycle, end_cycle, pmw->PMEM_SIM_CPU_CYCLES); 
#endif
			}
			//test
			/*insert the pair (offset, bid) into the set*/
			write_off = plog_block->start_diskaddr + plog_block->start_off;

			pmemobj_rwlock_wrlock(pop, &pline->meta_lock);
			pline->offset_map->insert( std::make_pair(write_off, item->block_off));
			pmemobj_rwlock_unlock(pop, &pline->meta_lock);

		}

		plog_block->lastLSN = rec_lsn;
#if defined (UNIV_PMEM_SIM_LATENCY)
		PMEM_DELAY(start_cycle, end_cycle, pmw->PMEM_SIM_CPU_CYCLES); 
#endif

		//pmemobj_rwlock_unlock(pop, &plog_block->lock);
		//pmemobj_rwlock_unlock(pop, &pline->lock);
		return rec_lsn;
	} //end handle regular logbuf
	//pmemobj_rwlock_unlock(pop, &pline->lock);
}

uint64_t
pm_ppl_write_rec_v2(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			key,
			byte*				log_src,
			uint32_t			rec_size)
{
	uint32_t					n;
	PMEM_PAGE_LOG_HASHED_LINE*	pline;
	PMEM_PAGE_LOG_FREE_POOL*	pfreepool;
	PMEM_PAGE_LOG_BUF*			plogbuf;
	PMEM_PAGE_LOG_BLOCK*		plog_block;
	plog_hash_t*				item;

	ulint hashed;
	byte* log_des;
	byte* temp;
	
	mlog_id_t type;
	ulint space, page_no;
	uint16_t check_size;
	
	uint64_t rec_lsn;
	uint64_t write_off;
	//uint64_t old_off;
	
	assert(rec_size > 0);

	temp = mlog_parse_initial_log_record(
			log_src, log_src + rec_size, &type, &space, &page_no);
	check_size = mach_read_from_2(temp);

	assert (check_size == rec_size);
	assert (type < MLOG_BIGGEST_TYPE);
	temp += 2;
	
	/*The last bucket is reserved for space 0*/	
	n = ppl->n_buckets;

	PMEM_LOG_HASH_KEY(hashed, key, n);

	assert(hashed < n);

retry:
	pline = D_RW(D_RW(ppl->buckets)[hashed]);
	plogbuf = D_RW(pline->logbuf);
	
	/*WARNING this lock may become bottle neck*/
	pmemobj_rwlock_wrlock(pop, &pline->lock);

	//if (plogbuf->state == PMEM_LOG_BUF_IN_FLUSH)
	//if (pline->is_flushing)
	if (pline->is_flushing ||
		plogbuf->state == PMEM_LOG_BUF_IN_FLUSH)
	{
		pmemobj_rwlock_unlock(pop, &pline->lock);
		/*wait for a logbuf available*/
		os_event_wait(pline->log_flush_event);
		/*wake up, the plogbuf may changed, better to re-acquire it*/
		goto retry;
	}	

	/*(2) Get the plogblock*/	
	//item = pm_ppl_hash_check_and_add(pop, ppl, pline, key); 
	//assert(item->block_off < pline->max_blocks);
	//plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
	//assert(plog_block);

	////assign LSN
	//rec_lsn = ut_time_us(NULL);	
	//mach_write_to_8(temp, rec_lsn);

	////////////////////////////////////////////
	// (1) Handle full log buf (if any)
	// /////////////////////////////////////////
	if (plogbuf->cur_off + rec_size > plogbuf->size) {
		pline->is_flushing = true;
		os_event_reset(pline->log_flush_event);
get_free_buf:
		// (1.1) Get a free log buf
		pfreepool = D_RW(ppl->free_pool);
		pmemobj_rwlock_wrlock(pop, &pfreepool->lock);

		TOID(PMEM_PAGE_LOG_BUF) free_buf = POBJ_LIST_FIRST (&pfreepool->head);
		if (pfreepool->cur_free_bufs == 0 || 
				TOID_IS_NULL(free_buf)){
			//no empty free logbuf, wait for an available one
			pmemobj_rwlock_unlock(pop, &pfreepool->lock);
			os_event_wait(ppl->free_log_pool_event);
			goto get_free_buf;
		}
		POBJ_LIST_REMOVE(pop, &pfreepool->head, free_buf, list_entries);
		pfreepool->cur_free_bufs--;
		
		os_event_reset(ppl->free_log_pool_event);
		pmemobj_rwlock_unlock(pop, &pfreepool->lock);

		assert(D_RW(free_buf)->cur_off == PMEM_LOG_BUF_HEADER_SIZE);
		assert(D_RW(free_buf)->n_recs == 0);

		/* (1.2) insert free logbuf into the head*/
		TOID_ASSIGN(D_RW(free_buf)->prev, pline->logbuf.oid);
		TOID_ASSIGN(D_RW(pline->logbuf)->next, free_buf.oid);
		TOID_ASSIGN(pline->logbuf, free_buf.oid);
		
		//test
		if ( D_RW(plogbuf->prev) != NULL){
			printf("PMEM_WARN: there is another in-flushing logbuf id %zu before this full logbuf %zu pline %zu\n", D_RW(plogbuf->prev)->id, plogbuf->id, plogbuf->hashed_id);
		}
		//move the diskaddr on the line ahead, the written size should be aligned with 512B for DIRECT_IO works
		pline->diskaddr += plogbuf->size;

		/*(2) Get the plogblock*/	
		item = pm_ppl_hash_check_and_add(pop, ppl, pline, key); 
		assert(item->block_off < pline->max_blocks);
		plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
		assert(plog_block);

		//assign LSN
		rec_lsn = ut_time_us(NULL);	
		mach_write_to_8(temp, rec_lsn);

		// (1.3) update plog_block
		if (plog_block->firstLSN == 0){
			//first write
			plog_block->start_off = D_RW(free_buf)->cur_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;

			plog_block->first_rec_size = rec_size;
			plog_block->first_rec_type = type;

			//update the oldest
			if (pline->oldest_block_off == UINT32_MAX) {
				pline->oldest_block_off = item->block_off;
			}
			//test
			/*insert the pair (offset, bid) into the set*/
			write_off = plog_block->start_diskaddr + plog_block->start_off;
			pline->offset_map->insert( std::make_pair(write_off, item->block_off));

		}
		plog_block->lastLSN = rec_lsn;

		// (1.4) write log rec on new buf
		D_RW(free_buf)->hashed_id = pline->hashed_id; 
		
		log_des = ppl->p_align + D_RW(free_buf)->pmemaddr + D_RW(free_buf)->cur_off;

		pm_write_log_rec_low(pop,
				log_des,
				log_src,
				rec_size);
		D_RW(free_buf)->n_recs++;

		D_RW(free_buf)->state = PMEM_LOG_BUF_IN_USED;
		D_RW(free_buf)->diskaddr = pline->diskaddr;

		/*IMPORTANT: always update offset after updating plog_block*/
		D_RW(free_buf)->cur_off += rec_size;

		// (1.5) write the header. This header is needed when recovery,
		byte* header = ppl->p_align + plogbuf->pmemaddr + 0;
		byte* ptr = header;
		mach_write_to_4(ptr, plogbuf->cur_off);
		ptr += 4;

		mach_write_to_4(ptr, plogbuf->n_recs);
		ptr += 4;

		//fill zero the un-used len
		uint64_t dif_len = plogbuf->size - plogbuf->cur_off;
		if (dif_len > 0) {
			memset(header + plogbuf->cur_off, 0, dif_len);
		}

		// (1.6) assign a pointer in the flusher to the full log buf, this function return immediately 
		pm_log_buf_assign_flusher(ppl, plogbuf);

		pline->is_flushing = false;
		os_event_set(pline->log_flush_event);

		pmemobj_rwlock_unlock(pop, &pline->lock);
		return rec_lsn;
	}
	else { 
		/*we only lock the line when necessary*/
		//pmemobj_rwlock_unlock(pop, &pline->lock);

		//plogbuf->cur_off += rec_size;
		/*(2) Get the plogblock*/	
		item = pm_ppl_hash_check_and_add(pop, ppl, pline, key); 
		assert(item->block_off < pline->max_blocks);
		plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
		assert(plog_block);

		//assign LSN
		rec_lsn = ut_time_us(NULL);	
		mach_write_to_8(temp, rec_lsn);
		//update plog_block
		if (plog_block->firstLSN == 0){
			//first write
			plog_block->start_off = plogbuf->cur_off;
			//plog_block->start_off = old_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;

			plog_block->first_rec_size = rec_size;
			plog_block->first_rec_type = type;

			//update the oldest
			if (pline->oldest_block_off == UINT32_MAX) {
				pline->oldest_block_off = item->block_off;
			}
			//test
			/*insert the pair (offset, bid) into the set*/
			write_off = plog_block->start_diskaddr + plog_block->start_off;
			pline->offset_map->insert( std::make_pair(write_off, item->block_off));
		}

		plog_block->lastLSN = rec_lsn;

		log_des = ppl->p_align + plogbuf->pmemaddr + plogbuf->cur_off;
		pm_write_log_rec_low(pop,
				log_des,
				log_src,
				rec_size);

		plogbuf->n_recs++;
		if (plogbuf->cur_off == PMEM_LOG_BUF_HEADER_SIZE)		  {
			//this is the first write on this logbuf
			plogbuf->state = PMEM_LOG_BUF_IN_USED;
		}

		/*IMPORTANT: always update offset after updating plog_block*/
		//old_off = plogbuf->cur_off;
		plogbuf->cur_off += rec_size;

		/* compute ckpt_lsn for this line (in necessary) */

		pmemobj_rwlock_unlock(pop, &pline->lock);
		if (!pline->is_req_checkpoint){
			pm_ppl_check_for_ckpt(pop, ppl, pline, plogbuf, rec_lsn);
		}

		return rec_lsn;
	} //end handle regular logbuf

}
uint64_t
pm_ppl_write_rec_old(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			key,
			byte*				log_src,
			uint32_t			rec_size)
{
	uint32_t					n;
	PMEM_PAGE_LOG_HASHED_LINE*	pline;
	PMEM_PAGE_LOG_FREE_POOL*	pfreepool;
	PMEM_PAGE_LOG_BUF*			plogbuf;
	PMEM_PAGE_LOG_BLOCK*		plog_block;
	plog_hash_t*				item;

	ulint hashed;
	byte* log_des;
	byte* temp;
	
	mlog_id_t type;
	ulint space, page_no;
	uint16_t check_size;
	
	uint64_t rec_lsn;
	uint64_t write_off;
	
	assert(rec_size > 0);

	temp = mlog_parse_initial_log_record(
			log_src, log_src + rec_size, &type, &space, &page_no);
	check_size = mach_read_from_2(temp);

	assert (check_size == rec_size);
	assert (type < MLOG_BIGGEST_TYPE);
	temp += 2;
	
	/*The last bucket is reserved for space 0*/	
	n = ppl->n_buckets;

	PMEM_LOG_HASH_KEY(hashed, key, n);

	assert(hashed < n);

retry:
	pline = D_RW(D_RW(ppl->buckets)[hashed]);
	plogbuf = D_RW(pline->logbuf);
	
	/*WARNING this lock may become bottle neck*/
	pmemobj_rwlock_wrlock(pop, &pline->lock);

	if (plogbuf->state == PMEM_LOG_BUF_IN_FLUSH)
	//if (pline->is_flushing)
	{
		pmemobj_rwlock_unlock(pop, &pline->lock);
		/*wait for a logbuf available*/
		//os_event_wait(pline->log_flush_event);
		/*wake up, the plogbuf may changed, better to re-acquire it*/
		goto retry;
	}	

	/*(2) Get the plogblock*/	

	/*if the hash key has already exist, get it
	otherwise, create the new item and add into the hashtable	
Note: After pm_ppl_hash_add, plog_block->state = PMEM_IN_USED_BLOCK and is_free == false, however, the log rec has not written to logbuf yet!
	*/
	item = pm_ppl_hash_check_and_add(pop, ppl, pline, key); 
	assert(item->block_off < pline->max_blocks);

	/*acquire lock on plogblock is not necessary
	 * plog_block->is_free is false after pm_ppl_hash_check_and_add()*/
	plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
	assert(plog_block);

	//assign LSN
	rec_lsn = ut_time_us(NULL);	
	mach_write_to_8(temp, rec_lsn);

	////////////////////////////////////////////
	// (1) Handle full log buf (if any)
	// /////////////////////////////////////////
	if (plogbuf->cur_off + rec_size > plogbuf->size) {
get_free_buf:
		// (1.1) Get a free log buf
		pfreepool = D_RW(ppl->free_pool);
		pmemobj_rwlock_wrlock(pop, &pfreepool->lock);

		TOID(PMEM_PAGE_LOG_BUF) free_buf = POBJ_LIST_FIRST (&pfreepool->head);
		if (pfreepool->cur_free_bufs == 0 || 
				TOID_IS_NULL(free_buf)){
			//no empty free logbuf, wait for an available one
			pmemobj_rwlock_unlock(pop, &pfreepool->lock);
			os_event_wait(ppl->free_log_pool_event);
			goto get_free_buf;
		}
		POBJ_LIST_REMOVE(pop, &pfreepool->head, free_buf, list_entries);
		pfreepool->cur_free_bufs--;
		
		os_event_reset(ppl->free_log_pool_event);
		pmemobj_rwlock_unlock(pop, &pfreepool->lock);

		assert(D_RW(free_buf)->cur_off == PMEM_LOG_BUF_HEADER_SIZE);
		assert(D_RW(free_buf)->n_recs == 0);

		/* (1.2) insert free logbuf into the head*/
		TOID_ASSIGN(D_RW(free_buf)->prev, pline->logbuf.oid);
		TOID_ASSIGN(D_RW(pline->logbuf)->next, free_buf.oid);
		TOID_ASSIGN(pline->logbuf, free_buf.oid);
		
		//test
		if ( D_RW(plogbuf->prev) != NULL){
			printf("PMEM_WARN: there is another in-flushing logbuf id %zu before this full logbuf %zu pline %zu\n", D_RW(plogbuf->prev)->id, plogbuf->id, plogbuf->hashed_id);
		}
		//move the diskaddr on the line ahead, the written size should be aligned with 512B for DIRECT_IO works
		pline->diskaddr += plogbuf->size;

		// (1.3) update plog_block
		if (plog_block->firstLSN == 0){
			//first write
			plog_block->start_off = D_RW(free_buf)->cur_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;

			plog_block->first_rec_size = rec_size;
			plog_block->first_rec_type = type;

			//update the oldest
			if (pline->oldest_block_off == UINT32_MAX) {
				pline->oldest_block_off = item->block_off;
			}
			//test
			/*insert the pair (offset, bid) into the set*/
			write_off = plog_block->start_diskaddr + plog_block->start_off;
			pline->offset_map->insert( std::make_pair(write_off, item->block_off));

		}
		plog_block->lastLSN = rec_lsn;

		// (1.4) write log rec on new buf
		D_RW(free_buf)->hashed_id = pline->hashed_id; 
		
		log_des = ppl->p_align + D_RW(free_buf)->pmemaddr + D_RW(free_buf)->cur_off;

		pm_write_log_rec_low(pop,
				log_des,
				log_src,
				rec_size);
		D_RW(free_buf)->n_recs++;

		D_RW(free_buf)->state = PMEM_LOG_BUF_IN_USED;
		D_RW(free_buf)->diskaddr = pline->diskaddr;

		/*IMPORTANT: always update offset after updating plog_block*/
		D_RW(free_buf)->cur_off += rec_size;

		// (1.5) write the header. This header is needed when recovery,
		byte* header = ppl->p_align + plogbuf->pmemaddr + 0;
		byte* ptr = header;
		mach_write_to_4(ptr, plogbuf->cur_off);
		ptr += 4;

		mach_write_to_4(ptr, plogbuf->n_recs);
		ptr += 4;

		//fill zero the un-used len
		uint64_t dif_len = plogbuf->size - plogbuf->cur_off;
		if (dif_len > 0) {
			memset(header + plogbuf->cur_off, 0, dif_len);
		}

		// (1.6) assign a pointer in the flusher to the full log buf, this function return immediately 
		pm_log_buf_assign_flusher(ppl, plogbuf);

		pmemobj_rwlock_unlock(pop, &pline->lock);
		return rec_lsn;
	}
	else { 
		/*we only lock the line when necessary*/
		//pmemobj_rwlock_unlock(pop, &pline->lock);

		//update plog_block
		if (plog_block->firstLSN == 0){
			//first write
			plog_block->start_off = plogbuf->cur_off;
			plog_block->start_diskaddr = pline->diskaddr;
			plog_block->firstLSN = rec_lsn;

			plog_block->first_rec_size = rec_size;
			plog_block->first_rec_type = type;

			//update the oldest
			if (pline->oldest_block_off == UINT32_MAX) {
				pline->oldest_block_off = item->block_off;
			}
			//test
			/*insert the pair (offset, bid) into the set*/
			write_off = plog_block->start_diskaddr + plog_block->start_off;
			pline->offset_map->insert( std::make_pair(write_off, item->block_off));
		}

		plog_block->lastLSN = rec_lsn;

		log_des = ppl->p_align + plogbuf->pmemaddr + plogbuf->cur_off;
		pm_write_log_rec_low(pop,
				log_des,
				log_src,
				rec_size);

		plogbuf->n_recs++;
		if (plogbuf->cur_off == PMEM_LOG_BUF_HEADER_SIZE)		  {
			//this is the first write on this logbuf
			plogbuf->state = PMEM_LOG_BUF_IN_USED;
		}

		/*IMPORTANT: always update offset after updating plog_block*/
		plogbuf->cur_off += rec_size;

		/* compute ckpt_lsn for this line (in necessary) */
		if (!pline->is_req_checkpoint){
			pm_ppl_check_for_ckpt(pop, ppl, pline, plogbuf, rec_lsn);
		}

		pmemobj_rwlock_unlock(pop, &pline->lock);
		return rec_lsn;
	} //end handle regular logbuf

}
/*
 * Check and compute the ckpt_lsn value if the logbuf's tail go too far from the head
 * Later, the master thread (1s interval) will call checkpoint based on this value 
 * The caller thread response for holding the pline->lock
 * See log_preflush_pool_modified_pages() in InnoDB
 * */
void
pm_ppl_check_for_ckpt(
			PMEMobjpool*				pop,
			PMEM_PAGE_PART_LOG*			ppl,
			PMEM_PAGE_LOG_HASHED_LINE*	pline,
			PMEM_PAGE_LOG_BUF*			plogbuf,
			uint64_t					cur_lsn
			)
{
	uint64_t oldest_off;
	uint64_t cur_off;
	uint64_t oldest_lsn;
	uint64_t age;

	PMEM_PAGE_LOG_BLOCK*	plog_block_oldest;
	
	if (pline->oldest_block_off == UINT32_MAX){
		//there is no log recs in this partition, nothing to do
		return;
	}

	assert(!pline->is_req_checkpoint);

	plog_block_oldest = D_RW(D_RW(pline->arr)[pline->oldest_block_off]);
	assert (plog_block_oldest);
	
	oldest_off = plog_block_oldest->start_diskaddr + plog_block_oldest->start_off;
	//cur_off = plogbuf->diskaddr + plogbuf->cur_off;
	cur_off = pline->diskaddr + plogbuf->cur_off;
	age = cur_off - oldest_off;
	
	/* we only set ckpt_lsn if it has not set yet */
	if ( age > PMEM_CKPT_MAX_OFFSET) {

		pline->is_req_checkpoint = true;	

		/*now compute the checkpoint lsn for this pline */
		oldest_lsn = plog_block_oldest->firstLSN;

		//uint64_t delta = (uint64_t) ((cur_lsn - oldest_lsn) * 1.0 *  PMEM_CKPT_THRESHOLD); 
		uint64_t delta = (uint64_t) ((cur_lsn - oldest_lsn) / 3.0 *  PMEM_CKPT_THRESHOLD); 
		
		//printf("==> pline %zu trigger ckpt lsn delta %f seconds\n", pline->hashed_id, (delta * 1.0 / 1000000));

		//Method 1
		pline->ckpt_lsn = oldest_lsn + delta;
		//pmemobj_persist(pop, &pline->ckpt_lsn, sizeof(pline->ckpt_lsn));

		//Method 2 => no ckpt call
		//pline->ckpt_lsn = oldest_lsn + 1000000 ;
		
		//Method 3 => call ckpt for each 20 ~ 40 seconds, not godd performance
		//pline->ckpt_lsn = cur_lsn - delta ;
		
		//update the global checkpoint lsn	
		pmemobj_rwlock_wrlock(pop, &ppl->ckpt_lock);
		if (ppl->max_oldest_lsn < pline->ckpt_lsn){
			ppl->max_oldest_lsn = pline->ckpt_lsn;
			//pmemobj_persist(pop, &ppl->max_oldest_lsn, sizeof(ppl->max_oldest_lsn));
		}

		//printf("SET is_req_checkpoint to true pline %zu \n", pline->hashed_id);
		pmemobj_rwlock_unlock(pop, &ppl->ckpt_lock);
	}
}

/*
 * Write REDO log records from mtr's heap to PMEM in partitioned-log   
 @param[in] pop
 @param[in] ppl
 @param[in] tid		trx id
 @param[in] log_src pointer to start of log rec source
 @param[in] size	total length of log recs
 @param[in] n_recs	number of log recs
 @param[in] key_arr	array of key
 @param[in] size_arr rec len array
 @param[out] ret_start_lsn	LSN of the first log recs after insert in PPL
 @param[out] ret_end_lsn	LSN of the last log recs after insert in PPL
 @param[in]	entry_id	TT entry id, -1 for the first time transaction write on PPL. Otherwise != -1
 @return the TT entry id
	
 * Return the entry id of TT  write on
 * */
uint64_t
pm_ppl_write(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			tid,
			byte*				log_src,
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			size_arr,
			uint64_t*			ret_start_lsn,
			uint64_t*			ret_end_lsn,
			uint64_t			entry_id)
{
	uint64_t ret;

	uint16_t type;
	uint32_t row;
	uint32_t col;

	ulint hashed;
	ulint i;

	uint64_t n;
	//uint64_t k;
	uint64_t bucket_id, local_id;



	PMEM_TT* ptt;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;


	TOID(PMEM_TT_ENTRY) entry;
	PMEM_TT_ENTRY* pe;

	
	//Starting from TT is easier and faster
	ptt = D_RW(ppl->tt);

	assert(ptt != NULL);
	assert(n_recs > 0);

	pm_ppl_parse_entry_id(entry_id, &type, &row, &col);

	n = ptt->n_buckets;
	//k = ptt->n_entries_per_bucket;
	
	if (type == PMEM_EID_UNDEFINED)
	{
		//Case A - special bucket
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
					size,
					n_recs,
					key_arr,
					size_arr,
					ret_start_lsn,
					ret_end_lsn,
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
				size,
				n_recs,
				key_arr,
				size_arr,
				ret_start_lsn,
				ret_end_lsn,
				pe,
				false);
		}
		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[local_id])->lock);
		return ret;
	} //end special case
	else if (type == PMEM_EID_NEW) 
	//else if (entry_id == -1) 
	{
		//Case B: A first log write of this transaction
		//Search the right entry to write
		//(1) Hash the tid
		PMEM_LOG_HASH_KEY(hashed, tid, n);
		assert (hashed < n);

		TOID_ASSIGN(line, (D_RW(ptt->buckets)[hashed]).oid);
		pline = D_RW(line);
		assert(pline);

		//(2) Search for the free entry to write on
		//lock the hash line

		i = 0;
retry:
		//a reallocate may come here
		for (; i < pline->max_entries; i++)
		{
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			TOID_ASSIGN (entry, (D_RW(pline->arr)[i]).oid);
			pe = D_RW(entry);
			if (pe->state == PMEM_TX_FREE){
				//found a free entry to write on
				pe->state = PMEM_TX_ACTIVE;
				pe->tid = tid;

				//create the index entry for transaction
				//ret = hashed * k + i;
				ret = pm_ppl_create_entry_id(
						PMEM_EID_REVISIT,
						hashed,
						i);
				
				__handle_pm_ppl_write_by_entry(
						pop,
						ppl,
						tid,
						log_src,
						size,
						n_recs,
						key_arr,
						size_arr,
						ret_start_lsn,
						ret_end_lsn,
						pe,
						true);

				//handle update log
				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
	//printf("END case A add log for tid %zu at entry %zu n_recs %zu ret %zu \n", tid,  entry_id, n_recs, ret);
				return ret;
			}	
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			//jump a litte far to avoid contention
			//i = (i + JUMP_STEP) % pline->n_entries;
			//try again
		} //end for
		assert(i == pline->max_entries);

		//there is no empty TT entries in this line, extend double current size
		__realloc_TT_line(pop, ppl, pline, pline->max_entries * 2);
		goto retry;
		
	}//end Case A
	else {
		//Case B: Get the entry from the input id
		assert(type == PMEM_EID_REVISIT);

		ret = entry_id;

		//bucket_id = entry_id / k;
		//local_id = entry_id % k;
		
		bucket_id = row;
		local_id = col;

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
				size,
				n_recs,
				key_arr,
				size_arr,
				ret_start_lsn,
				ret_end_lsn,
				pe,
				false);

		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[local_id])->lock);

	//printf("END case B add log for tid %zu at entry %zu n_recs %zu bucket_id %zu local_id %zu \n", tid,  entry_id, n_recs, bucket_id, local_id);
		return ret;
	}//end case B
}

/*key = fold(space, page_id) */
PMEM_PAGE_LOG_BLOCK*
pm_ppl_get_log_block_by_key(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			key)
{
	ulint hashed;
	ulint j;

	uint64_t n;
	//uint64_t k;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	//k = ppl->n_blocks_per_bucket;

	PMEM_LOG_HASH_KEY(hashed, key, n);
	assert (hashed < n);

	TOID_ASSIGN(line, (D_RW(ppl->buckets)[hashed]).oid);
	pline = D_RW(line);
	assert(pline);
	//assert(pline->n_blocks == k);

	//for log block on the hashed line
	for (j = 0; j < pline->max_blocks; j++){
		plog_block = D_RW(D_RW(pline->arr)[j]);
		if (!plog_block->is_free &&
				plog_block->key == key){
			return plog_block;
		}
	}
	return NULL;
}

PMEM_PAGE_LOG_BLOCK*
__get_log_block_by_id(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			uint64_t			bid)
{
	uint64_t bucket_id, local_id;
	uint16_t type;
	uint32_t row, col;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	pm_ppl_parse_entry_id(bid, &type, &row, &col);
	bucket_id = row;
	local_id = col;

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
			uint64_t			size,
			uint64_t			n_recs,
			uint64_t*			key_arr,
			uint64_t*			size_arr,
			uint64_t*			ret_start_lsn,
			uint64_t*			ret_end_lsn,
			PMEM_TT_ENTRY*		pe,
			bool				is_new)
{
	ulint i, j;

	uint64_t key;	
	uint64_t rec_size;// log record size
	uint64_t cur_off;
	uint64_t LSN = 0;	// log record LSN

	PMEM_PAGE_REF* pref;

	i = 0;
	cur_off = 0;

	while (cur_off < size){
		// retrieve info
		key = key_arr[i];
		rec_size = size_arr[i];

		//find the corresponding pageref with this log rec
		//for (j = 0; j < pe->n_dp_entries; j++) 
		for (j = 0; j < pe->max_dp_entries; j++) 
		{
			pref = D_RW(D_RW(pe->dp_arr)[j]);
			if (pref->key == key){
				//pageref already exist
				break;
			}
		} //end for each pageref in entry

		//if (j < pe->n_dp_entries)
		if (j < pe->max_dp_entries)
		{
			//pageref already exist, we don't increase count
			//Note: Because pm_ppl_flush() may reset a plogblock even though it's count > 0, check for out-of-date
			PMEM_PAGE_LOG_BLOCK* plog_block =
				__get_log_block_by_id(pop, ppl, pref->idx);
			if (!plog_block->is_free &&
					plog_block->key == key){
				
				//update log block			
				__update_page_log_block_on_write(
						pop,
						ppl,
						log_src,
						cur_off,
						rec_size,
						key,
						&LSN,
						pref->idx);

				pref->pageLSN = LSN;
			}
			else{
				//pref is out-of-date, the plog_block is either reset now or the key is not match (another trx has occupied the reset plog_block)
				//pref->idx = -1;

				uint64_t new_bid = __update_page_log_block_on_write(
						pop,
						ppl,
						log_src,
						cur_off,
						rec_size,
						key,
						&LSN,
						PMEM_DUMMY_EID);

				//update pref
				pref->key = key;
				pref->idx = new_bid;
				pref->pageLSN = LSN;
			}
		}
		else {
			// new pageref, increase the count
			//find the first free pageref from the beginning to reuse the reclaim index
			//for (j = 0; j < pe->n_dp_entries; j++) 
			for (j = 0; j < pe->max_dp_entries; j++) 
			{
				pref = D_RW(D_RW(pe->dp_arr)[j]);
				if (pref->idx == PMEM_DUMMY_EID){
					//found
					break;
				}
			}

			//if (j == pe->n_dp_entries)
			if (j == pe->max_dp_entries)
			{

				//pe is full
				__realloc_TT_entry(pop, ppl, pe, pe->max_dp_entries * 2);

				pref = D_RW(D_RW(pe->dp_arr)[j]);
				//pe->n_dp_entries++;
			}

			//update log block
			uint64_t new_bid = __update_page_log_block_on_write(
					pop,
					ppl,
					log_src,
					cur_off,
					rec_size,
					key,
					&LSN,
					PMEM_DUMMY_EID);

			pref->key = key;
			pref->idx = new_bid;
			pref->pageLSN = LSN;

		}
		
		if (i == 0){
			*ret_start_lsn = LSN;
		}
		cur_off += rec_size;
		i++;
	} //end while
	*ret_end_lsn = LSN;
	assert(*ret_start_lsn <= *ret_end_lsn);	
	assert(i == n_recs);
}


/*
 * Write a log record on per-page partitioned log
 * log_src (in): pointer to the beginning of array log records in mini-transaction's heap
 *@param[in] cur_off: start offset of current log record
 *@param[in] rec_size: size of current log record
 *@param[in] key: fold of page_no and space 
 *@param[out] LSN : log record's LSN
 *@param[in] eid: entry id of the TT entry
 *@param[in] bid:
 * -1: no ref
 *  Otherwise: reference to the log block to write 
 *
 *@return: the bid of the block written
 * */
uint64_t
__update_page_log_block_on_write(
			PMEMobjpool*		pop,
			PMEM_PAGE_PART_LOG*	ppl,
			byte*				log_src,
			uint64_t			cur_off,
			uint64_t			rec_size,
			uint64_t			key,
			uint64_t*			LSN,
			uint64_t			block_id)
{

	uint64_t ret;

	uint16_t type;
	uint32_t row;
	uint32_t col;

	ulint hashed;
	ulint i;

	uint64_t n;
	uint64_t bucket_id, local_id;
	int64_t n_try;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;
	
	n = ppl->n_buckets;
	//k = ppl->n_blocks_per_bucket;

	if (block_id == PMEM_DUMMY_EID) {
		//Case A: there is no help from fast access, the log block may exist or not. Search from the beginning
		PMEM_LOG_HASH_KEY(hashed, key, n);
		assert (hashed < n);

		TOID_ASSIGN(line, (D_RW(ppl->buckets)[hashed]).oid);
		pline = D_RW(line);
		assert(pline);
		
		//(1) search for the exist log block O(k)
		//n_try = k;
		n_try = pline->max_blocks;

		//PMEM_LOG_HASH_KEY(i, key, k);
		PMEM_LOG_HASH_KEY(i, key, pline->max_blocks);

		while (n_try > 0){
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			plog_block = D_RW(D_RW(pline->arr)[i]);
			if (!plog_block->is_free &&
				plog_block->key == key){
				//Case A1: exist block
				//ret = plog_block->bid;
				ret = pm_ppl_create_entry_id(PMEM_EID_REVISIT, hashed, i);
				
				//(2) append log
				__pm_write_log_buf(
						pop,
						ppl,
						hashed,
						log_src,
						cur_off,
						rec_size,
						LSN,
						plog_block,
						false);

				//plog_block->n_log_recs++;

				//(3) update metadata
				//inc number of active tx on this page			
				plog_block->count++;

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				return ret;
			}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);

			n_try--;
			i = (i + 1) % pline->max_blocks;
			//next log block
		}//end search for exist log block

retry:
		//Case A2: If you reach here, then the log block is not existed, write the new one. During the scan, some log block may be reclaimed, search from the begin


		//search for a free block to write on O(k)
		//n_try = k;
		n_try = pline->max_blocks;

		//PMEM_LOG_HASH_KEY(i, key, k);
		PMEM_LOG_HASH_KEY(i, key, pline->max_blocks);

		while( n_try > 0) {
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			plog_block = D_RW(D_RW(pline->arr)[i]);
			if (plog_block->is_free){
				//found, write data on the new block
				//ret = plog_block->bid;
				ret = pm_ppl_create_entry_id(PMEM_EID_REVISIT, hashed, i);

				//update the bid
				plog_block->bid = ret;

				plog_block->is_free = false;
				plog_block->state = PMEM_IN_USED_BLOCK;
				plog_block->key = key;

				//(2) append log
				__pm_write_log_buf(
						pop,
						ppl,
						hashed,
						log_src,
						cur_off,
						rec_size,
						LSN,
						plog_block,
						true);

				//plog_block->n_log_recs++;

				//(3) update metadata
				assert(plog_block->count == 0);
				plog_block->count = 1;

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);

				return ret;
			}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);

			n_try--;
			i = (i + 1) % pline->max_blocks;
			//next
		}//end search for a free block to write on

		//If you reach here, then there is no free block, extend
		//we need to lock the whole line to prevent threads reallocate multiple time
		__realloc_page_log_block_line(pop, ppl, pline, pline->max_blocks * 2);

		goto retry;
	}//end CASE A
	else {
		//Case B: fast access mode, similar to case A1
		ret = block_id;

		//bucket_id = block_id / k;
		//local_id = block_id % k;
		pm_ppl_parse_entry_id(block_id, &type, &row, &col);
		assert(type == PMEM_EID_REVISIT);
		bucket_id = row;
		local_id = col;

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
				bucket_id,
				log_src,
				cur_off,
				rec_size,
				LSN,
				plog_block,
				false);
		//plog_block->n_log_recs++;

		//because block_id != -1, the txref must exist, no need to update tx_idx_arr, n_tx_idx, and count
		
		pmemobj_rwlock_unlock(pop, &plog_block->lock);
		return ret;
	} //end case B
}
/*
 *Write a log rec to a log buf
 @param[in] ppl:pointer to the part-log to get the free pool
 @param[in] hashed_id: id of the hashed line
 @param[in] log_src: pointer to the log rec
 @param[in] src_off: offset on the log_src of the log rec
 @param[in] rec_size: len of log rec
 @param[out] rec_lsn LSN of the log rec, is the timestamp when it is insert to logbuf
 @param[in] plog_block: pointer to the log block
 @param[in] is_first_write: true if it is the first write on the log_block
 * */
void
__pm_write_log_buf(
			PMEMobjpool*				pop,
			PMEM_PAGE_PART_LOG*			ppl,
			//PMEM_PAGE_LOG_HASHED_LINE*	pline,
			uint64_t					hashed_id,
			byte*						log_src,
			uint64_t					src_off,
			uint64_t					rec_size,
			uint64_t*					rec_lsn,
			PMEM_PAGE_LOG_BLOCK*		plog_block,
			bool						is_first_write)
{

	byte* log_des;
	byte* temp;

	mlog_id_t type;
	ulint space, page_no;
	uint16_t check_size;

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
	// (0) We assign LSN used OS timpestamp
	// check size and 
	// write 8-byte rec_lsn field on log rec
	// ////////////////////////////////////////
	
	*rec_lsn = ut_time_us(NULL);	
	
	//check size and type
	temp = log_src + src_off;

	temp = mlog_parse_initial_log_record(
			temp, temp + rec_size, &type, &space, &page_no);
	check_size = mach_read_from_2(temp);

	assert (check_size == rec_size);
	assert (type < MLOG_BIGGEST_TYPE);

	temp += 2;
	//write lsn to rec_lsn 8-byte slot, we reserved it in mlog_write_initial_log_record_low()
	mach_write_to_8(temp, *rec_lsn);

	////////////////////////////////////////////
	// (1) Handle full log buf (if any)
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
		TOID_ASSIGN(pline->tail_logbuf, pline->logbuf.oid);
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
			plog_block->firstLSN = *rec_lsn;
		}
		assert(plog_block->lastLSN <= *rec_lsn);
		plog_block->lastLSN = *rec_lsn;


		pm_write_log_rec_low(pop,
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
			plog_block->firstLSN = *rec_lsn;
		}
		assert(plog_block->lastLSN <= *rec_lsn);
		plog_block->lastLSN = *rec_lsn;

		log_des = ppl->p_align + plogbuf->pmemaddr + plogbuf->cur_off;

		pm_write_log_rec_low(pop,
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

///////////////////////////////////////////////////
////////////////// FLUSHER ////////////////////////
////////////////////////////////////////////////////
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
		printf("PMEM_ERROR in pm_log_buf_assign_flusher() requested/size = %zu /%zu \n", flusher->n_requested, flusher->size);
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
	TOID(PMEM_PAGE_LOG_BUF) logbuf;

	assert(plogbuf);
	TOID_ASSIGN(logbuf, plogbuf->self);

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
 * @param[in] pop
 * @param[in] ppl
 * @param[in] node - the relevant log file node with the logbuf
 * @param[in] plogbuf - the finished AIO logbuf
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
	
	if (plogbuf->state == PMEM_LOG_BUF_FREE){
		/*this logbuf has already reset*/
		printf("PMEM WARN handle finished an free logbuf id %zu pline %zu \n", plogbuf->id, plogbuf->hashed_id);
		return;
	}		
	/*(1)flush file */
	assert(node->is_open);
	os_file_flush(node->handle);

	/*advance the write_diskaddr
	 * write_diskaddr may smaller than diskaddr multiple logbuf's sizes, i.e. 
	 * write_diskaddr + k*plogbuf->size == diskaddr
	 * 0 <= k <= n
	 * */
	
	/*(2) remove plogbuf from the linked-list*/
	PMEM_PAGE_LOG_BUF* next = D_RW(plogbuf->next);	
	PMEM_PAGE_LOG_BUF* prev = D_RW(plogbuf->prev);	
	
	assert(next);

	if (prev == NULL){
		//this logbuf is the head (the most common case)

		pline = D_RW(D_RW(ppl->buckets)[plogbuf->hashed_id]);
		assert(pline);

		pmemobj_rwlock_wrlock(pop, &pline->lock);
		TOID_ASSIGN(pline->tail_logbuf, (plogbuf->next).oid);
		TOID_ASSIGN(next->prev, OID_NULL);

		//update the persistent addr
		pline->write_diskaddr = plogbuf->diskaddr + plogbuf->size;

		pmemobj_rwlock_unlock(pop, &pline->lock);
		
	} else {
		// the rare case
		printf("===> PMEM_INFO rare case of finishing AIO logbuf_id %zu prev_id %zu next_id %zu. This may cause log holes when recover\n", plogbuf->id, prev->id, next->id);
		TOID_ASSIGN(prev->next, (plogbuf->next).oid);
		TOID_ASSIGN(next->prev, (plogbuf->prev).oid);
		//we don't update persistent addr
	}
	/* (3) reset the log buf */
	//printf("PMEM_TEST reset plogbuf id %zu on pline %zu\n", plogbuf->id, plogbuf->hashed_id);

	plogbuf->state = PMEM_LOG_BUF_FREE;
	//plogbuf->cur_off = 0;
	plogbuf->cur_off = PMEM_LOG_BUF_HEADER_SIZE;
	plogbuf->n_recs = 0;
	plogbuf->hashed_id = -1;
	plogbuf->diskaddr = 123; //dummy offset 

	TOID_ASSIGN(plogbuf->next, OID_NULL);
	TOID_ASSIGN(plogbuf->prev, OID_NULL);

	//reset the ref in line

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
		printf("PMEM_ERROR in pm_log_buf_assign_redoer() requested/size = %zu /%zu \n", redoer->n_requested, redoer->size);

		mutex_exit(&redoer->mutex);
		assert (n_try);
	}

	mutex_exit(&redoer->mutex);
}

/*
 * Write log rec from transaction's heap to NVDIMM
 * @param[in]: pop
 * @param[in/out]: log_des the destination 
 * @param[in]: log_src the source
 * @param[in]: rec_size data size
 *
 * */
void
__pm_write_log_rec_low(
			PMEMobjpool*			pop,
			byte*					log_des,
			byte*					log_src,
			uint64_t				size)
{

	pmemobj_memcpy_persist(
			pop, 
			log_des,
			log_src,
			size);
	//if (size <= CACHELINE_SIZE){
	//	//We need persistent copy, Do not need a transaction for atomicity
	//		pmemobj_memcpy_persist(
	//			pop, 
	//			log_des,
	//			log_src,
	//			size);

	//}
	//else{
	//	TX_BEGIN(pop) {
	//		TX_MEMCPY(log_des, log_src, size);
	//	}TX_ONABORT {
	//	}TX_END
	//}
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
		uint64_t				eid) 
{
	//test new DAL
	return;

	uint64_t i;
	uint64_t bucket_id, local_id;

	uint16_t type;
	uint32_t row, col;

	PMEM_TT* ptt;

	TOID(PMEM_TT_HASHED_LINE) line;
	PMEM_TT_HASHED_LINE* pline;

	TOID(PMEM_TT_ENTRY) entry;
	PMEM_TT_ENTRY* pe;
	PMEM_PAGE_REF* pref;

	//(1) retreive the pe by eid
	pm_ppl_parse_entry_id(eid, &type, &row, &col);

	assert(type == PMEM_EID_REVISIT);
	//assert(eid != -1);
	assert(tid);

	ptt = D_RW(ppl->tt);
	assert(ptt != NULL);


	bucket_id = row;
	local_id = col;


	TOID_ASSIGN(line, (D_RW(ptt->buckets)[bucket_id]).oid);
	pline = D_RW(line);
	assert(pline);
	TOID_ASSIGN (entry, (D_RW(pline->arr)[local_id]).oid);
	pe = D_RW(entry);
	assert(pe->tid == tid);

	pmemobj_rwlock_wrlock(pop, &pe->lock);
	
	//(2) for each pageref in the entry	
	for (i = 0; i < pe->max_dp_entries; i++) 
	{
		pref = D_RW(D_RW(pe->dp_arr)[i]);
		//if (pref->idx >= 0)
		if (pref->idx != PMEM_DUMMY_EID)
		{
			//Double-check for out-of-date log_block
			PMEM_PAGE_LOG_BLOCK* plog_block =
				__get_log_block_by_id(pop, ppl, pref->idx);
			if (plog_block->is_free || plog_block->key == 0){
				pref->idx = PMEM_DUMMY_EID;
			}
			else {
				//update the corresponding log block and try to reclaim it
				__update_page_log_block_on_commit(
						pop, ppl, pref, pe->eid);
			}
		}
	} //end for each pageref in the entry

	__reset_TT_entry(pop, ppl, pe);
	
	pmemobj_rwlock_unlock(pop, &pe->lock);
	return;
}


/*
 * Update the page log block on commit
 * Called by pm_ppl_commit()
 *
 * @param[in]: pop
 * @param[in]: ppl
 * @param[in]: pref the page reference in TT entry help to retrieve the log block
 * @param[in] eid: the eid of the transaction, use this to invalid txref in the log block because the TT entry will be reclaim soon
 * */
bool
__update_page_log_block_on_commit(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG* ppl,
		PMEM_PAGE_REF*		pref,
		uint64_t			eid)
{
	uint64_t bucket_id, local_id;

	uint16_t type;
	uint32_t row, col;

	assert(pref != NULL);
	assert(pref->idx != PMEM_DUMMY_EID);

	uint64_t bid = pref->idx;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK* plog_block;

	
	//(1) Get the log block by block id
	

	//bucket_id = bid / k;
	//local_id = bid % k;
	
	pm_ppl_parse_entry_id(bid, &type, &row, &col);
	bucket_id = row;
	local_id = col;

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
		//assert(0);
		return false;
	}

	plog_block->count--;

	//(2) Reclaim the log block

	if (plog_block->count <= 0 &&
		plog_block->lastLSN <= plog_block->pageLSN)
	{
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
	plog_block->is_free = true;
	plog_block->state = PMEM_FREE_BLOCK;
	plog_block->key = 0;
	plog_block->count = 0;
	
	//plog_block->bid = PMEM_DUMMY_EID;
	//plog_block->cur_size = 0;
	//plog_block->n_log_recs = 0;

	plog_block->pageLSN = 0;
	plog_block->firstLSN = 0;
	plog_block->lastLSN = 0;
	plog_block->start_off = 0;
	plog_block->start_diskaddr = 0;

	plog_block->first_rec_found = 0;
	plog_block->first_rec_size = 0;
	plog_block->first_rec_type = (mlog_id_t) 0;
}

/*
 * Call when a cleaner thread flush page from buffer pool.
 * Set a plogblock state to PMEM_IN_FLUSH_BLOCK but not modify plogblock
 * On AIO complete, reset ploglock
 * */
void
pm_ppl_set_flush_state(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		buf_page_t*			bpage)
{
	ulint hashed;
	uint32_t n;
	uint64_t key;
	plog_hash_t* item;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	key = bpage->id.fold();

	n = ppl->n_buckets;
	PMEM_LOG_HASH_KEY(hashed, key, n);

	pline = D_RW(D_RW(ppl->buckets)[hashed]);
	assert(pline);

	item = pm_ppl_hash_get(pop, ppl, pline, key);
	if (item != NULL){
		plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
		assert(plog_block->key == key);
		assert(plog_block->state = PMEM_IN_USED_BLOCK);

		pmemobj_rwlock_wrlock(pop, &plog_block->lock);
		plog_block->state = PMEM_IN_FLUSH_BLOCK;
		pmemobj_rwlock_unlock(pop, &plog_block->lock);
	}
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
		PMEM_WRAPPER*		pmw,
		PMEM_PAGE_PART_LOG*	ppl,
		buf_page_t*			bpage,
		uint64_t			space,
		uint64_t			page_no,
		uint64_t			key,
		uint64_t			pageLSN) 
{
#if defined (UNIV_PMEM_SIM_LATENCY)
	uint64_t start_cycle, end_cycle;
#endif

	ulint hashed;
	uint32_t n;
	
	uint64_t write_off;
	uint64_t min_off;

	plog_hash_t* item;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	
	//(1) Start from the per-page log block
	
	PMEM_LOG_HASH_KEY(hashed, key, n);

	assert (hashed < n);

	TOID_ASSIGN(line, (D_RW(ppl->buckets)[hashed]).oid);
	pline = D_RW(line);
	assert(pline);

	//new implement using hashtable
	item = pm_ppl_hash_get(pop, ppl, pline, key);
	if (item != NULL){
		assert(item->block_off < pline->max_blocks);

		plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
		assert(plog_block->key == key);
		//if (plog_block->state != PMEM_IN_FLUSH_BLOCK){
		//	printf("===> WARN in pm_ppl_flush_page() plog_block->state is %zu\n", plog_block->state);
		//}
		//assert(plog_block->state == PMEM_IN_FLUSH_BLOCK);	
		//pmemobj_rwlock_wrlock(pop, &pline->lock);
		pmemobj_rwlock_wrlock(pop, &pline->meta_lock);
		pmemobj_rwlock_wrlock(pop, &plog_block->lock);

		/*save the write_off before reseting*/	
		write_off = plog_block->start_diskaddr + plog_block->start_off;

		if (USE_BIT_ARRAY) {
			pm_bit_clear(pline, pline->bit_arr, sizeof(long long), plog_block->id);
		}

		__reset_page_log_block(plog_block);
#if defined (UNIV_PMEMOBJ_PERSIST)
		pmemobj_persist(pop, plog_block, sizeof(PMEM_PAGE_LOG_BLOCK));
#endif

#if defined (UNIV_PMEM_SIM_LATENCY)
		/*the reset function takes 12 accesses*/
		PMEM_DELAY(start_cycle, end_cycle, 12 * pmw->PMEM_SIM_CPU_CYCLES); 
#endif

		pmemobj_rwlock_unlock(pop, &plog_block->lock);

		/*remove item from map*/	

		std::map<uint64_t, uint32_t>::iterator it;
		it = pline->offset_map->find(write_off);

		if (it != pline->offset_map->end()){
			pline->offset_map->erase(it);
		}
		else {
			/*write_off is not found, logical error*/
			assert(0);
		}

		/*if the removed block is the oldest, update the new one	*/
		if (item->block_off == pline->oldest_block_off){
			/*Method 1: scan in the array (slow)*/
			//pm_ppl_update_oldest(pop, ppl, pline);

			/*Method 2: use std::map */
			min_off = ULONG_MAX;
			if (pline->offset_map->size() > 0){
				/* get the next min offset*/
				it = pline->offset_map->begin();

				min_off = it->second;
				PMEM_PAGE_LOG_BLOCK* pmin_log_block;
				pmin_log_block = D_RW(D_RW(pline->arr)[min_off]);
				/*the second smallest must larger than the smallest*/
				if (pmin_log_block->start_diskaddr + pmin_log_block->start_off <= write_off){
					printf("===> PMEM_ERROR in pm_ppl_flush_page, second smallest (%zu + %zu = %zu) must larger than the smallest write_off %zu",
							pmin_log_block->start_diskaddr,
							pmin_log_block->start_off,
							pmin_log_block->start_diskaddr + pmin_log_block->start_off,
							write_off);
					assert(pmin_log_block->start_diskaddr + pmin_log_block->start_off > write_off);
				}

				if (pline->is_req_checkpoint){
					if (pmin_log_block->firstLSN > pline->ckpt_lsn){
						pline->is_req_checkpoint = false;
					}
				}
			} else {
				pline->is_req_checkpoint = false;
			}

			pline->oldest_block_off = min_off;	
#if defined (UNIV_PMEM_SIM_LATENCY)
			PMEM_DELAY(start_cycle, end_cycle, 11 * pmw->PMEM_SIM_CPU_CYCLES); 
#endif

		}//end if (item->block_off == ...)
		//pmemobj_rwlock_wrlock(pop, &pline->meta_lock);
		HASH_DELETE(plog_hash_t, addr_hash, pline->addr_hash, key, item);
		pmemobj_rwlock_unlock(pop, &pline->meta_lock);
		//pmemobj_rwlock_unlock(pop, &pline->lock);
	}	
	
	return;
}

/*
 * Update the oldest_block_off in pline
 * Called in pm_ppl_flush_page()
 * The caller reponse for holding the pline->lock
 * */
void
pm_ppl_update_oldest(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*		ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline
		)
{

	uint32_t i;

	uint64_t min_off;
	uint64_t tem_off;
	uint64_t oldest_block_off;
	uint64_t oldest_lsn;

	PMEM_PAGE_LOG_BLOCK*	plog_block;

	oldest_block_off = UINT32_MAX;
	min_off = ULONG_MAX;
	oldest_lsn = 0;

	for (i = 0; i < pline->max_blocks; i++){
		plog_block = D_RW(D_RW(pline->arr)[i]);
		assert(plog_block);
		
		tem_off = plog_block->start_diskaddr + plog_block->start_off;
		if (!plog_block->is_free &&
			   	min_off > tem_off){
			min_off = tem_off;
			oldest_block_off = i;	
			oldest_lsn = plog_block->firstLSN;
		}
	}
	pline->oldest_block_off = oldest_block_off;
	
	if (min_off == ULONG_MAX){
		pline->is_req_checkpoint = false;
		return;
	}

	//whether it still checkpoint or not
	if (pline->is_req_checkpoint){
		if (oldest_lsn > pline->ckpt_lsn){
			pline->is_req_checkpoint = false;
			//printf("UNSET is_req_checkpoint to false pline %zu \n", pline->hashed_id);
		}
	}
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
	
	uint32_t n, i;
	uint64_t max_log_write_lock_wait_time = 0;
	uint64_t max_log_flush_lock_wait_time = 0;

	double avg_log_write_lock_wait_time = 0;
	double avg_log_flush_lock_wait_time = 0;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;

	n = ppl->n_buckets;

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
