/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
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

#include "my_pmem_common.h"
#include "my_pmemobj.h"

#include "os0file.h"
#include "buf0dblwr.h"


#if defined (UNIV_PMEM_SIM_LATENCY)
static uint64_t PMEM_SIM_LATENCY = 1000;
//static float PMEM_CPU_FREQ = 2.2;
static uint64_t PMEM_SIM_CPU_CYCLES = PMEM_SIM_LATENCY * PMEM_CPU_FREQ;
#endif //UNIV_PMEM_SIM_LATENCY

#if defined (UNIV_PMEMOBJ_BUF)
/*
  There are two types of structure need to be allocated: structures in NVM that non-volatile after shutdown server or power-off and D-RAM structures that only need when the server is running. 
 * Case 1: First time the server start (fresh server): allocate structures in NVM
 * Case 2: server've had some data, NVM structures already existed. Just get them
 *
 * For both cases, we need to allocate structures in D-RAM
 * */
void
pm_wrapper_buf_alloc_or_open(
		PMEM_WRAPPER*		pmw,
		const size_t		buf_size,
		const size_t		page_size)
{

	uint64_t i;
	char sbuf[256];

	PMEM_N_BUCKETS = srv_pmem_buf_n_buckets;
	PMEM_BUCKET_SIZE = srv_pmem_buf_bucket_size;
	PMEM_BUF_FLUSH_PCT = srv_pmem_buf_flush_pct;
#if defined (UNIV_PMEMOBJ_BUF_FLUSHER)
	PMEM_N_FLUSH_THREADS= srv_pmem_n_flush_threads;
	PMEM_FLUSHER_WAKE_THRESHOLD = srv_pmem_flush_threshold;
#endif

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_N_BUCKET_BITS = log2(srv_pmem_buf_n_buckets);
	PMEM_N_SPACE_BITS = srv_pmem_n_space_bits;
	PMEM_PAGE_PER_BUCKET_BITS = srv_pmem_page_per_bucket_bits;
	printf("======> >> > >PMEM PARTITION: n_bucket_bits %zu n_space_bits %zu page_per_bucket_bits %zu\n",
			PMEM_N_BUCKET_BITS, PMEM_N_SPACE_BITS, PMEM_PAGE_PER_BUCKET_BITS);
#endif 

#if defined (UNIV_PMEMOBJ_BLOOM)
	PMEM_BLOOM_N_ELEMENTS = srv_pmem_bloom_n_elements;
	PMEM_BLOOM_FPR = srv_pmem_bloom_fpr;
#endif
	/////////////////////////////////////////////////
	// PART 1: NVM structures
	// ///////////////////////////////////////////////
	if (!pmw->pbuf) {
		//Case 1: Alocate new buffer in PMEM
			printf("PMEMOBJ_INFO: allocate %zd MB of buffer in pmem\n", buf_size);

		if ( pm_wrapper_buf_alloc(pmw, buf_size, page_size) == PMEM_ERROR ) {
			printf("PMEMOBJ_ERROR: error when allocate buffer in buf_dblwr_init()\n");
		}
	}
	else {
		//Case 2: Reused a buffer in PMEM
		printf("!!!!!!! [PMEMOBJ_INFO]: the server restart from a crash but the buffer is persist, in pmem: size = %zd free_pool has = %zd free lists\n", 
				pmw->pbuf->size, D_RW(pmw->pbuf->free_pool)->cur_lists);
		//Check the page_size of the previous run with current run
		if (pmw->pbuf->page_size != page_size) {
			printf("PMEM_ERROR: the pmem buffer size = %zu is different with UNIV_PAGE_SIZE = %zu, you must use the same page_size!!!\n ",
					pmw->pbuf->page_size, page_size);
			assert(0);
		}
			
		//We need to re-align the p_align
		byte* p;
		p = static_cast<byte*> (pmemobj_direct(pmw->pbuf->data));
		assert(p);
		pmw->pbuf->p_align = static_cast<byte*> (ut_align(p, page_size));
	}
	//New in PL-NVM, we need to save the const int the local variable of pmem_buf, so that the pmem0log.cc can access them
	pmw->pbuf->PMEM_N_BUCKETS = PMEM_N_BUCKETS;
	pmw->pbuf->PMEM_BUCKET_SIZE = PMEM_BUCKET_SIZE;
	pmw->pbuf->PMEM_BUF_FLUSH_PCT = PMEM_BUF_FLUSH_PCT;
	pmw->pbuf->PMEM_N_FLUSH_THREADS =  PMEM_N_FLUSH_THREADS;
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	pmw->pbuf->PMEM_N_BUCKET_BITS = PMEM_N_BUCKET_BITS;
	pmw->pbuf->PMEM_N_SPACE_BITS = PMEM_N_SPACE_BITS;
	pmw->pbuf->PMEM_PAGE_PER_BUCKET_BITS = PMEM_PAGE_PER_BUCKET_BITS;
#endif //UNIV_PMEMOBJ_BUF_PARTITION

#if defined (UNIV_PMEMOBJ_BLOOM)
	pmw->pbuf->PMEM_BLOOM_N_ELEMENTS = PMEM_BLOOM_N_ELEMENTS;
	pmw->pbuf->PMEM_BLOOM_FPR = PMEM_BLOOM_FPR;
#endif
	////////////////////////////////////////////////////
	// Part 2: D-RAM structures and open file(s)
	// ///////////////////////////////////////////////////
	
#if defined( UNIV_PMEMOBJ_BUF_STAT)
	pm_buf_bucket_stat_init(pmw->pbuf);
#endif	
#if defined(UNIV_PMEMOBJ_BUF_FLUSHER)
	//init threads for handle flushing, implement in buf0flu.cc
	pmw->pbuf->flusher = pm_flusher_init(PMEM_N_FLUSH_THREADS);
#endif 
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pm_filemap_init(pmw->pbuf);
#endif


	//In any case (new allocation or resued, we should allocate the flush_events for buckets in DRAM
	pmw->pbuf->flush_events = (os_event_t*) calloc(PMEM_N_BUCKETS, sizeof(os_event_t));

	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		sprintf(sbuf,"pm_flush_bucket%zu", i);
		pmw->pbuf->flush_events[i] = os_event_create(sbuf);
	}
		pmw->pbuf->free_pool_event = os_event_create("pm_free_pool_event");


	/*Alocate the param array
	 * Size of param array list should at least equal to PMEM_N_BUCKETS
	 * (i.e. one bucket has at least one param array)
	 * In case of one bucket receive heavily write such that the list
	 * is full while the previous list still not finish aio_batch
	 * In this situation, the params of the current list may overwrite the
	 * params of the previous list. We may encounter this situation with SINGLE_BUCKET partition
	 * where one space only map to one bucket, hence all writes are focus on one bucket
	 */
	PMEM_BUF_BLOCK_LIST* plist;
	ulint arr_size = 2 * PMEM_N_BUCKETS;

	pmw->pbuf->param_arr_size = arr_size;
	pmw->pbuf->param_arrs = static_cast<PMEM_AIO_PARAM_ARRAY*> (
		calloc(arr_size, sizeof(PMEM_AIO_PARAM_ARRAY)));	
	for ( i = 0; i < arr_size; i++) {
		//plist = D_RW(D_RW(pmw->pbuf->buckets)[i]);

		pmw->pbuf->param_arrs[i].params = static_cast<PMEM_AIO_PARAM*> (
		//pmw->pbuf->param_arrs[i] = static_cast<PMEM_AIO_PARAM*> (
				//calloc(plist->max_pages, sizeof(PMEM_AIO_PARAM)));
				calloc(PMEM_BUCKET_SIZE, sizeof(PMEM_AIO_PARAM)));
		pmw->pbuf->param_arrs[i].is_free = true;
	}
	pmw->pbuf->cur_free_param = 0; //start with the 0
	
	//Open file 
	pmw->pbuf->deb_file = fopen("pmem_debug.txt","a");
	
//	////test for recovery
//	printf("========== > Test for recovery\n");
//	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
//	PMEM_BUF_BLOCK_LIST* pcurlist;
//
//	printf("The bucket ====\n");
//	for (i = 0; i < PMEM_N_BUCKETS; i++) {
//		TOID_ASSIGN(cur_list, (D_RW(pmw->pbuf->buckets)[i]).oid);
//		plist = D_RW(cur_list);
//		printf("list %zu is_flush %d cur_pages %zu max_pages %zu\n",plist->list_id, plist->is_flush, plist->cur_pages, plist->max_pages );
//		
//		TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
//		printf("\t[next list \n");	
//		while( !TOID_IS_NULL(cur_list)) {
//			plist = D_RW(cur_list);
//			printf("\t\t next list %zu is_flush %d cur_pages %zu max_pages %zu\n",plist->list_id, plist->is_flush, plist->cur_pages, plist->max_pages );
//			TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
//		}
//		printf("\t end next list] \n");	
//
//		//print the linked-list 
//
//	}
//	printf("The free pool ====\n");
//	PMEM_BUF_FREE_POOL* pfree_pool = D_RW(pmw->pbuf->free_pool);	
//	printf("cur_lists = %zu max_lists=%zu\n",
//			pfree_pool->cur_lists, pfree_pool->max_lists);

}

/*
 * CLose/deallocate resource in DRAM
 * */
void pm_wrapper_buf_close(PMEM_WRAPPER* pmw) {
	uint64_t i;

#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	//pm_filemap_print(pmw->pbuf, pmw->pbuf->deb_file);
	pm_filemap_print(pmw->pbuf, debug_file);
	pm_filemap_close(pmw->pbuf);
#endif

	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		os_event_destroy(pmw->pbuf->flush_events[i]); 
	}
	os_event_destroy(pmw->pbuf->free_pool_event);
	free(pmw->pbuf->flush_events);

	//Free the param array
	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		//free(pmw->pbuf->params_arr[i]);
		free(pmw->pbuf->param_arrs[i].params);
	}
	//free(pmw->pbuf->params_arr);
	free(pmw->pbuf->param_arrs);
#if defined (UNIV_PMEMOBJ_BUF_FLUSHER)
	//Free the flusher
	pm_buf_flusher_close(pmw->pbuf->flusher);
#endif 

#if defined (UNIV_PMEMOBJ_BLOOM)
	//Because we want to keep the bloom filter in PM
	//we will not deallocate its resource
#endif 
	fclose(pmw->pbuf->deb_file);
}

int
pm_wrapper_buf_alloc(
		PMEM_WRAPPER*		pmw,
	    const size_t		size,
		const size_t		page_size)
{
	assert(pmw);
	pmw->pbuf = pm_pop_buf_alloc(pmw->pop, size, page_size);
	if (!pmw->pbuf)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;

}
/*
 * Allocate new log buffer in persistent memory and assign to the pointer in the wrapper
 * This function only allocate structures that in NVM
 * Structures in D-RAM are allocated outside in pm_wrapper_buf_alloc_or_open() function
 * */
PMEM_BUF* 
pm_pop_buf_alloc(
		PMEMobjpool*		pop,
		const size_t		size,
		const size_t		page_size)
{
	char* p;
	size_t align_size;

	TOID(PMEM_BUF) buf; 

	POBJ_ZNEW(pop, &buf, PMEM_BUF);

	PMEM_BUF *pbuf = D_RW(buf);
	//align sizes to a pow of 2
	assert(ut_is_2pow(page_size));
	align_size = ut_uint64_align_up(size, page_size);


	pbuf->size = align_size;
	pbuf->page_size = page_size;
	pbuf->type = BUF_TYPE;
	pbuf->is_new = true;

	pbuf->is_async_only = false;

	pbuf->data = pm_pop_alloc_bytes(pop, align_size);
	//align the pmem address for DIRECT_IO
	p = static_cast<char*> (pmemobj_direct(pbuf->data));
	assert(p);
	//pbuf->p_align = static_cast<char*> (ut_align(p, page_size));
	pbuf->p_align = static_cast<byte*> (ut_align(p, page_size));
	pmemobj_persist(pop, pbuf->p_align, sizeof(*pbuf->p_align));

	if (OID_IS_NULL(pbuf->data)){
		//assert(0);
		return NULL;
	}

	pm_buf_lists_init(pop, pbuf, align_size, page_size);
#if defined (UNIV_PMEMOBJ_BLOOM)
	//pbuf->bf = pm_bloom_alloc(BLOOM_SIZE, 0.01, NULL);
	pbuf->cbf = pm_cbf_alloc(
			PMEM_BLOOM_N_ELEMENTS,
		   	PMEM_BLOOM_FPR,
		   	NULL);
#endif

	pmemobj_persist(pop, pbuf, sizeof(*pbuf));
	return pbuf;
} 

/*
 * Init in-PMEM lists
 * bucket lists
 * free pool lists
 * and the special list
 * */
void 
pm_buf_lists_init(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf, 
		const size_t	total_size,
	   	const size_t	page_size)
{
	uint64_t i;
	uint64_t j;
	uint64_t cur_bucket;
	size_t offset;
	PMEM_BUF_FREE_POOL* pfreepool;
	PMEM_BUF_BLOCK_LIST* pfreelist;
	PMEM_BUF_BLOCK_LIST* plist;
	PMEM_BUF_BLOCK_LIST* pspeclist;
	page_size_t page_size_obj(page_size, page_size, false);


	size_t n_pages = (total_size / page_size);


	size_t bucket_size = PMEM_BUCKET_SIZE * page_size;
	size_t n_pages_per_bucket = PMEM_BUCKET_SIZE;

	//we need one more list for the special list
	size_t n_lists_in_free_pool = n_pages / PMEM_BUCKET_SIZE - PMEM_N_BUCKETS - 1;

	printf("\n\n=======> PMEM_INFO: n_pages = %zu bucket size = %f MB (%zu %zu-KB pages) n_lists in free_pool %zu\n", n_pages, bucket_size*1.0 / (1024*1024), n_pages_per_bucket, (page_size/1024), n_lists_in_free_pool);

	//Don't reset those variables during the init
	offset = 0;
	cur_bucket = 0;

	//init the temp args struct
	struct list_constr_args* args = (struct list_constr_args*) malloc(sizeof(struct list_constr_args)); 
	args->size.copy_from(page_size_obj);
	args->check = PMEM_AIO_CHECK;
	args->state = PMEM_FREE_BLOCK;
	TOID_ASSIGN(args->list, OID_NULL);

	//(1) Init the buckets
	//The sub-buf lists
	POBJ_ALLOC(pop, &buf->buckets, TOID(PMEM_BUF_BLOCK_LIST),
			sizeof(TOID(PMEM_BUF_BLOCK_LIST)) * PMEM_N_BUCKETS, NULL, NULL);
	if (TOID_IS_NULL(buf->buckets) ){
		fprintf(stderr, "POBJ_ALLOC\n");
	}

	for(i = 0; i < PMEM_N_BUCKETS; i++) {
		//init the bucket 
		POBJ_ZNEW(pop, &D_RW(buf->buckets)[i], PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(D_RW(buf->buckets)[i])) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		plist = D_RW(D_RW(buf->buckets)[i]);

		//pmemobj_rwlock_wrlock(pop, &plist->lock);

		plist->cur_pages = 0;
		plist->is_flush = false;
		plist->n_aio_pending = 0;
		plist->n_sio_pending = 0;
		plist->max_pages = bucket_size / page_size;
		plist->list_id = cur_bucket;
		plist->hashed_id = cur_bucket;
		//plist->flush_worker_id = PMEM_ID_NONE;
		cur_bucket++;
		plist->hashed_id = i;
		plist->check = PMEM_AIO_CHECK;
		TOID_ASSIGN(plist->next_list, OID_NULL);
		TOID_ASSIGN(plist->prev_list, OID_NULL);
		//plist->pext_list = NULL;
		//TOID_ASSIGN(plist->pext_list, OID_NULL);
	
		//init pages in bucket
		pm_buf_single_list_init(pop, D_RW(buf->buckets)[i], offset, args, n_pages_per_bucket, page_size);

		//POBJ_ALLOC(pop, &plist->arr, TOID(PMEM_BUF_BLOCK),
		//		sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		//for (j = 0; j < n_pages_per_bucket; j++) {
		//	args->pmemaddr = offset;
		//	TOID_ASSIGN(args->list, (D_RW(buf->buckets)[i]).oid);
		//	offset += page_size;	
		//	POBJ_NEW(pop, &D_RW(plist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		//}

		//pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		//pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		//pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		//pmemobj_persist(pop, &plist->next_list, sizeof(plist->next_list));
		//pmemobj_persist(pop, &plist->n_aio_pending, sizeof(plist->n_aio_pending));
		//pmemobj_persist(pop, &plist->n_sio_pending, sizeof(plist->n_sio_pending));
		//pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		//pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
		//pmemobj_persist(pop, &plist->hashed_id, sizeof(plist->hashed_id));
		//pmemobj_persist(pop, &plist->next_free_block, sizeof(plist->next_free_block));
		//pmemobj_persist(pop, plist, sizeof(*plist));

		//pmemobj_rwlock_unlock(pop, &plist->lock);

		// next bucket
	}

	//(2) Init the free pool
	POBJ_ZNEW(pop, &buf->free_pool, PMEM_BUF_FREE_POOL);	
	if(TOID_IS_NULL(buf->free_pool)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	pfreepool = D_RW(buf->free_pool);
	pfreepool->max_lists = n_lists_in_free_pool;
	pfreepool->cur_lists = 0;
	
	for(i = 0; i < n_lists_in_free_pool; i++) {
		TOID(PMEM_BUF_BLOCK_LIST) freelist;
		POBJ_ZNEW(pop, &freelist, PMEM_BUF_BLOCK_LIST);	
		if(TOID_IS_NULL(freelist)) {
			fprintf(stderr, "POBJ_ZNEW\n");
			assert(0);
		}
		pfreelist = D_RW(freelist);

		//pmemobj_rwlock_wrlock(pop, &pfreelist->lock);

		pfreelist->cur_pages = 0;
		pfreelist->max_pages = bucket_size / page_size;
		pfreelist->is_flush = false;
		pfreelist->n_aio_pending = 0;
		pfreelist->n_sio_pending = 0;
		pfreelist->list_id = cur_bucket;
		pfreelist->hashed_id = PMEM_ID_NONE;
		//pfreelist->flush_worker_id = PMEM_ID_NONE;
		cur_bucket++;
		pfreelist->check = PMEM_AIO_CHECK;
		TOID_ASSIGN(pfreelist->next_list, OID_NULL);
		TOID_ASSIGN(pfreelist->prev_list, OID_NULL);
	
		//init pages in bucket
		pm_buf_single_list_init(pop, freelist, offset, args, n_pages_per_bucket, page_size);

		//POBJ_ALLOC(pop, &pfreelist->arr, TOID(PMEM_BUF_BLOCK),
		//		sizeof(TOID(PMEM_BUF_BLOCK)) * n_pages_per_bucket, NULL, NULL);

		//for (j = 0; j < n_pages_per_bucket; j++) {
		//	args->pmemaddr = offset;
		//	offset += page_size;	
		//	TOID_ASSIGN(args->list, freelist.oid);

		//	POBJ_NEW(pop, &D_RW(pfreelist->arr)[j], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		//}

		//pmemobj_persist(pop, &pfreelist->cur_pages, sizeof(pfreelist->cur_pages));
		//pmemobj_persist(pop, &pfreelist->max_pages, sizeof(pfreelist->max_pages));
		//pmemobj_persist(pop, &pfreelist->is_flush, sizeof(pfreelist->is_flush));
		//pmemobj_persist(pop, &pfreelist->next_list, sizeof(pfreelist->next_list));
		//pmemobj_persist(pop, &pfreelist->prev_list, sizeof(pfreelist->prev_list));
		//pmemobj_persist(pop, &pfreelist->n_aio_pending, sizeof(pfreelist->n_aio_pending));
		//pmemobj_persist(pop, &pfreelist->n_sio_pending, sizeof(pfreelist->n_sio_pending));
		//pmemobj_persist(pop, &pfreelist->check, sizeof(pfreelist->check));
		//pmemobj_persist(pop, &pfreelist->list_id, sizeof(pfreelist->list_id));
		//pmemobj_persist(pop, &pfreelist->next_free_block, sizeof(pfreelist->next_free_block));
		//pmemobj_persist(pop, pfreelist, sizeof(*plist));

		//pmemobj_rwlock_unlock(pop, &pfreelist->lock);

		//insert this list in the freepool
		POBJ_LIST_INSERT_HEAD(pop, &pfreepool->head, freelist, list_entries);
		pfreepool->cur_lists++;
		//loop: init next buckes
	} //end init the freepool
	pmemobj_persist(pop, &buf->free_pool, sizeof(buf->free_pool));

	// (3) Init the special list used in recovery
	POBJ_ZNEW(pop, &buf->spec_list, PMEM_BUF_BLOCK_LIST);	
	if(TOID_IS_NULL(buf->spec_list)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	pspeclist = D_RW(buf->spec_list);
	pspeclist->cur_pages = 0;
	pspeclist->max_pages = bucket_size / page_size;
	pspeclist->is_flush = false;
	pspeclist->n_aio_pending = 0;
	pspeclist->n_sio_pending = 0;
	pspeclist->list_id = cur_bucket;
	pspeclist->hashed_id = PMEM_ID_NONE;
	//pfreelist->flush_worker_id = PMEM_ID_NONE;
	cur_bucket++;
	pspeclist->check = PMEM_AIO_CHECK;
	TOID_ASSIGN(pspeclist->next_list, OID_NULL);
	TOID_ASSIGN(pspeclist->prev_list, OID_NULL);
	//init pages in spec list 
	pm_buf_single_list_init(pop, buf->spec_list, offset, args, n_pages_per_bucket, page_size);
	
}


/*
 * Allocate and init blocks in a PMEM_BUF_BLOCK_LIST
 * THis function is called in pm_buf_lists_init()
 * pop [in]: pmemobject pool
 * plist [in/out]: pointer to the list
 * offset [in/out]: current offset, this offset will increase during the function run
 * n [in]: number of pages 
 * args [in]: temp struct to hold info
 * */
void 
pm_buf_single_list_init(
		PMEMobjpool*				pop,
		TOID(PMEM_BUF_BLOCK_LIST)	inlist,
		size_t&						offset,
		struct list_constr_args*	args,
		const size_t				n,
		const size_t				page_size){
		
		ulint i;

		PMEM_BUF_BLOCK_LIST*	plist;
		plist = D_RW(inlist);

		POBJ_ALLOC(pop, &plist->arr, TOID(PMEM_BUF_BLOCK),
				sizeof(TOID(PMEM_BUF_BLOCK)) * n, NULL, NULL);

		for (i = 0; i < n; i++) {
			args->pmemaddr = offset;
			offset += page_size;	

			TOID_ASSIGN(args->list, inlist.oid);
			POBJ_NEW(pop, &D_RW(plist->arr)[i], PMEM_BUF_BLOCK, pm_buf_block_init, args);
		}
		// Make properties in the list persist
		pmemobj_persist(pop, &plist->cur_pages, sizeof(plist->cur_pages));
		pmemobj_persist(pop, &plist->max_pages, sizeof(plist->max_pages));
		pmemobj_persist(pop, &plist->is_flush, sizeof(plist->is_flush));
		pmemobj_persist(pop, &plist->next_list, sizeof(plist->next_list));
		pmemobj_persist(pop, &plist->n_aio_pending, sizeof(plist->n_aio_pending));
		pmemobj_persist(pop, &plist->n_sio_pending, sizeof(plist->n_sio_pending));
		pmemobj_persist(pop, &plist->check, sizeof(plist->check));
		pmemobj_persist(pop, &plist->list_id, sizeof(plist->list_id));
		pmemobj_persist(pop, &plist->hashed_id, sizeof(plist->hashed_id));
		pmemobj_persist(pop, &plist->next_free_block, sizeof(plist->next_free_block));

		pmemobj_persist(pop, plist, sizeof(*plist));
}

/*This function is called as the func pointer in POBJ_LIST_INSERT_NEW_HEAD()*/
int
pm_buf_block_init(
		PMEMobjpool*	pop,
	   	void*			ptr,
	   	void*			arg)
{
	struct list_constr_args *args = (struct list_constr_args *) arg;
	PMEM_BUF_BLOCK* block = (PMEM_BUF_BLOCK*) ptr;
//	block->id = args->id;
	block->id.copy_from(args->id);
//	block->size = args->size;
	block->size.copy_from(args->size);
	block->check = args->check;
	block->state = args->state;
	block->sync = false;
	//block->bpage = args->bpage;
	TOID_ASSIGN(block->list, (args->list).oid);
	block->pmemaddr = args->pmemaddr;


	pmemobj_persist(pop, &block->id, sizeof(block->id));
	pmemobj_persist(pop, &block->size, sizeof(block->size));
	pmemobj_persist(pop, &block->check, sizeof(block->check));
	//pmemobj_persist(pop, &block->bpage, sizeof(block->bpage));
	pmemobj_persist(pop, &block->state, sizeof(block->state));
	pmemobj_persist(pop, &block->list, sizeof(block->list));
	pmemobj_persist(pop, &block->pmemaddr, sizeof(block->pmemaddr));
	return 0;
}

PMEM_BUF* pm_pop_get_buf(PMEMobjpool* pop) {
	TOID(PMEM_BUF) buf;
	//get the first object in pmem has type PMEM_BUF
	buf = POBJ_FIRST(pop, PMEM_BUF);

	if (TOID_IS_NULL(buf)) {
		return NULL;
	}			
	else {
		PMEM_BUF *pbuf = D_RW(buf);
		if(!pbuf) {
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
			return NULL;
		}
		return pbuf;
	}
}

/*
 * *Write a page to pmem buffer using "instance swap"
 * This function is called by innodb cleaner thread
 * When a list is full, the cleaner thread does:
 * (1) find the free list from free_pool and swap with current full list
 * (2) add current full list to waiting-list of the flusher and notify the flusher about new added list then return (to reduce latency)
 * (3) The flusher looking for a idle worker to handle full list
@param[in] pop		the PMEMobjpool
@param[in] buf		the pointer to PMEM_BUF_
@param[in] src_data	data contains the bytes to write
@param[in] page_size
 * */
int
pm_buf_write_with_flusher(
			PMEMobjpool*	pop,
		   	PMEM_WRAPPER*	pmw	,
		   	page_id_t		page_id,
		   	page_size_t		size,
			uint64_t		pageLSN,
		   	byte*			src_data,
		   	bool			sync) 
{
	
#if defined (UNIV_PMEM_SIM_LATENCY)
	uint64_t start_cycle, end_cycle;
#endif
	PMEM_BUF* buf = pmw->pbuf;
	//bool is_lock_free_block = false;
	//bool is_lock_free_list = false;
	bool is_safe_check = false;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	//Does some checks 
	if (buf->is_async_only)
		assert(!sync);

	assert(buf);
	assert(src_data);


#if defined (UNIV_PMEMOBJ_BLOOM)
//add the fold() value to the bloom filter
pm_cbf_add(buf->cbf, page_id.fold());
#endif

#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
	assert (pm_check_io(src_data, page_id) );
#endif 
	page_size = size.physical();
	//UNIV_MEM_ASSERT_RW(src_data, page_size);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY)
//page 0 is put in the special list
	if (page_id.page_no() == 0) {
		PMEM_BUF_BLOCK_LIST* pspec_list;
		PMEM_BUF_BLOCK*		pspec_block;
		fil_node_t*			node;

		node = pm_get_node_from_space(page_id.space());
		if (node == NULL) {
			printf("PMEM_ERROR node from space is NULL\n");
			assert(0);
		}

		pspec_list = D_RW(buf->spec_list); 

		pmemobj_rwlock_wrlock(pop, &pspec_list->lock);

		pdata = buf->p_align;
		//scan in the special list
		for (i = 0; i < pspec_list->cur_pages; i++){
			pspec_block = D_RW(D_RW(pspec_list->arr)[i]);

			if (pspec_block->state == PMEM_FREE_BLOCK){
				break;
			}	
			else if (pspec_block->state == PMEM_IN_USED_BLOCK) {
				if (pspec_block->id.equals_to(page_id) ||
						strstr(pspec_block->file_name, node->name) != 0) {
					//overwrite this spec block
					pspec_block->sync = sync;
#if defined (UNIV_PMEMOBJ_PERSIST)
					pmemobj_persist(pop, &pspec_block->sync, sizeof(pspec_block->sync));
#endif
#if defined (UNIV_PMEMOBJ_NO_TX)
					//memcpy(pdata + pspec_block->pmemaddr, src_data, page_size); 
					pmemobj_memcpy_persist(pop, pdata + pspec_block->pmemaddr, src_data, page_size); 
#else
TX_BEGIN(pop) {
					//pmemobj_memcpy_persist(pop, pdata + pspec_block->pmemaddr, src_data, page_size); 
					TX_MEMCPY(pdata + pspec_block->pmemaddr, src_data, page_size); 
}TX_ONABORT {
}TX_END
#endif
					//update the file_name, page_id in case of tmp space
					strcpy(pspec_block->file_name, node->name);
					pspec_block->id.copy_from(page_id);
#if defined (UNIV_PMEMOBJ_PERSIST)
					pmemobj_persist(pop, &pspec_block->id, sizeof(pspec_block->id));
#endif
					pmemobj_rwlock_unlock(pop, &pspec_list->lock);

					return PMEM_SUCCESS;
				}
				//else: just skip this block
			}
			//next block
		}//end for
		
		if (i < pspec_list->cur_pages) {
			printf("PMEM_BUF Logical error when handle the special list\n");
			assert(0);
		}
		if (i == pspec_list->cur_pages) {
			pspec_block = D_RW(D_RW(pspec_list->arr)[i]);
			//add new block to the spec list
			pspec_block->sync = sync;
			pspec_block->id.copy_from(page_id);
			pspec_block->state = PMEM_IN_USED_BLOCK;
			//get file handle
			//[Note] is it safe without acquire fil_system->mutex?
			node = pm_get_node_from_space(page_id.space());
			if (node == NULL) {
				printf("PMEM_ERROR node from space is NULL\n");
				assert(0);
			}
			strcpy(pspec_block->file_name, node->name);

#if defined (UNIV_PMEMOBJ_PERSIST)
			pmemobj_persist(pop, &pspec_block->sync, sizeof(pspec_block->sync));
			pmemobj_persist(pop, &pspec_block->id, sizeof(pspec_block->id));
			pmemobj_persist(pop, &pspec_block->state, sizeof(pspec_block->state));
			pmemobj_persist(pop, &pspec_block->file_name, sizeof(pspec_block->file_name));
#endif

#if defined (UNIV_PMEM_SIM_LATENCY)
			/*5 times write to NVM*/
			PMEM_DELAY(start_cycle, end_cycle, 5 * pmw->PMEM_SIM_CPU_CYCLES);
			memcpy(pdata + pspec_block->pmemaddr, src_data, page_size); 
#else
#if defined (UNIV_PMEMOBJ_NO_TX)
			pmemobj_memcpy_persist(pop, pdata + pspec_block->pmemaddr, src_data, page_size); 
			//memcpy(pdata + pspec_block->pmemaddr, src_data, page_size); 
#else
TX_BEGIN(pop) {
			TX_MEMCPY(pdata + pspec_block->pmemaddr, src_data, page_size); 
}TX_ONABORT {
}TX_END
#endif //UNIV_PMEMOBJ_NO_TX

#endif //UNIV_PMEM_SIM_LATENCY
			++(pspec_list->cur_pages);

			printf("Add new block to the spec list, space_no %zu,file %s cur_pages %zu \n", page_id.space(),node->name,  pspec_list->cur_pages);

			//We do not handle flushing the spec list here
			if (pspec_list->cur_pages >= pspec_list->max_pages * PMEM_BUF_FLUSH_PCT) {
				printf("We do not handle flushing spec list in this version, adjust the input params to get larger size of spec list\n");
				assert(0);
			}
		}
		pmemobj_rwlock_unlock(pop, &pspec_list->lock);
		return PMEM_SUCCESS;
	} // end if page_no == 0
#endif //UNIV_PMEMOBJ_BUF_RECOVERY


#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(buf, hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), buf->PMEM_N_BUCKETS);
#endif

retry:
	//the safe check
	if (is_safe_check){
		if (D_RO(D_RO(buf->buckets)[hashed])->is_flush) {
			os_event_wait(buf->flush_events[hashed]);
			goto retry;
		}
	}
	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);

	pmemobj_rwlock_wrlock(pop, &phashlist->lock);

	//double check
	if (phashlist->is_flush) {
		//When I was blocked (due to mutex) this list is non-flush. When I acquire the lock, it becomes flushing

/* NOTE for recovery 
 *
 * Don't call pm_buf_handle_full_hashed_list before  innodb apply log records that re load spaces (MLOG_FILE_NAME)
 * Hence, we call pm_buf_handle_full_hashed_list only inside pm_buf_write and after the 
 * recv_recovery_from_checkpoint_finish() (through pm_buf_resume_flushing)
 * */
		if (buf->is_recovery	&&
			(phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT)) {
			pmemobj_rwlock_unlock(pop, &phashlist->lock);
			//printf("PMEM_INFO: call pm_buf_handle_full_hashed_list during recovering, hashed = %zu list_id = %zu, cur_pages= %zu, is_flush = %d...\n", hashed, phashlist->list_id, phashlist->cur_pages, phashlist->is_flush);
			pm_buf_handle_full_hashed_list(pop, pmw, hashed);
			goto retry;
		}
		
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);

		goto retry;
	}

	pdata = buf->p_align;
	//(1) search in the hashed list for a first FREE block to write on 
	for (i = 0; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);

		if (pfree_block->state == PMEM_FREE_BLOCK) {
			//found!
			//if(is_lock_free_block)
			//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			break;	
		}
		else if(pfree_block->state == PMEM_IN_USED_BLOCK) 		{
			if (pfree_block->id.equals_to(page_id)) {
				//overwrite the old page
				//if(is_lock_free_block)
				//pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
				if (pfree_block->sync != sync) {
					if (sync == false) {
						++phashlist->n_aio_pending;
						--phashlist->n_sio_pending;
					}
					else {
						--phashlist->n_aio_pending;
						++phashlist->n_sio_pending;
					}
				}
				pfree_block->sync = sync;
#if defined (UNIV_PMEMOBJ_PERSIST)
				pmemobj_persist(pop, &pfree_block->sync, sizeof(pfree_block->sync));
#endif

#if defined (UNIV_PMEM_SIM_LATENCY)
			/*3 times write to NVM*/
			PMEM_DELAY(start_cycle, end_cycle, 3 * pmw->PMEM_SIM_CPU_CYCLES);
			memcpy(pdata + pfree_block->pmemaddr, src_data, page_size); 
#else //PMEM_BUF
#if defined (UNIV_PMEMOBJ_NO_TX)
				pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
				//memcpy(pdata + pfree_block->pmemaddr, src_data, page_size); 
#else
TX_BEGIN(pop) {
				TX_MEMCPY(pdata + pfree_block->pmemaddr, src_data, page_size); 
}TX_ONABORT {
}TX_END
#endif //UNIV_PMEMOBJ_NO_TX

#endif //UNIV_PMEM_SIM_LATENCY

#if defined (UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_overwrites;
#endif
				pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
				printf("========   PMEM_DEBUG: in pmem_buf_write, OVERWRITTEN page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	
				return PMEM_SUCCESS;
			}
		}
		//next block
	}

	if ( i == phashlist->max_pages ) {
		//ALl blocks in this hash_list are either non-fre or locked
		//This is rarely happen but we still deal with it
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);
		//os_thread_sleep(PMEM_WAIT_FOR_WRITE);
	printf("\n\n    PMEM_DEBUG: in pmem_buf_write,  hash_list id %zu no free or non-block pages retry \n ", phashlist->list_id);
	//test
	exit(0);
		goto retry;
	}
#if defined(UNIV_PMEMOBJ_BUF_DEBUG)
	printf("========   PMEM_DEBUG: in pmem_buf_write, the write page_id %zu space %zu size %zu hash_list id %zu \n ", page_id.page_no(), page_id.space(), size.physical(), phashlist->list_id);
#endif	

	// (2) At this point, we get the free and un-blocked block, write data to this block
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pmemobj_rwlock_wrlock(pop, &buf->filemap->lock);
	pm_filemap_update_items(buf, page_id, hashed, PMEM_BUCKET_SIZE);
	pmemobj_rwlock_unlock(pop, &buf->filemap->lock);
#endif 
	//This code is test for recovery, it has lock/unlock mutex. Note that we need the filename in pmem block because at recovery time, the space instance related with pmem_block may not load in the system. In that case, filename is used for ibd_load() func
	//If the performance reduce, then remove it
	fil_node_t*			node;

	node = pm_get_node_from_space(page_id.space());
	if (node == NULL) {
		printf("PMEM_ERROR node from space is NULL\n");
		assert(0);
	}
	strcpy(pfree_block->file_name, node->name);
	//end code
	
	pfree_block->sync = sync;
	pfree_block->id.copy_from(page_id);
	
	assert(pfree_block->size.equals_to(size));
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_IN_USED_BLOCK;

#if defined (UNIV_PMEMOBJ_PERSIST)
	pmemobj_persist(pop, &pfree_block->sync, sizeof(pfree_block->sync));
	pmemobj_persist(pop, &pfree_block->id, sizeof(pfree_block->id));
	pmemobj_persist(pop, &pfree_block->state, sizeof(pfree_block->state));
#endif

#if defined (UNIV_PMEM_SIM_LATENCY)
		memcpy(pdata + pfree_block->pmemaddr, src_data, page_size); 
#else
#if defined (UNIV_PMEMOBJ_NO_TX)
		pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
		//memcpy(pdata + pfree_block->pmemaddr, src_data, page_size); 
#else
	TX_BEGIN(pop) {
		TX_MEMCPY(pdata + pfree_block->pmemaddr, src_data, page_size); 
	}TX_ONABORT {
	}TX_END
#endif //UNIV_PMEMOBJ_NO_TX
#endif //UNIV_PMEM_SIM_LATENCY

	//if(is_lock_free_block)
	//pmemobj_rwlock_unlock(pop, &pfree_block->lock);

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);
#if defined (UNIV_PMEMOBJ_PERSIST)
	pmemobj_persist(pop, &phashlist->cur_pages, sizeof(phashlist->cur_pages));
#endif	
	//phashlist->n_aio_pending = phashlist->cur_pages;
	//we only pending aio when flush list
	if (sync == false)
		++(phashlist->n_aio_pending);		
	else
		++(phashlist->n_sio_pending);		

#if defined (UNIV_PMEM_SIM_LATENCY)
	/*7 times write to NVM*/
	PMEM_DELAY(start_cycle, end_cycle, 7 * pmw->PMEM_SIM_CPU_CYCLES);
#endif
// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT) {
		//(3) The hashlist is (nearly) full, flush it and assign a free list 
		phashlist->hashed_id = hashed;
		phashlist->is_flush = true;

#if defined (UNIV_PMEMOBJ_PERSIST)
		pmemobj_persist(pop, &phashlist->hashed_id, sizeof(phashlist->hashed_id));
		pmemobj_persist(pop, &phashlist->is_flush, sizeof(phashlist->is_flush));
#endif
		//block upcomming writes into this bucket
		os_event_reset(buf->flush_events[hashed]);
		
#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_flushed_lists;
#endif 

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n[1] BEGIN pm_buf_handle_full list_id %zu, hashed_id %zu\n", phashlist->list_id, hashed);
#endif 
		pm_buf_handle_full_hashed_list(pop, pmw, hashed);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n[1] END pm_buf_handle_full list_id %zu, hashed_id %zu\n", phashlist->list_id, hashed);
#endif 

		//unblock the upcomming writes on this bucket
		os_event_set(buf->flush_events[hashed]);
	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
	}

	return PMEM_SUCCESS;
}

/*
 * VERSION 4
 * Similar with pm_buf_write_with_flusher but write in append mode
 * We handle remove old versions in flusher thread
 * Read to PMEM_BUF need to scan from the tail to the head in order to get
 * the lastest version
 * */
int
pm_buf_write_with_flusher_append(
			PMEMobjpool*	pop,
		   	PMEM_BUF*		buf,
		   	page_id_t		page_id,
		   	page_size_t		size,
		   	byte*			src_data,
		   	bool			sync) 
{

	//bool is_lock_free_block = false;
	//bool is_lock_free_list = false;
	bool is_safe_check = false;

	ulint hashed;
	ulint i;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	//Does some checks 
	if (buf->is_async_only)
		assert(!sync);

	assert(buf);
	assert(src_data);
#if defined (UNIV_PMEMOBJ_BUF_DEBUG)
	assert (pm_check_io(src_data, page_id) );
#endif 
	page_size = size.physical();
	//UNIV_MEM_ASSERT_RW(src_data, page_size);

	//PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(buf, hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), buf->PMEM_N_BUCKETS);
#endif
//	hashed = hash_f1(page_id.space(),
//			page_id.page_no(), PMEM_N_BUCKETS, PMEM_PAGE_PER_BUCKET_BITS);

retry:
	//the safe check
	if (is_safe_check){
		if (D_RO(D_RO(buf->buckets)[hashed])->is_flush) {
			os_event_wait(buf->flush_events[hashed]);
			goto retry;
		}
	}
	
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);
	assert(phashlist);

	pmemobj_rwlock_wrlock(pop, &phashlist->lock);

	//double check
	if (phashlist->is_flush) {
		//When I was blocked (due to mutex) this list is non-flush. When I acquire the lock, it becomes flushing

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);

		goto retry;
	}
	//Now the list is non-flush and I have acquired the lock. Let's do my work
	pdata = buf->p_align;
	//(1) append only 
	for (i = phashlist->cur_pages; i < phashlist->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);	
		if (pfree_block->state == PMEM_FREE_BLOCK) {
			break;	
		}
		else{
			printf("PMEM_ERROR: error in append mode\n");
			assert(0);
		}
	}
	if ( i == phashlist->max_pages ) {
		//ALl blocks in this hash_list are either non-fre or locked
		//This is rarely happen but we still deal with it
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);
		//os_thread_sleep(PMEM_WAIT_FOR_WRITE);

	printf("\n\n    PMEM_DEBUG: in pmem_buf_write,  hash_list id %zu no free or non-block pages retry \n ", phashlist->list_id);
		goto retry;
	}


	// (2) At this point, we get the free and un-blocked block, write data to this block
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
	pmemobj_rwlock_wrlock(pop, &buf->filemap->lock);
	pm_filemap_update_items(buf, page_id, hashed, PMEM_BUCKET_SIZE);
	pmemobj_rwlock_unlock(pop, &buf->filemap->lock);
#endif 
	//D_RW(free_block)->bpage = bpage;
	pfree_block->sync = sync;

	pfree_block->id.copy_from(page_id);
	
	assert(pfree_block->size.equals_to(size));
	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_IN_USED_BLOCK;

	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 

	//if(is_lock_free_block)
	//pmemobj_rwlock_unlock(pop, &pfree_block->lock);

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_writes;
#endif 
	//handle hash_list, 

	++(phashlist->cur_pages);
	
	//phashlist->n_aio_pending = phashlist->cur_pages;
	//we only pending aio when flush list
	if (sync == false)
		++(phashlist->n_aio_pending);		
	else
		++(phashlist->n_sio_pending);		

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT) {
		//(3) The hashlist is (nearly) full, flush it and assign a free list 
		phashlist->hashed_id = hashed;
		phashlist->is_flush = true;
		//block upcomming writes into this bucket
		os_event_reset(buf->flush_events[hashed]);
		
		pmemobj_rwlock_unlock(pop, &phashlist->lock);

		PMEM_FLUSHER* flusher = buf->flusher;
assign_worker:
		mutex_enter(&flusher->mutex);
		//pm_buf_flush_list(pop, buf, phashlist);
		if (flusher->n_requested == flusher->size) {
			//all requested slot is full)
			printf("PMEM_INFO: all reqs are booked, sleep and wait \n");
			mutex_exit(&flusher->mutex);
			os_event_wait(flusher->is_req_full);	
			goto assign_worker;	
		}
			
		//find an idle thread to assign flushing task
		ulint n_try = flusher->size;
		while (n_try > 0) {
			if (flusher->flush_list_arr[flusher->tail] == NULL) {
				//found
				//phashlist->flush_worker_id = PMEM_ID_NONE;
				flusher->flush_list_arr[flusher->tail] = phashlist;
				//printf("before request hashed_id = %d list_id = %zu\n", phashlist->hashed_id, phashlist->list_id);
				++flusher->n_requested;
				//delay calling flush up to a threshold
				//printf("trigger worker...\n");
				if (flusher->n_requested == flusher->size - 2) {
				os_event_set(flusher->is_req_not_empty);
				}

				if (flusher->n_requested >= flusher->size) {
					os_event_reset(flusher->is_req_full);
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
			printf("PMEM_ERROR requested/size = %zu /%zu / %zu\n", flusher->n_requested, flusher->size);
			//mutex_exit(&flusher->mutex);
			//os_event_wait(flusher->is_flush_full);
			//goto assign_worker;
			assert (n_try);
		}

		mutex_exit(&flusher->mutex);
//end FLUSHER handling

		//this assert inform that all write is async
		if (buf->is_async_only){ 
			if (phashlist->n_aio_pending != phashlist->cur_pages) {
				printf("!!!! ====> PMEM_ERROR: n_aio_pending=%zu != cur_pages = %zu. They should be equal\n", phashlist->n_aio_pending, phashlist->cur_pages);
				assert (phashlist->n_aio_pending == phashlist->cur_pages);
			}
		}
#if defined (UNIV_PMEMOBJ_BUF_STAT)
		++buf->bucket_stats[hashed].n_flushed_lists;
#endif

get_free_list:
		//Get a free list from the free pool
		pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

		TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
		if (D_RW(buf->free_pool)->cur_lists == 0 ||
				TOID_IS_NULL(first_list) ) {
			pthread_t tid;
		 	tid = pthread_self();
			printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
			pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
			//pmemobj_rwlock_unlock(pop, &phashlist->lock);
			//os_thread_sleep(PMEM_WAIT_FOR_WRITE);
			//os_thread_sleep(PMEM_WAIT_FOR_FREE_LIST);
			os_event_wait(buf->free_pool_event);

			goto get_free_list;
		}

		POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
		D_RW(buf->free_pool)->cur_lists--;
		//The free_pool may empty now, wait in necessary
		os_event_reset(buf->free_pool_event);
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));

		assert(!TOID_IS_NULL(first_list));
		//This ref hashed_id used for batch AIO
		//Hashed id of old list is kept until its batch AIO is completed
		D_RW(first_list)->hashed_id = hashed;
		
		//if(is_lock_free_list)
		//pmemobj_rwlock_wrlock(pop, &D_RW(first_list)->lock);

		//Asign linked-list refs 
		TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
		TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
		TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);
		

#if defined (UNIV_PMEMOBJ_BUF_STAT)
		if ( !TOID_IS_NULL( D_RW(hash_list)->next_list ))
		++buf->bucket_stats[hashed].max_linked_lists;
#endif 
		//unblock the upcomming writes on this bucket
		os_event_set(buf->flush_events[hashed]);

		//if(is_lock_free_list)
		//pmemobj_rwlock_unlock(pop, &D_RW(first_list)->lock);
		//pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//[flush position 2 ]
		//pm_buf_flush_list(pop, buf, phashlist);
		//pmemobj_rwlock_unlock(pop, &phashlist->lock);

		//pmemobj_rwlock_unlock(pop, &phashlist->lock);

	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
	}

	return PMEM_SUCCESS;
}

/////////////////////////// pm_buf_flush_list versions///
/*  VERSION 0 (BATCH)
 * Async write pages from the list to datafile
 * The caller thread need to lock/unlock the plist
 * See buf_dblwr_write_block_to_datafile
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *	@param[in] plist	pointer to the list that will be flushed 
 * */
void
pm_buf_flush_list(
		PMEMobjpool*			pop,
	   	PMEM_BUF*				buf,
	   	PMEM_BUF_BLOCK_LIST*	plist) {

	assert(pop);
	assert(buf);

#if defined (UNIV_PMEMOBJ_BUF)
		ulint type = IORequest::PM_WRITE;
#else
		ulint type = IORequest::WRITE;
#endif
		IORequest request(type);

		dberr_t err = pm_fil_io_batch(request, pop, buf, plist);
		
}


/*
 *This function is called from aio complete (fil_aio_wait)
 (1) Reset the list
 (2) Flush spaces in this list
 * */
void
pm_handle_finished_block(
		PMEMobjpool*		pop,
		PMEM_WRAPPER*		pmw	,
	   	PMEM_BUF*			buf,
	   	PMEM_BUF_BLOCK*		pblock)
{

#if defined (UNIV_PMEM_SIM_LATENCY)
	uint64_t start_cycle, end_cycle;
#endif
	//bool is_lock_prev_list = false;

	//(1) handle the flush_list
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	assert(pflush_list);
	
	//possible owners: producer (pm_buf_write), consummer (this function) threads
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);
	
	if(pblock->sync)
		pflush_list->n_sio_pending--;
	else
		pflush_list->n_aio_pending--;

	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {
		//Now all pages in this list are persistent in disk
		//(0) flush spaces
		pm_buf_flush_spaces_in_list(pop, buf, pflush_list);

		//(1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
#if defined (UNIV_PMEM_SIM_LATENCY)
			/*2 times write to NVM*/
			PMEM_DELAY(start_cycle, end_cycle, 2 * pmw->PMEM_SIM_CPU_CYCLES);
#endif
		}

		pflush_list->cur_pages = 0;
		pflush_list->is_flush = false;
		pflush_list->hashed_id = PMEM_ID_NONE;
		
		// (2) Remove this list from the doubled-linked list
		
		//assert( !TOID_IS_NULL(pflush_list->prev_list) );
		if( !TOID_IS_NULL(pflush_list->prev_list) ) {

			//if (is_lock_prev_list)
			//pmemobj_rwlock_wrlock(pop, &D_RW(pflush_list->prev_list)->lock);
			TOID_ASSIGN( D_RW(pflush_list->prev_list)->next_list, pflush_list->next_list.oid);
			//if (is_lock_prev_list)
			//pmemobj_rwlock_unlock(pop, &D_RW(pflush_list->prev_list)->lock);
		}

		if (!TOID_IS_NULL(pflush_list->next_list) ) {
			//pmemobj_rwlock_wrlock(pop, &D_RW(pflush_list->next_list)->lock);

			TOID_ASSIGN(D_RW(pflush_list->next_list)->prev_list, pflush_list->prev_list.oid);

			//pmemobj_rwlock_unlock(pop, &D_RW(pflush_list->next_list)->lock);
		}
		
		TOID_ASSIGN(pflush_list->next_list, OID_NULL);
		TOID_ASSIGN(pflush_list->prev_list, OID_NULL);

		// (3) we return this list to the free_pool
		PMEM_BUF_FREE_POOL* pfree_pool;
		pfree_pool = D_RW(buf->free_pool);

#if defined (UNIV_PMEM_SIM_LATENCY)
		/*7 times write to NVM*/
		PMEM_DELAY(start_cycle, end_cycle, 7 * pmw->PMEM_SIM_CPU_CYCLES);
#endif

		//printf("PMEM_DEBUG: in fil_aio_wait(), try to lock free_pool list id: %zd, cur_lists in free_pool= %zd \n", pflush_list->list_id, pfree_pool->cur_lists);
		pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

		POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, flush_list, list_entries);
		pfree_pool->cur_lists++;
#if defined (UNIV_PMEM_SIM_LATENCY)
		/*2 times write to NVM*/
		PMEM_DELAY(start_cycle, end_cycle, 2 * pmw->PMEM_SIM_CPU_CYCLES);
#endif
		//wakeup who is waitting for free_pool available
		os_event_set(buf->free_pool_event);

		pmemobj_rwlock_unlock(pop, &pfree_pool->lock);

	}
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}

/*
 *This function is called from aio complete (fil_aio_wait)
 (1) Reset the list
 (2) Flush spaces in this list
 * */
void
pm_handle_finished_block_no_free_pool(
		PMEMobjpool*		pop,
	   	PMEM_BUF*			buf,
	   	PMEM_BUF_BLOCK*		pblock)
{

	//bool is_lock_prev_list = false;

	//(1) handle the flush_list
	TOID(PMEM_BUF_BLOCK_LIST) flush_list;

	TOID_ASSIGN(flush_list, pblock->list.oid);
	PMEM_BUF_BLOCK_LIST* pflush_list = D_RW(flush_list);

	assert(pflush_list);
	
	//possible owners: producer (pm_buf_write), consummer (this function) threads
	pmemobj_rwlock_wrlock(pop, &pflush_list->lock);
	
	if(pblock->sync)
		pflush_list->n_sio_pending--;
	else
		pflush_list->n_aio_pending--;

	if (pflush_list->n_aio_pending + pflush_list->n_sio_pending == 0) {
		//Now all pages in this list are persistent in disk
		//(0) flush spaces
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("finish list %zu hash_id %zu \n",
				pflush_list->list_id, pflush_list->hashed_id);
#endif
		pm_buf_flush_spaces_in_list(pop, buf, pflush_list);

		//(1) Reset blocks in the list
		ulint i;
		for (i = 0; i < pflush_list->max_pages; i++) {
			D_RW(D_RW(pflush_list->arr)[i])->state = PMEM_FREE_BLOCK;
			D_RW(D_RW(pflush_list->arr)[i])->sync = false;
		}

		pflush_list->cur_pages = 0;
		pflush_list->is_flush = false;

		os_event_set(buf->flush_events[pflush_list->hashed_id]);

		//pflush_list->hashed_id = PMEM_ID_NONE;

	}
	//the list has some unfinished aio	
	pmemobj_rwlock_unlock(pop, &pflush_list->lock);
}
/*
 *	Read a page from pmem buffer using the page_id as key
 *	@param[in] pop		PMEMobjpool pointer
 *	@param[in] buf		PMEM_BUF pointer
 *  @param[in] page_id	read key
 *  @param[out] data	read data if the page_id exist in the buffer
 *  @return:  size of read page
 * */
const PMEM_BUF_BLOCK* 
pm_buf_read(
		PMEMobjpool*		pop,
	   	PMEM_BUF*			buf,
	   	const page_id_t		page_id,
	   	const page_size_t	size,
	   	byte*				data, 
		bool				sync) 
{


#if defined (UNIV_PMEMOBJ_BLOOM)
	//int bloom_ret = pm_bloom_check(buf->bf, page_id.fold());
	int bloom_ret = pm_cbf_check(buf->cbf, page_id.fold());
	if (bloom_ret == BLOOM_NOT_EXIST){
		return NULL;
	}
#endif //UNIV_PMEMOBJ_BLOOM
	//bool is_lock_on_read = true;	
	ulint hashed;
	int i;

#if defined(UNIV_PMEMOBJ_BUF_STAT)
	ulint cur_level = 0;
#endif
	//int found;

	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	//PMEM_BUF_BLOCK_LIST* plist;
	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	//char* pdata;
	byte* pdata;
	//size_t bytes_read;
	
	assert(buf);
	assert(data);	


/*handle page 0
// Note that there are two case for reading a page 0
// Case 1: read through buffer pool (handle in this function, during the server working time)
// Case 2: read without buffer pool during the sever stat/stop 
*/
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY)
	if (page_id.page_no() == 0) {
		//PMEM_BUF_BLOCK_LIST* pspec_list;
		PMEM_BUF_BLOCK*		pspec_block;

		const PMEM_BUF_BLOCK_LIST* pspec_list = D_RO(buf->spec_list); 
		//pmemobj_rwlock_rdlock(pop, &pspec_list->lock);
		//scan in the special list
		for (i = 0; i < pspec_list->cur_pages; i++){
			//const PMEM_BUF_BLOCK* pspec_block = D_RO(D_RO(pspec_list->arr)[i]);
			if (	D_RO(D_RO(D_RO(buf->spec_list)->arr)[i]) != NULL && 
					D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->id.equals_to(page_id)) {
				pspec_block = D_RW(D_RW(D_RW(buf->spec_list)->arr)[i]);
				//if(is_lock_on_read)
				pmemobj_rwlock_rdlock(pop, &pspec_block->lock);
				//found
				pdata = buf->p_align;
				memcpy(data, pdata + pspec_block->pmemaddr, pspec_block->size.physical()); 

				//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
				printf("==> PMEM_DEBUG read page 0 (case 1) of space %zu file %s\n",
				pspec_block->id.space(), pspec_block->file_name);

				//if(is_lock_on_read)
				pmemobj_rwlock_unlock(pop, &pspec_block->lock);
				return pspec_block;
			}
			//else: just skip this block
			//next block
		}//end for

		//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
		//this page 0 is not in PMEM, return NULL to read it from disk
		return NULL;
	} //end if page_no == 0
#endif //UNIV_PMEMOBJ_BUF_RECOVERY


#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(buf, hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), buf->PMEM_N_BUCKETS);
#endif

	TOID_ASSIGN(cur_list, (D_RO(buf->buckets)[hashed]).oid);

#if defined(UNIV_PMEMOBJ_BUF_STAT)
	//printf("====> space %zu page %zu hashed %zu \n", page_id.space(), page_id.page_no(), hashed);
	++buf->bucket_stats[hashed].n_reads;
#endif

	if ( TOID_IS_NULL(cur_list)) {
		//assert(!TOID_IS_NULL(cur_list));
		printf("PMEM_ERROR error in get hashded list, but return NULL, check again! \n");
		return NULL;
	}
	//plist = D_RO(D_RO(buf->buckets)[hashed]);
	//assert(plist);

	//pblock = NULL;
	//found = -1;
	
	while ( !TOID_IS_NULL(cur_list) && (D_RO(cur_list) != NULL) ) {
		//plist = D_RW(cur_list);
		//pmemobj_rwlock_rdlock(pop, &plist->lock);
		//Scan in this list
		if (D_RO(cur_list) == NULL) {
			printf("===> ERROR read NULL list \n");
			assert(0);
		}
		for (i = 0; i < D_RO(cur_list)->cur_pages; i++) {
			//accepted states: PMEM_IN_USED_BLOCK, PMEM_IN_FLUSH_BLOCK, PMEM_DEL_MARK_BLOCK
			//if ( D_RO(D_RO(plist->arr)[i])->state != PMEM_FREE_BLOCK &&
			if (	D_RO(D_RO(D_RO(cur_list)->arr)[i]) != NULL && 
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.equals_to(page_id)) {
				pblock = D_RW(D_RW(D_RW(cur_list)->arr)[i]);
				//if(is_lock_on_read)
				pmemobj_rwlock_rdlock(pop, &pblock->lock);
				
				pdata = buf->p_align;

				memcpy(data, pdata + pblock->pmemaddr, pblock->size.physical()); 
				//bytes_read = pblock->size.physical();
#if defined (UNIV_PMEMOBJ_DEBUG)
				assert( pm_check_io(pdata + pblock->pmemaddr, pblock->id) ) ;
#endif
#if defined(UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_reads_hit;
				if (D_RO(cur_list)->is_flush)
					++buf->bucket_stats[hashed].n_reads_flushing;
#endif
				//if(is_lock_on_read)
				pmemobj_rwlock_unlock(pop, &pblock->lock);
#if defined (UNIV_PMEMOBJ_BLOOM)
				if (bloom_ret == BLOOM_NOT_EXIST){
					printf("===> BLOOM false nagative fold %zu \n", page_id.fold());
				}
#endif
				//return pblock;
				return D_RO(D_RO(D_RO(cur_list)->arr)[i]);
			}
		}//end for

		//next list
		if ( TOID_IS_NULL(D_RO(cur_list)->next_list))
			break;
		TOID_ASSIGN(cur_list, (D_RO(cur_list)->next_list).oid);
		if (TOID_IS_NULL(cur_list) || D_RO(cur_list) == NULL)
			break;

#if defined(UNIV_PMEMOBJ_BUF_STAT)
		cur_level++;
		if (buf->bucket_stats[hashed].max_linked_lists < cur_level)
			buf->bucket_stats[hashed].max_linked_lists = cur_level;
#endif
		
	} //end while
	
#if defined (UNIV_PMEMOBJ_BLOOM)
				if (bloom_ret == BLOOM_MAY_EXIST){
					//printf("++++> BLOOM false positive fold %zu \n", page_id.fold());
					buf->cbf->n_false_pos_reads++;
				}
#endif
		return NULL;
}
/*
 * Use this function with pm_buf_write_with_flusher_append
 * */
const PMEM_BUF_BLOCK* 
pm_buf_read_lasted(
		PMEMobjpool*		pop,
	   	PMEM_BUF*			buf,
	   	const page_id_t		page_id,
	   	const page_size_t	size,
	   	byte*				data, 
		bool				sync) 
{
	
	bool is_lock_on_read = true;	
	ulint hashed;
	int i;

#if defined(UNIV_PMEMOBJ_BUF_STAT)
	ulint cur_level = 0;
#endif
	//int found;

	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	//PMEM_BUF_BLOCK_LIST* plist;
	TOID(PMEM_BUF_BLOCK) iter;
	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pblock;
	//char* pdata;
	byte* pdata;
	//size_t bytes_read;
	
	if (buf == NULL){
		printf("PMEM_ERROR, param buf is null in pm_buf_read\n");
		assert(0);
	}

	if (data == NULL){
		printf("PMEM_ERROR, param data is null in pm_buf_read\n");
		assert(0);
	}
	//bytes_read = 0;

	//PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(buf, hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), buf->PMEM_N_BUCKETS);
#endif
//	hashed = hash_f1(page_id.space(),
//			page_id.page_no(), PMEM_N_BUCKETS, PMEM_PAGE_PER_BUCKET_BITS);
	TOID_ASSIGN(cur_list, (D_RO(buf->buckets)[hashed]).oid);
	if ( TOID_IS_NULL(cur_list)) {
		//assert(!TOID_IS_NULL(cur_list));
		printf("PMEM_ERROR error in get hashded list, but return NULL, check again! \n");
		return NULL;
	}
	//plist = D_RO(D_RO(buf->buckets)[hashed]);
	//assert(plist);

	//pblock = NULL;
	//found = -1;

	while ( !TOID_IS_NULL(cur_list) ) {
		//backward scan in the current list
		ulint cur_pages = D_RO(cur_list)->cur_pages;

		for (i = cur_pages - 1; i >= 0 ; i--) {
			//accepted states: PMEM_IN_USED_BLOCK, PMEM_IN_FLUSH_BLOCK

			if (	(D_RO(D_RO(D_RO(cur_list)->arr)[i]) != NULL) &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
					D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.equals_to(page_id)) {
				pblock = D_RW(D_RW(D_RW(cur_list)->arr)[i]);
				if(is_lock_on_read)
					pmemobj_rwlock_rdlock(pop, &pblock->lock);

				//printf("====> PMEM_DEBUG found page_id[space %zu,page_no %zu] pmemaddr=%zu\n ", D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.space(), D_RO(D_RO(D_RO(cur_list)->arr)[i])->id.page_no(), D_RO(D_RO(D_RO(cur_list)->arr)[i])->pmemaddr);
				//copy data from PMEM_BUF to buf	
				pdata = buf->p_align;
				memcpy(data, pdata + pblock->pmemaddr, pblock->size.physical()); 
				//bytes_read = pblock->size.physical();
#if defined (UNIV_PMEMOBJ_DEBUG)
				assert( pm_check_io(pdata + pblock->pmemaddr, pblock->id) ) ;
#endif
#if defined(UNIV_PMEMOBJ_BUF_STAT)
				++buf->bucket_stats[hashed].n_reads;
				if (D_RO(cur_list)->is_flush)
					++buf->bucket_stats[hashed].n_reads_flushing;
#endif
				if(is_lock_on_read)
					pmemobj_rwlock_unlock(pop, &pblock->lock);

				//return bytes_read;
				return pblock;
			}
		}//end for

		//next list
		if ( TOID_IS_NULL(D_RO(cur_list)->next_list))
			break;
		TOID_ASSIGN(cur_list, (D_RO(cur_list)->next_list).oid);
#if defined(UNIV_PMEMOBJ_BUF_STAT)
		cur_level++;
		if (buf->bucket_stats[hashed].max_linked_lists < cur_level)
			buf->bucket_stats[hashed].max_linked_lists = cur_level;
#endif

	} //end while

		return NULL;
}

/*handle page 0
// Note that there are two case for reading a page 0
// Case 1: read through buffer pool (handle in pm_buf_read  during the server working time)
// Case 2: read without buffer pool during the sever stat/stop  (this function)
*/
const PMEM_BUF_BLOCK*
pm_buf_read_page_zero(
		PMEMobjpool*		pop,
		PMEM_BUF*			buf,
		char*				file_name,
		byte*				data) {

	ulint i;
	byte* pdata;

	PMEM_BUF_BLOCK* pspec_block;

	PMEM_BUF_BLOCK_LIST* pspec_list = D_RW(buf->spec_list); 

	//scan in the special list with the scan key is file handle
	for (i = 0; i < pspec_list->cur_pages; i++){
		if (	D_RO(D_RO(D_RO(buf->spec_list)->arr)[i]) != NULL && 
				D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->state != PMEM_FREE_BLOCK &&
				strstr(D_RO(D_RO(D_RO(buf->spec_list)->arr)[i])->file_name, file_name) != 0) {
			//if (pspec_block != NULL &&
			//	pspec_block->state != PMEM_FREE_BLOCK &&
			//	strstr(pspec_block->file_name,file_name)!= 0)  {
			pspec_block = D_RW(D_RW(D_RW(buf->spec_list)->arr)[i]);
			pmemobj_rwlock_rdlock(pop, &pspec_block->lock);
			//found
			printf("!!!!!!!! PMEM_DEBUG read_page_zero file= %s \n", pspec_block->file_name);
			pdata = buf->p_align;
			memcpy(data, pdata + pspec_block->pmemaddr, pspec_block->size.physical()); 

			//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
			pmemobj_rwlock_unlock(pop, &pspec_block->lock);
			return pspec_block;
		}
		//else: just skip this block
		//next block
		}//end for

	//pmemobj_rwlock_unlock(pop, &pspec_list->lock);
	//this page 0 is not in PMEM, return NULL to read it from disk
	return NULL;

}

/*
 * Check full lists in the buckets and linked-list 
 * Resume flushing them 
   Logical of this function is similar with pm_buf_write
   in case the list is full

  This function should be called by only recovery thread. We don't handle thread lock here 
 * */
void
pm_buf_resume_flushing(
		PMEMobjpool*			pop,
		PMEM_WRAPPER*				pmw) {

	PMEM_BUF* buf = pmw->pbuf;

	ulint i;
	TOID(PMEM_BUF_BLOCK_LIST) cur_list;
	PMEM_BUF_BLOCK_LIST* plist;
	PMEM_BUF_BLOCK_LIST* phashlist;

	for (i = 0; i < PMEM_N_BUCKETS; i++) {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		//printf ("\n====>resuming flush hash %zu\n", i);
#endif
		TOID_ASSIGN(cur_list, (D_RW(buf->buckets)[i]).oid);
		phashlist = D_RW(cur_list);
		if (phashlist->cur_pages >= phashlist->max_pages * PMEM_BUF_FLUSH_PCT) {
			assert(phashlist->is_flush);
			assert(phashlist->hashed_id == i);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("\ncase 1 PMEM_RECOVERY ==> resume flushing for hashed_id %zu list_id %zu\n", i, phashlist->list_id);
#endif
			pm_buf_handle_full_hashed_list(pop, pmw, i);
		}

		//(2) Check the linked-list of current list	
		TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
		while( !TOID_IS_NULL(cur_list)) {
			plist = D_RW(cur_list);
			if (plist->cur_pages >= plist->max_pages * PMEM_BUF_FLUSH_PCT) {
				//for the list in flusher, we only assign the flusher thread, no handle free list replace
				assert(plist->is_flush);
				assert(plist->hashed_id == i);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
				printf("\n\t\t case 2 PMEM_RECOVERY ==> resume flushing for linked list %zu of hashlist %zu hashed_id %zu\n", plist->list_id, phashlist->list_id, i);
#endif
				if (plist->list_id == 352){
					ulint test = 1;
				}
				pm_buf_assign_flusher(buf, plist);
			}

			TOID_ASSIGN(cur_list, (D_RW(cur_list)->next_list).oid);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("pm_buf_resume_flushing get next linked_list of hash_id %zu list_id %zu\n", i, plist->list_id);
#endif
			//next linked-list
		} //end while
		//next hashed list	
	} //end for
}

/*
 *Logicaly move a block to a index in the input list
 Simulate write a block on plist
 * */
void 
__append_buf_block(
		PMEMobjpool*		pop,
		PMEM_BUF*			buf,
		PMEM_BUF_BLOCK_LIST* plist,
		PMEM_BUF_BLOCK* p1)
{

	byte* pdata = buf->p_align;
	PMEM_BUF_BLOCK* p2;
	
	assert(plist->cur_pages < plist->max_pages);

	p2 = D_RW(D_RW(plist->arr)[plist->cur_pages]);

	assert(p2 != NULL);
	assert(p2->state == PMEM_FREE_BLOCK);

	// Start the copy
	p2->id.copy_from(p1->id);
	p2->state = PMEM_IN_USED_BLOCK;
	strcpy(p2->file_name, p1->file_name);

	pmemobj_memcpy_persist(pop, 
			pdata + p2->pmemaddr,
		    pdata +	p1->pmemaddr,
			p1->size.physical());

	p2->sync = p1->sync;
	if (p1->sync == false)
		++(plist->n_aio_pending);
	else
		++(plist->n_sio_pending);

	plist->cur_pages++;
}

/*
 *Handle flushng a bucket list when it is full
 (1) Assign a pointer in worker thread to the full list
 (2) Swap the full list with the first free list from the free pool 
 * */
void
pm_buf_handle_full_hashed_list(
		PMEMobjpool*		pop,
		PMEM_WRAPPER*		pmw,
		ulint				hashed) {

	PMEM_BUF* buf = pmw->pbuf;

	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;

	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);
	phashlist = D_RW(hash_list);


	/*    (1) Handle free list*/
get_free_list:
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n [1.2] BEGIN get_free_list to replace full list %zu ==>", phashlist->list_id);
#endif
	//Get a free list from the free pool
	pmemobj_rwlock_wrlock(pop, &(D_RW(buf->free_pool)->lock));

	TOID(PMEM_BUF_BLOCK_LIST) first_list = POBJ_LIST_FIRST (&(D_RW(buf->free_pool)->head));
	if (D_RW(buf->free_pool)->cur_lists == 0 ||
			TOID_IS_NULL(first_list) ) {
		pthread_t tid;
		tid = pthread_self();
		printf("PMEM_INFO: thread %zu free_pool->cur_lists = %zu, the first list is NULL? %d the free list is empty, sleep then retry..\n", tid, D_RO(buf->free_pool)->cur_lists, TOID_IS_NULL(first_list));
		pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));
		os_event_wait(buf->free_pool_event);

		goto get_free_list;
	}

	POBJ_LIST_REMOVE(pop, &D_RW(buf->free_pool)->head, first_list, list_entries);
	D_RW(buf->free_pool)->cur_lists--;
	//The free_pool may empty now, wait in necessary
	os_event_reset(buf->free_pool_event);

	pmemobj_rwlock_unlock(pop, &(D_RW(buf->free_pool)->lock));


	/*(2) Handle flusher */
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n[[1.1] BEGIN handle assign flusher list %zu hashed %zu ===> ", phashlist->list_id, hashed);
#endif
	pm_buf_assign_flusher(buf, phashlist);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n[1.1] END handle assign flusher list %zu]\n", phashlist->list_id);
#endif

	/* (3) Asign linked-list refs */
	assert(!TOID_IS_NULL(first_list));
	//This ref hashed_id used for batch AIO
	//Hashed id of old list is kept until its batch AIO is completed
	D_RW(first_list)->hashed_id = hashed;
	TOID_ASSIGN(D_RW(buf->buckets)[hashed], first_list.oid);
	TOID_ASSIGN( D_RW(first_list)->next_list, hash_list.oid);
	TOID_ASSIGN( D_RW(hash_list)->prev_list, first_list.oid);

#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("\n [1.2] END get_free_list %zu to replace full list %zu, hashed %zu ==>", D_RW(first_list)->list_id, phashlist->list_id, hashed);
#endif

#if defined (UNIV_PMEMOBJ_BUF_STAT)
	if ( !TOID_IS_NULL( D_RW(hash_list)->next_list ))
		++buf->bucket_stats[hashed].max_linked_lists;
#endif 

}

void
pm_buf_assign_flusher(
		PMEM_BUF*				buf,
		PMEM_BUF_BLOCK_LIST*	phashlist) {

	PMEM_FLUSHER* flusher = buf->flusher;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	printf("[pm_buf_assign_flusher begin hashed_id %zu phashlist %zu flusher size = %zu cur request = %zu ", phashlist->hashed_id, phashlist->list_id, flusher->size, flusher->n_requested);
#endif
assign_worker:
	mutex_enter(&flusher->mutex);
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	//printf ("pass mutex enter hash_id %zu list_id %zu==>", phashlist->hashed_id, phashlist->list_id);
#endif

	if (flusher->n_requested == flusher->size) {
		//all requested slot is full)
		printf("PMEM_INFO: all reqs are booked, sleep and wait \n");
		mutex_exit(&flusher->mutex);
		os_event_wait(flusher->is_req_full);	
		goto assign_worker;	
	}

	//find an idle thread to assign flushing task
	int64_t n_try = flusher->size;
	while (n_try > 0) {
		if (flusher->flush_list_arr[flusher->tail] == NULL) {
			//found
			flusher->flush_list_arr[flusher->tail] = phashlist;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("pm_buf_assign_flusher pointer id = %zu, list_id = %zu\n", flusher->tail, phashlist->list_id);
#endif
			++flusher->n_requested;
			//delay calling flush up to a threshold
			//printf("trigger worker...\n");
			if (flusher->n_requested >= PMEM_FLUSHER_WAKE_THRESHOLD) {
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
			printf("\n in [1.1] try to trigger flusher list_id = %zu\n", phashlist->list_id);
#endif
				os_event_set(flusher->is_req_not_empty);
				//see pm_flusher_worker() --> pm_buf_flush_list()
			}

			if (flusher->n_requested >= flusher->size) {
				os_event_reset(flusher->is_req_full);
			}
			//for the next 
			flusher->tail = (flusher->tail + 1) % flusher->size;
			break;
		}
		//circled increase
		flusher->tail = (flusher->tail + 1) % flusher->size;
		n_try--;
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("pm_buf_assign_flusher n_try = %zu\n", n_try);
#endif
	} //end while 
#if defined (UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
	//printf("pass while phashlist %zu flusher_tail = %zu flusher size = %zu n_requested %zu]\n", phashlist->list_id, flusher->tail, flusher->size, flusher->n_requested);
#endif
	//check
	if (n_try == 0) {
		/*This imply an logical error 
		 * */
		printf("PMEM_ERROR requested/size = %zu /%zu / %zu\n", flusher->n_requested, flusher->size);
		mutex_exit(&flusher->mutex);
		assert (n_try);
	}

	mutex_exit(&flusher->mutex);
	//end FLUSHER handling
}

///////////////////////// PARTITION ///////////////////////////
#if defined (UNIV_PMEMOBJ_BUF_PARTITION_STAT)
void 
pm_filemap_init(
		PMEM_BUF*		buf){
	
	ulint i;
	PMEM_FILE_MAP* fm;

	fm = static_cast<PMEM_FILE_MAP*> (malloc(sizeof(PMEM_FILE_MAP)));
	fm->max_size = 1024*1024;
	fm->items = static_cast<PMEM_FILE_MAP_ITEM**> (
		calloc(fm->max_size, sizeof(PMEM_FILE_MAP_ITEM*)));	
	//for (i = 0; i < fm->max_size; i++) {
	//	fm->items[i].count = 0
	//	fm->item[i].hashed_ids = static_cast<int*> (
	//			calloc(PMEM_BUCKET_SIZE, sizeof(int)));
	//}

	fm->size = 0;

	buf->filemap = fm;
}


#endif //UNIV_PMEMOBJ_PARTITION

//						END OF PARTITION//////////////////////

////////////////////////// Log-structure Buffer Implementation //////////////////
#if defined (UNIV_PMEMOBJ_LSB)
void
pm_wrapper_lsb_alloc_or_open(
		PMEM_WRAPPER*		pmw,
		const size_t		buf_size,
		const size_t		page_size)
{

	uint64_t i;
	char sbuf[256];

	PMEM_N_BUCKETS = srv_pmem_buf_n_buckets;
	PMEM_BUCKET_SIZE = srv_pmem_buf_bucket_size;
	PMEM_BUF_FLUSH_PCT = srv_pmem_buf_flush_pct;
	PMEM_N_FLUSH_THREADS= srv_pmem_n_flush_threads;
	PMEM_FLUSHER_WAKE_THRESHOLD = srv_pmem_flush_threshold;

	/////////////////////////////////////////////////
	// PART 1: NVM structures
	// ///////////////////////////////////////////////
	if (!pmw->plsb) {
		//Case 1: Alocate new buffer in PMEM
			printf("PMEMOBJ_INFO: allocate %zd MB of buffer in pmem\n", buf_size);

		if ( pm_wrapper_lsb_alloc(pmw, buf_size, page_size) == PMEM_ERROR ) {
			printf("PMEMOBJ_ERROR: error when allocate buffer in buf_dblwr_init()\n");
		}
	}
	else {
		//Case 2: Reused a buffer in PMEM
		printf("!!!!!!! [PMEMOBJ_INFO]: the server restart from a crash but the buffer is persist, in pmem: size = %zd \n", 
				pmw->plsb->size);
		//Check the page_size of the previous run with current run
		if (pmw->plsb->page_size != page_size) {
			printf("PMEM_ERROR: the pmem buffer size = %zu is different with UNIV_PAGE_SIZE = %zu, you must use the same page_size!!!\n ",
					pmw->plsb->page_size, page_size);
			assert(0);
		}
			
		//We need to re-align the p_align
		byte* p;
		p = static_cast<byte*> (pmemobj_direct(pmw->plsb->data));
		assert(p);
		pmw->plsb->p_align = static_cast<byte*> (ut_align(p, page_size));
	}
	////////////////////////////////////////////////////
	// Part 2: D-RAM structures and open file(s)
	// ///////////////////////////////////////////////////
	
	//init threads for handle flushing, implement in buf0flu.cc, each thread handle one bucket
	//pmw->plsb->flusher = pm_flusher_init(PMEM_N_FLUSH_THREADS);
	pmw->plsb->flusher = pm_flusher_init(PMEM_N_BUCKETS);

	//In any case (new allocation or resued, we should allocate the flush_events for buckets in DRAM
	pmw->plsb->flush_events = (os_event_t*) calloc(PMEM_N_BUCKETS, sizeof(os_event_t));

	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		sprintf(sbuf,"pm_flush_bucket%zu", i);
		pmw->plsb->flush_events[i] = os_event_create(sbuf);
	}

	/*Alocate the param array
	 */
	PMEM_BUF_BLOCK_LIST* plist;
	ulint arr_size = 2 * PMEM_N_BUCKETS;
	ulint max_bucket_size = buf_size / page_size; 

	pmw->plsb->param_arr_size = arr_size;
	pmw->plsb->param_arrs = static_cast<PMEM_AIO_PARAM_ARRAY*> (
		calloc(arr_size, sizeof(PMEM_AIO_PARAM_ARRAY)));	
	for ( i = 0; i < arr_size; i++) {
		//plist = D_RW(D_RW(pmw->pbuf->buckets)[i]);

		pmw->plsb->param_arrs[i].params = static_cast<PMEM_AIO_PARAM*> (
				//the worse case is one bucket has all pages in lsb list, alloc the size to max_bucket_size
				calloc(max_bucket_size, sizeof(PMEM_AIO_PARAM)));
		pmw->plsb->param_arrs[i].is_free = true;
	}
	pmw->plsb->cur_free_param = 0; //start with the 0
	
}

/*
 * CLose/deallocate resource in DRAM
 * */
void pm_wrapper_lsb_close(PMEM_WRAPPER* pmw) {
	uint64_t i;
	
	os_event_destroy(pmw->plsb->all_aio_finished);

	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		os_event_destroy(pmw->plsb->flush_events[i]); 
	}
	free(pmw->plsb->flush_events);

	//Free the param array
	for ( i = 0; i < PMEM_N_BUCKETS; i++) {
		//free(pmw->pbuf->params_arr[i]);
		free(pmw->plsb->param_arrs[i].params);
	}
	//free(pmw->pbuf->params_arr);
	free(pmw->plsb->param_arrs);
	//Free the flusher
	pm_buf_flusher_close(pmw->plsb->flusher);
}

int
pm_wrapper_lsb_alloc(
		PMEM_WRAPPER*		pmw,
	    const size_t		size,
		const size_t		page_size)
{
	assert(pmw);
	pmw->plsb = pm_pop_lsb_alloc(pmw->pop, size, page_size);
	if (!pmw->plsb)
		return PMEM_ERROR;
	else
		return PMEM_SUCCESS;

}
/*
 * Allocate LSB in persistent memory and assign to the pointer in the wrapper
 * This function only allocate structures that in NVM
 * Structures in D-RAM are allocated outside in pm_wrapper_buf_alloc_or_open() function
 * */
PMEM_LSB* 
pm_pop_lsb_alloc(
		PMEMobjpool*		pop,
		const size_t		size,
		const size_t		page_size)
{
	char* p;
	size_t align_size;

	//(1) allocate in PMEM
	TOID(PMEM_LSB) lsb; 

	POBJ_ZNEW(pop, &lsb, PMEM_LSB);

	PMEM_LSB *plsb = D_RW(lsb);
	//align sizes to a pow of 2
	assert(ut_is_2pow(page_size));
	align_size = ut_uint64_align_up(size, page_size);


	plsb->size = align_size;
	plsb->page_size = page_size;
	plsb->type = BUF_TYPE;
	plsb->is_new = true;

	plsb->all_aio_finished = os_event_create("lsb_all_aio_finished");

	plsb->data = pm_pop_alloc_bytes(pop, align_size);
	//align the pmem address for DIRECT_IO
	p = static_cast<char*> (pmemobj_direct(plsb->data));
	assert(p);
	//pbuf->p_align = static_cast<char*> (ut_align(p, page_size));
	plsb->p_align = static_cast<byte*> (ut_align(p, page_size));
	pmemobj_persist(pop, plsb->p_align, sizeof(*plsb->p_align));

	if (OID_IS_NULL(plsb->data)){
		//assert(0);
		return NULL;
	}
	//(2) init the list
	pm_lsb_lists_init(pop, plsb, align_size, page_size);
	pmemobj_persist(pop, plsb, sizeof(*plsb));

	//(3) init the hashtable
	pm_lsb_hashtable_init(pop, plsb, PMEM_N_BUCKETS);
	
	return plsb;
} 
/*
 * Init in-PMEM lists
 * centralized LSB lists
 * */
void 
pm_lsb_lists_init(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb, 
		const size_t	total_size,
	   	const size_t	page_size)
{
	uint64_t i;
	uint64_t j;
	size_t offset;
	PMEM_BUF_BLOCK_LIST* plist;
	page_size_t page_size_obj(page_size, page_size, false);


	size_t n_pages = (total_size / page_size);


	//Don't reset those variables during the init
	offset = 0;

	//init the temp args struct
	struct list_constr_args* args = (struct list_constr_args*) malloc(sizeof(struct list_constr_args)); 
	args->size.copy_from(page_size_obj);
	args->check = PMEM_AIO_CHECK;
	args->state = PMEM_FREE_BLOCK;
	TOID_ASSIGN(args->list, OID_NULL);

	//(1) Init the buckets

	//init the list
	POBJ_ZNEW(pop, &lsb->lsb_list, PMEM_BUF_BLOCK_LIST);	
	if(TOID_IS_NULL(lsb->lsb_list)) {
		fprintf(stderr, "POBJ_ZNEW\n");
		assert(0);
	}
	plist = D_RW(lsb->lsb_list);

	//pmemobj_rwlock_wrlock(pop, &plist->lock);

	plist->cur_pages = 0;
	plist->is_flush = false;
	plist->n_aio_pending = 0;
	plist->n_sio_pending = 0;
	plist->max_pages = n_pages;
	plist->list_id = 0;
	plist->hashed_id = 0;
	plist->check = PMEM_AIO_CHECK;
	TOID_ASSIGN(plist->next_list, OID_NULL);
	TOID_ASSIGN(plist->prev_list, OID_NULL);

	//init pages in bucket
	pm_buf_single_list_init(pop, lsb->lsb_list, offset, args, n_pages, page_size);

	
}
/*
 *Init the hashtable
 * */
void
pm_lsb_hashtable_init(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb, 
		const size_t	n_buckets){

	ulint i;
	PMEM_LSB_HASHTABLE* pht;

	POBJ_ZNEW(pop, &lsb->ht, PMEM_LSB_HASHTABLE);	
	pht = D_RW(lsb->ht);

	pht->n_buckets = n_buckets;
	pht->buckets = (PMEM_LSB_HASH_BUCKET*) calloc(n_buckets, sizeof(PMEM_LSB_HASH_BUCKET));
	
	//init buckets
	for (i = 0; i < n_buckets; ++i){
		PMEM_LSB_HASH_BUCKET b = pht->buckets[i];
		b.head = b.tail = NULL;
		b.n_entries = 0;
	}

}

void
pm_lsb_hashtable_free(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb) {
	
	PMEM_LSB_HASHTABLE* pht = D_RW(lsb->ht);
	
	pm_lsb_hashtable_reset(pop, lsb);
	free(pht->buckets);
}

void
pm_lsb_hashtable_reset(
		PMEMobjpool*	pop,
		PMEM_LSB*		lsb) {

	ulint i;	
	PMEM_LSB_HASH_ENTRY* e;
	PMEM_LSB_HASH_ENTRY* prev;
	PMEM_LSB_HASHTABLE* pht = D_RW(lsb->ht);
	PMEM_LSB_HASH_BUCKET* pbucket;
	
	for (i = 0; i < pht->n_buckets; ++i){
		pbucket = &pht->buckets[i];
		
		//we need the lock here, read thread may access	
		pmemobj_rwlock_wrlock(pop, &pbucket->lock);
		//reset each bucket
		e = pbucket->head;
		while (e != NULL){
			prev = e;
			e = e->next;	
			prev->next = NULL;
			prev->prev=NULL;
			free(prev);
			prev = NULL;
		}
		pbucket->head = pht->buckets[i].tail = NULL;
		pbucket->n_entries = 0;
		pmemobj_rwlock_unlock(pop, &pbucket->lock);
	}

}
/*
 * Add an entry in hashtable
 * The caller response for allocate the entry
 * The caller must acquire the lsb_list lock
 * */

int 
pm_lsb_hashtable_add_entry(
		PMEMobjpool*			pop,
		PMEM_LSB*				lsb,
		PMEM_LSB_HASH_ENTRY*	entry){

	ulint hashed;
	PMEM_LSB_HASHTABLE* pht = D_RW(lsb->ht);
	PMEM_LSB_HASH_BUCKET* bucket;
	PMEM_LSB_HASH_ENTRY* e;
	PMEM_LSB_HASH_ENTRY* tem;

	PMEM_BUF_BLOCK_LIST* plist = D_RW(lsb->lsb_list);
	
	PMEM_HASH_KEY(hashed, entry->id.fold(), pht->n_buckets);
	bucket = &(pht->buckets[hashed]);
	
	//we need the lock here, read thread may access the bucket	
	pmemobj_rwlock_wrlock(pop, &bucket->lock);
	int hole_id = -1;

	e = bucket->head;

	//(1) On-the-fly reclaiming the duplicated page
	while (e != NULL){
		if (e->id.equals_to(entry->id)){
			hole_id = e->lsb_entry_id;
			//Remove the entry in hashtable
			if (e->prev == NULL){
				//The head entry
				bucket->head = e->next;
				if (e->next != NULL)
					e->next->prev = NULL;
			}
			else if (e->next == NULL){
				bucket->tail = e->prev;
				e->prev->next = NULL;
			}
			else {
				tem = e->prev;
				tem->next = e->next;
				e->next->prev = tem;	
			}
			e->prev = NULL;
			e->next = NULL;
			free(e);
			e = NULL;
			--bucket->n_entries;
			break;
		}
		e = e->next;
	}

	//(2) Add the entry in the hashtable
	if (bucket->n_entries == 0){
		bucket->head = bucket->tail = entry;
	}
	else {
		//add to the tail of the bucket
		bucket->tail->next = entry;
		entry->prev = bucket->tail;
		bucket->tail = entry;
	}	
	++bucket->n_entries;
	pmemobj_rwlock_unlock(pop, &bucket->lock);

	return hole_id;
}

PMEM_LSB_HASH_ENTRY*
pm_lsb_hashtable_search_entry(
		PMEMobjpool*			pop,
		PMEM_LSB*				lsb,
		PMEM_LSB_HASH_ENTRY*	entry){

	PMEM_LSB_HASH_ENTRY* ret_entry = NULL;

	ulint hashed;
	PMEM_LSB_HASHTABLE* pht = D_RW(lsb->ht);
	PMEM_LSB_HASH_BUCKET* bucket;
	PMEM_LSB_HASH_ENTRY* e;
	PMEM_LSB_HASH_ENTRY* tem;
	
	PMEM_HASH_KEY(hashed, entry->id.fold(), pht->n_buckets);
	bucket = &(pht->buckets[hashed]);
	
	e = bucket->head;
	while (e != NULL){
		if (e->id.equals_to(entry->id)){
			ret_entry = e;
			break;
		}
		e = e->next;
	}
	return ret_entry;
}

int
pm_lsb_write(
			PMEMobjpool*	pop,
		   	PMEM_LSB*		lsb,
		   	page_id_t		page_id,
		   	page_size_t		size,
		   	byte*			src_data,
		   	bool			sync) 
{

	ulint hashed;
	ulint i, i_free;

	PMEM_BUF_BLOCK_LIST* plist;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	PMEM_BUF_BLOCK* pblock;

	byte* pdata;
	//page_id_t page_id;
	size_t page_size;

	assert(lsb);
	assert(src_data);

	plist = D_RW(lsb->lsb_list);

	page_size = size.physical();

try_again:
	//acquire lock
	pmemobj_rwlock_wrlock(pop, &lsb->lsb_lock);
	if (plist->cur_pages >= plist->max_pages && plist->is_flush){
		
		pmemobj_rwlock_unlock(pop, &lsb->lsb_lock);
		os_event_wait(lsb->all_aio_finished);
		goto try_again;
	}	
	//(1) Search the first free slot in the lsb list
	for (i = 0; i < plist->max_pages; ++i){
		pblock = D_RW(D_RW(plist->arr)[i]);
		if (pblock->state == PMEM_FREE_BLOCK) {
			pfree_block = pblock;
			i_free = i;
			break;
		}
	}
	assert(i < plist->max_pages);
	//(2) Add data to the free block in lsb list
	pdata = lsb->p_align;

	fil_node_t*			node;
	node = pm_get_node_from_space(page_id.space());
	strcpy(pfree_block->file_name, node->name);

	pfree_block->sync = sync;
	pfree_block->id.copy_from(page_id);

	assert(pfree_block->size.equals_to(size));

	pfree_block->state = PMEM_IN_USED_BLOCK;

TX_BEGIN(pop) {
	pmemobj_memcpy_persist(pop, pdata + pfree_block->pmemaddr, src_data, page_size); 
}TX_ONABORT {

}TX_END

	++plist->cur_pages;
	// (3) Add a corresponding entry in the hashtable
	PMEM_LSB_HASH_ENTRY* e = (PMEM_LSB_HASH_ENTRY*) malloc(sizeof(PMEM_LSB_HASH_ENTRY));
	strcpy(e->file_name, node->name);
	e->id.copy_from(page_id);
	e->size = page_size;
	//save the id of the list entry, use this for making the hole in reclaiming process
	e->lsb_entry_id = i_free;
	e->next = e->prev = NULL;	
	//add entry in the hashtable, if the entry with page_id exist, remove it and set the corresponding page in lsb list as a hole	
	
	int hole_id = pm_lsb_hashtable_add_entry(pop, lsb, e);
	if (hole_id >= 0){
		//Set the hole in the lsb list
		PMEM_BUF_BLOCK* phole_block = D_RW(D_RW(plist->arr)[hole_id]);
		phole_block->state = PMEM_FREE_BLOCK;
#if defined (UNIV_PMEMOBJ_LSB_DEBUG)
		printf("LSB: reclaim exist page at %d, cur_pages / max_pages %zu/%zu \n", hole_id, plist->cur_pages, plist->max_pages);
#endif
		--plist->cur_pages;
	}
	
	if (plist->cur_pages >= plist->max_pages - 2){
#if defined (UNIV_PMEMOBJ_LSB_DEBUG)
		printf("LSB [1] pm_lsb_write the lsb list is full\n");
#endif
		//handle full lsb list
		plist->is_flush = true;
		os_event_reset(lsb->all_aio_finished);

		pm_lsb_assign_flusher(lsb);
#if defined (UNIV_PMEMOBJ_LSB_DEBUG)
		printf("LSB [4] pm_lsb_write finish assign flusher threads\n");
#endif
		//wait for AIO finish
		os_event_wait(lsb->all_aio_finished);
		//now all aio finished
		plist->is_flush = false;
#if defined (UNIV_PMEMOBJ_LSB_DEBUG)
		printf("LSB [6] pm_lsb_write wake from sleep.\n");
#endif
	}

	pmemobj_rwlock_unlock(pop, &lsb->lsb_lock);

	return PMEM_SUCCESS;
}

/*
 * Assign flusher to each bucket in the lsb list
 * */
void
pm_lsb_assign_flusher(
		PMEM_LSB*				lsb)
{
	ulint i;
	ulint n_pages = 0; //n_pages to the flusher

	PMEM_FLUSHER* flusher = lsb->flusher;
	PMEM_LSB_HASHTABLE* pht = D_RW(lsb->ht);
	PMEM_LSB_HASH_BUCKET* pbucket;

	mutex_enter(&flusher->mutex);
	//For simple implementation, supose the flusher has enough entry to handle all buckets in the lsb list
	assert(flusher->size >= pht->n_buckets);
	
	//this value is set 0 here and increase in pm_lsb_handle_finished_block() in buf0flu.cc	
	lsb->n_aio_submitted = lsb->n_aio_completed = 0;

	flusher->n_requested = 0;

	for (i = 0; i < pht->n_buckets; ++i) {
		pbucket = &pht->buckets[i];
		//skip empty bucket
		if (pbucket->n_entries == 0){
#if defined(UNIV_PMEMOBJ_LSB_DEBUG)
			printf("LSB ==> bucket %zu has no entry, skip it! \n", i);
#endif
			continue;
		}

		flusher->bucket_arr[flusher->n_requested] = pbucket;
		++flusher->n_requested;
		n_pages += pbucket->n_entries;
	}
	//trigger the worker thread
#if defined(UNIV_PMEMOBJ_LSB_DEBUG)
	printf("LSB [2] Before call flusher worker, n_requests %zu n_pages %zu \n", flusher->n_requested, n_pages);
#endif
	os_event_set(flusher->is_req_not_empty);
	//os_event_reset(flusher->is_req_full);

	mutex_exit(&flusher->mutex);
}
/*
 * Propagate all pages in the bucket using batch AIO
 * (one io_submit() for all pages)
 * */
void
pm_lsb_flush_bucket_batch(
		PMEMobjpool*			pop,
	   	PMEM_LSB*				lsb,
	   	PMEM_LSB_HASH_BUCKET*	pbucket) {

	assert(pop);
	assert(lsb);

#if defined (UNIV_PMEMOBJ_BUF)
		ulint type = IORequest::PM_WRITE;
#else
		ulint type = IORequest::WRITE;
#endif
		IORequest request(type);
		//pm_fil_io_batch() defined in fil0fil.h and implemented in fil0fil.cc
		dberr_t err = pm_lsb_fil_io_batch(request, pop, lsb, pbucket);
		
}
/*
 * Propagate each page in the bucket using normal AIO (one io_submit() for one page)
 * */
void
pm_lsb_flush_bucket(
		PMEMobjpool*			pop,
		PMEM_LSB*				lsb,
		PMEM_LSB_HASH_BUCKET*	pbucket) {

	assert(pop);
	assert(lsb);
	ulint count;

#if defined (UNIV_PMEMOBJ_BUF)
	ulint type = IORequest::PM_WRITE;
#else
	ulint type = IORequest::WRITE;
#endif
	IORequest request(type);

	PMEM_BUF_BLOCK* pblock;
	PMEM_BUF_BLOCK_LIST* plsb_list;
	byte* pdata;
	PMEM_LSB_HASH_ENTRY* e;

	pdata = lsb->p_align;
	plsb_list = D_RW(lsb->lsb_list);
	
	count = 0;
	e = pbucket->head;
	while (e != NULL){
		//Get the corresponding pblock with this hashtable entry
		int id = e->lsb_entry_id;
		assert(id >= 0 && id < plsb_list->max_pages);

		pblock = D_RW(D_RW(plsb_list->arr)[id]);
		assert(pblock);

		if (pblock->state == PMEM_FREE_BLOCK) {
			printf("====> LSB skip the free block in pm_lsb_fil_io_batch \n ");
			e = e->next;
			continue;
		}
		pblock->state = PMEM_IN_FLUSH_BLOCK;	
		dberr_t err = fil_io(request, 
			false, pblock->id, pblock->size, 0, pblock->size.physical(),
			pdata + pblock->pmemaddr, pblock);
		if (err != DB_SUCCESS){
			printf("PMEM_ERROR: fil_io() in pm_buf_list_write_to_datafile() space_id = %"PRIu32" page_id = %"PRIu32" size = %zu \n ", pblock->id.space(), pblock->id.page_no(), pblock->size.physical());
			assert(0);
		}

		++count;
		if (count >= pbucket->n_entries)
			break;
		e = e->next;

	}//end while for each page in the bucket

}
/*
 * Read a page from the LSB using page_id as key
 * Searching in the hashtable, get the corresponding block in case the block is found
 * */
const PMEM_BUF_BLOCK* 
pm_lsb_read(
		PMEMobjpool*		pop,
	   	PMEM_LSB*			lsb,
	   	const page_id_t		page_id,
	   	const page_size_t	size,
	   	byte*				data, 
		bool				sync)
{
	ulint hashed;
	ulint count;
	int lsb_id;
	byte* pdata;
	
	bool is_lock_bucket = true;

	PMEM_LSB_HASH_BUCKET* pbucket;
	
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
	pbucket = &(D_RW(lsb->ht))->buckets[hashed];
	assert(pbucket);

	//We need the lock here, write thread may modify the bucket
	if(is_lock_bucket)
		pmemobj_rwlock_rdlock(pop, &pbucket->lock);

	pdata = lsb->p_align;
	count = 0;
	
	//search in the hashtable	
	PMEM_LSB_HASH_ENTRY* e = pbucket->head;
	while (e != NULL){
		if (e->id.equals_to(page_id)){
			//found, get the corresponding block in the lsb list
			lsb_id = e->lsb_entry_id;
			const PMEM_BUF_BLOCK* pblock = D_RO(D_RO(D_RO(lsb->lsb_list)->arr)[lsb_id]);
			
			assert(pblock);
			assert(pblock->size.physical() == size.physical());
			memcpy(data, pdata + pblock->pmemaddr, pblock->size.physical()); 

			if(is_lock_bucket)
				pmemobj_rwlock_unlock(pop, &pbucket->lock);
			return pblock;
		}
		e = e->next;
		++count;
		if (count >= pbucket->n_entries){
			break;
		}
	}//end while

	//not found
	if(is_lock_bucket)
		pmemobj_rwlock_unlock(pop, &pbucket->lock);
	return NULL;
}


#endif //UNIV_PMEMOBJ_LSB
//////////////////////// STATISTICS FUNCTS/////////////////////

#if defined (UNIV_PMEMOBJ_BUF_STAT)

#define PMEM_BUF_BUCKET_STAT_PRINT(pb, index) do {\
	assert (0 <= index && index <= PMEM_N_BUCKETS);\
	PMEM_BUCKET_STAT* p = &pb->bucket_stats[index];\
	printf("bucket %zu [n_writes %zu,\t n_overwrites %zu,\t n_reads %zu, n_reads_hit %zu, n_reads_flushing %zu \tmax_linked_lists %zu, \tn_flushed_lists %zu] \n ",index,  p->n_writes, p->n_overwrites, p->n_reads, p->n_reads_hit, p->n_reads_flushing, p->max_linked_lists, p->n_flushed_lists); \
}while (0)


void pm_buf_bucket_stat_init(PMEM_BUF* pbuf) {
	int i;

	PMEM_BUCKET_STAT* arr = 
		(PMEM_BUCKET_STAT*) calloc(PMEM_N_BUCKETS, sizeof(PMEM_BUCKET_STAT));
	
	for (i = 0; i < PMEM_N_BUCKETS; i++) {
		arr[i].n_writes = arr[i].n_overwrites = 
			arr[i].n_reads = arr[i].n_reads_hit = arr[i].n_reads_flushing = arr[i].max_linked_lists =
			arr[i].n_flushed_lists = 0;
	}
	pbuf->bucket_stats = arr;
}

/*
 *Print statistic infomation for all hashed lists
 * */
void pm_buf_stat_print_all(PMEM_BUF* pbuf) {
	ulint i;
	PMEM_BUCKET_STAT* arr = pbuf->bucket_stats;
	PMEM_BUCKET_STAT sumstat;

	sumstat.n_writes = sumstat.n_overwrites =
		sumstat.n_reads = sumstat.n_reads_hit = sumstat.n_reads_flushing = sumstat.max_linked_lists = sumstat.n_flushed_lists = 0;


	for (i = 0; i < PMEM_N_BUCKETS; i++) {
		sumstat.n_writes += arr[i].n_writes;
		sumstat.n_overwrites += arr[i].n_overwrites;
		sumstat.n_reads += arr[i].n_reads;
		sumstat.n_reads_hit += arr[i].n_reads_hit;
		sumstat.n_reads_flushing += arr[i].n_reads_flushing;
		sumstat.n_flushed_lists += arr[i].n_flushed_lists;

		if (sumstat.max_linked_lists < arr[i].max_linked_lists) {
			sumstat.max_linked_lists = arr[i].max_linked_lists;
		}

		PMEM_BUF_BUCKET_STAT_PRINT(pbuf, i);
	}

	printf("\n==========\n Statistic info:\n n_writes\t n_overwrites \t n_reads \t n_reads_hit \t n_reads_flushing \t max_linked_lists \t n_flushed_lists \n %zu \t %zu \t %zu \t %zu \t %zu \t %zu \t %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	printf("\n==========\n");

	fprintf(pbuf->deb_file, "\n==========\n Statistic info:\n n_writes\t n_overwrites \t n_reads \t n_reads_hit \t n_reads_flushing \t max_linked_lists \t n_flushed_lists \n %zu \t %zu \t %zu \t %zu \t %zu \t %zu \t %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	fprintf(pbuf->deb_file, "\n==========\n");

	fprintf(debug_file, "\n==========\n Statistic info:\n n_writes n_overwrites n_reads n_reads_hit n_reads_flushing max_linked_lists n_flushed_lists \n %zu %zu %zu %zu %zu %zu %zu \n",
			sumstat.n_writes, sumstat.n_overwrites, sumstat.n_reads, sumstat.n_reads_hit, sumstat.n_reads_flushing, sumstat.max_linked_lists, sumstat.n_flushed_lists);
	fprintf(pbuf->deb_file, "\n==========\n");

}

#endif //UNIV_PMEMOBJ_STAT
///////////////////// DEBUG Functions /////////////////////////

/*
 *Read the header from frame that contains page_no and space. Then check whether they matched with the page_id.page_no() and page_id.space()
 * */
bool pm_check_io(byte* frame, page_id_t page_id) {
	//Does some checking
	ulint   read_page_no;
	ulint   read_space_id;

	read_page_no = mach_read_from_4(frame + FIL_PAGE_OFFSET);
	read_space_id = mach_read_from_4(frame + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
	if ((page_id.space() != 0
				&& page_id.space() != read_space_id)
			|| page_id.page_no() != read_page_no) {                                      
		/* We did not compare space_id to read_space_id
		 *             if bpage->space == 0, because the field on the
		 *                         page may contain garbage in MySQL < 4.1.1,                                        
		 *                                     which only supported bpage->space == 0. */

		ib::error() << "PMEM_ERROR: Space id and page no stored in "
			"the page, read in are "
			<< page_id_t(read_space_id, read_page_no)
			<< ", should be " << page_id;
		return 0;
	}   
	return 1;
}


void pm_buf_print_lists_info(PMEM_BUF* buf){
	PMEM_BUF_FREE_POOL* pfreepool;
	PMEM_BUF_BLOCK_LIST* plist;
	uint64_t i;

	printf("PMEM_DEBUG ==================\n");

	pfreepool = D_RW(buf->free_pool);
	printf("The free pool: curlists=%zd \n", pfreepool->cur_lists);

	printf("The buckets: \n");
	
	for (i = 0; i < PMEM_N_BUCKETS; i++){

		plist = D_RW( D_RW(buf->buckets)[i] );
		printf("\tBucket %zu: list_id=%zu cur_pages=%zd ", i, plist->list_id, plist->cur_pages);
		printf("\n");
	}		
}
//////////////////////// THREAD HANDLER /////////////
#endif //UNIV_PMEMOBJ_BUF
