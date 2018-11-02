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

#include "my_pmem_common.h"
#include "my_pmemobj.h"

#include "os0file.h"

//////////////////// Dirty Page Table functions//////////
/*
 * Init the Dirty Page Table as the hashtable with n buckets
 * */
MEM_DPT* init_DPT(uint64_t n) {
	MEM_DPT* dpt;
	int i;

	dpt = (MEM_DPT*) malloc(sizeof(MEM_DPT));
	dpt->n = n;
	dpt->buckets = (MEM_DPT_ENTRY**) calloc(
			n, sizeof(MEM_DPT_ENTRY*));

	for (i = 0; i < n; i++) {
		dpt->buckets[i] = NULL;
	}

	return dpt;
}

/*
 * Add a log record into the dirty page table.
 * The same log record has added in Transaction Table
 * */
void add_log_to_DPT(MEM_DPT* dpt, MEM_LOG_REC* rec){
	ulint hashed;
	MEM_DPT_ENTRY* bucket;
	MEM_DPT_ENTRY* prev_bucket;
	
	//(1) Get the hash
	PMEM_LOG_HASH_KEY(hashed, rec->pid.fold(), dpt->n);
	assert (hashed < dpt->n);

	bucket = dpt->buckets[hashed];
	prev_bucket = NULL;

	//(2) Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->id.equals_to(rec->pid)){
			//(2.1) insert the log record into this bucket
			add_log_to_DPT_entry(bucket, rec); 
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the dirty page is not exist
	if (bucket == NULL){
		//(2.2.1) new bucket 
		MEM_DPT_ENTRY* new_entry = (MEM_DPT_ENTRY*) malloc(sizeof(MEM_DPT_ENTRY));	

		new_entry->id.copy_from(rec->pid);
		new_entry->curLSN = 0;
		new_entry->next = NULL;

		//new list and add the rec as the first item
		new_entry->list = (MEM_LOG_LIST*) malloc(sizeof(MEM_LOG_LIST));
		new_entry->list->head = new_entry->list->tail = NULL;
		new_entry->list->n_items = 0;
		add_log_to_DPT_entry(new_entry, rec); 

		//link the new bucket
		if (prev_bucket == NULL){
			//this is the first bucket
			dpt->buckets[hashed] = new_entry;
		}	
		else{
			prev_bucket->next = new_entry;
		}

	} //end if (bucket == NULL)

}

/*
 *Add a log record to a DPT entry (as sorted doubled linked list)
 * */
void add_log_to_DPT_entry(MEM_DPT_ENTRY* entry, MEM_LOG_REC* rec){
	MEM_LOG_REC*	it;
	MEM_LOG_LIST*	list;
	uint64_t		lsn;
	
	assert(entry);
	assert(rec);

	mutex_enter(&entry->lock);
	// (1) Generate the LSN
	lsn = entry->curLSN = entry->curLSN	+ 1;
	rec->lsn = lsn;

	// (2) insert the log record to the list
	list = entry->list;
	if (list->n_items == 0){
		list->head = list->tail = rec;
		list->n_items++;

		mutex_exit(&entry->lock);

		return;
	}

	it = list->head;
	while(it != NULL){
		if (it->lsn > rec->lsn){
			//insert the log right before it
			if(it->dpt_prev == NULL){
				rec->dpt_next = it;
				rec->dpt_prev = it->dpt_prev;
				it->dpt_prev = rec;
				list->head = rec;
			}
			else {
				rec->dpt_next = it;
				rec->dpt_prev = it->dpt_prev;
				it->dpt_prev->dpt_next = rec;
				it->dpt_prev = rec;
			}
			break;
		}
		it = it->dpt_next;
	}
	if (it == NULL){
		//insert to the tail
		rec->dpt_next = NULL;
		rec->dpt_prev = list->tail;
		list->tail->dpt_next = rec;
		list->tail = rec;
	}

	list->n_items++;
	mutex_exit(&entry->lock);
}
//////////////////// End Dirty Page Table functions//////////


//////////////////// Transaction Table functions//////////
/*
 * Init the Transaction Table as the hashtable with n buckets
 * */
MEM_TT* init_TT(uint64_t n) {
	MEM_TT* tt;

	tt = (MEM_TT*) malloc(sizeof(MEM_TT));
	tt->n = n;
	tt->buckets = (MEM_TT_ENTRY**) calloc(
			n, sizeof(MEM_TT_ENTRY*));

	return tt;
}

/*
 * Add a log record into the dirty page table.
 * */
void add_log_to_TT(MEM_TT* tt, MEM_LOG_REC* rec){
	ulint hashed;
	MEM_TT_ENTRY* bucket;
	MEM_TT_ENTRY* prev_bucket;

	//(1) Get the hash by transaction id
	PMEM_LOG_HASH_KEY(hashed, rec->tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	prev_bucket = NULL;

	//(2) Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->tid == rec->tid){
			//(2.1) insert the log record into this bucket
			add_log_to_TT_entry(bucket, rec); 
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the transaction is not exist
	if (bucket == NULL){
		//(2.2.1) new bucket 
		MEM_TT_ENTRY* new_entry = (MEM_TT_ENTRY*) malloc(sizeof(MEM_TT_ENTRY));	

		new_entry->tid = rec->tid;
		new_entry->next = NULL;

		//new list and add the rec as the first item
		new_entry->list = (MEM_LOG_LIST*) malloc(sizeof(MEM_LOG_LIST));
		new_entry->list->head = new_entry->list->tail = NULL;
		new_entry->list->n_items = 0;
		add_log_to_TT_entry(new_entry, rec); 

		//link the new bucket
		if (prev_bucket == NULL){
			//this is the first bucket
			tt->buckets[hashed] = new_entry;
		}	
		else{
			prev_bucket->next = new_entry;
		}

	} //end if (bucket == NULL)

	return;
}

/*
 *Add a log record to a tail of TT entry (as doubled linked list)
 * */
void add_log_to_TT_entry(MEM_TT_ENTRY* entry, MEM_LOG_REC* rec){

	MEM_LOG_LIST*	list;

	assert(entry);
	assert(rec);
	//we don't need to acquire the lock because only this transaction can touch the entry
	//mutex_enter(&entry->lock);
	//(1) Insert the log record at the tail of the list
	list = entry->list;
	//If the list is empty
	if (list->n_items == 0){
		list->head = list->tail = rec;
		list->n_items++;

		return;
	}
	
	rec->tt_next = NULL;
	rec->tt_prev = list->tail;
	list->tail->tt_next = rec;
	list->tail = rec;

	list->n_items++;
	return;
}
/*
 * Handle changes in the transaction table when a transaction tid commit 
 * */
int trx_commit_TT(MEM_TT* tt, uint64_t tid){
	ulint			hashed;
	MEM_TT_ENTRY*	bucket;
	MEM_LOG_LIST*	list;
	MEM_LOG_REC*	rec;
	
	//(1) Get the hash by transaction id
	PMEM_LOG_HASH_KEY(hashed, rec->tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	
	//(2) Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->tid == rec->tid){
			break;
		}
		bucket = bucket->next;
	}

	if (bucket == NULL){
		printf("PMEM_LOG Error, tid %zu not found in transaction table\n", tid);
		return PMEM_ERROR;
	}
	//If you reach here, bucket is the pointer to the list of REDO log 
	
	//(2) Write REDO log records by forward scaning in the list of rec 
	list = bucket->list;
	rec = list->head;
	assert(rec != NULL);
	//forward scanning
	while (rec != NULL){

		rec = rec->tt_next;
	}//end while (rec != NULL)
}

/*
 * write REDO log records when a transaction commits
 * rec_arr (in): pointers to REDO log record in a same page
 * arr_size (in): size of ther array
 * page_id (in): page_id of the dirty page
 * */
int
pm_REDO_log_write(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
	   	MEM_LOG_REC**	rec_arr,
		uint64_t		arr_size,
		page_id_t		page_id
	   	) 
{
	ulint hashed;
	ulint i;
	
	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;
	PMEM_BUF_BLOCK* pblock;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;


	//(1) Get the bucket that has the page
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), PMEM_N_BUCKETS);
#endif

	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);

	phashlist = D_RW(hash_list);
	assert(phashlist);
	//(2) Get the page in the bucket
	
	pblock = NULL;

	for (i = 0; i < D_RO(hash_list)->cur_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);
		if(pfree_block->state == PMEM_IN_USED_BLOCK &&
				pfree_block->id.equals_to(page_id)) {

			//Case A: Directy apply REDO log to the page
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			//TODO: apply REDO log to the page
			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}
		else if(pfree_block->state == PMEM_PLACE_HOLDER_BLOCK &&
				pfree_block->id.equals_to(page_id)) {

			//Case B: add this log record to exist REDO log 
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			//TODO: add to exist REDO log
			add_REDO_log_arr(pop, pfree_block, rec_arr, arr_size);
			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}
		else if (pfree_block->state == PMEM_FREE_BLOCK) {
			//Case C: create the place-holder	and add the log
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			pfree_block->state = PMEM_PLACE_HOLDER_BLOCK;

			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}

	} //end for
	
	if (pblock == NULL){
		//(2.1) The dirty page is not in PB-NVM, create the place-holder
	}



	return PMEM_SUCCESS;
}

/*
 * Add the array of pointers (each point to a REDO log record) into a PMEM_BUF
 * Important: The input log record array is sorted by lsn
 * When insert item in log record array to pmem log list, we start from the head and remember the inserted position for the next insert
 * */
void 
add_REDO_log_arr(
		PMEMobjpool*	pop,
		PMEM_BUF_BLOCK*	pblock,
	   	MEM_LOG_REC**	rec_arr,
		uint64_t		arr_size){
		
		ulint i;	
		MEM_LOG_REC* memrec;
		PMEM_LOG_REC* cur_insert;

		assert(pblock);

		PMEM_LOG_LIST* plog_list = D_RW(pblock->log_list);

		pmemobj_rwlock_wrlock(pop, &plog_list->lock);

		cur_insert = plog_list->head;

		for (i = 0; i < arr_size; i++){
			PMEM_LOG_REC* pmemrec;

			memrec = rec_arr[i];
			//(1) Allocate the pmem log record from in-mem log record
			pmemrec = alloc_pmemrec_from_memrec(pop,
					memrec,
					PMEM_REDO_LOG);

			// (2) Add the pmem log record to the list, start from the cur_insert
			if (plog_list->n_items == 0){
				//The first item
				plog_list->head = plog_list->tail = pmemrec;
				plog_list->n_items++;
				cur_insert = plog_list->head;
			}
			else{
				//Insert from the cur_insert
				while(cur_insert != NULL){
					if (cur_insert->lsn > pmemrec->lsn){
						if(cur_insert->prev == NULL){
							//insert in head
							pmemrec->next = cur_insert;
							pmemrec->prev = cur_insert->prev;
							cur_insert->prev = pmemrec;
							plog_list->head = pmemrec;
						}
						else {
							//insert between cur_insert and its prev
							pmemrec->next = cur_insert;
							pmemrec->prev = cur_insert->prev;
							cur_insert->prev->next = pmemrec;
							cur_insert->prev = pmemrec;
						}
						break;
					}
					//next log record in the list
					cur_insert = cur_insert->next;
				} //end while
				if (cur_insert == NULL){
					//insert in tail
					pmemrec->next = NULL;
					pmemrec->prev = plog_list->tail;
					plog_list->tail->next = pmemrec;
					plog_list->tail = pmemrec;

					cur_insert = plog_list->tail;
				}
			}
			//next rec in the record array, we remember the cur_insert in the pmem log_list so we don't need to restart searching from the head
		}//end for
		pmemobj_rwlock_unlock(pop, &plog_list->lock);


}
/*
 * Allocate the pmem log record from in-mem log record
 * */
PMEM_LOG_REC* alloc_pmemrec_from_memrec(
		PMEMobjpool*	pop,
		MEM_LOG_REC*	mem_rec,
		PMEM_LOG_TYPE	type
		)
{

	TOID(char) byte_array;
	byte* plog_data;
	TOID(PMEM_LOG_REC) pmem_rec;
	PMEM_LOG_REC* ppmem_rec;

	//Allocate the pmem log record
	TX_BEGIN(pop) {
		POBJ_ZNEW(pop, &pmem_rec, PMEM_LOG_REC);
		ppmem_rec = D_RW(pmem_rec);

		//Copy data
		ppmem_rec->type = type;
		ppmem_rec->pid.copy_from(mem_rec->pid);
		ppmem_rec->lsn = mem_rec->lsn;
		ppmem_rec->size = mem_rec->size;

		POBJ_ALLOC(pop,
			   	&byte_array,
			   	char,
			   	sizeof(*D_RW(byte_array)) * ppmem_rec->size,
				NULL,
				NULL);
		ppmem_rec->log_data = byte_array.oid;	

		plog_data = static_cast<byte*> (pmemobj_direct(ppmem_rec->log_data));

		pmemobj_memcpy_persist(pop, plog_data, mem_rec->mem_addr, mem_rec->size);
		//Add to the list
	}TX_ONABORT {
	}TX_END

	return ppmem_rec;
}

