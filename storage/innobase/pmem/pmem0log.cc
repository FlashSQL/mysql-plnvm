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

///////////////////////// ALLOC / FREE ///////////////
/*
 * Init the Dirty Page Table as the hashtable with n buckets
 * */
MEM_DPT* alloc_DPT(uint64_t n) {
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

void
free_DPT(MEM_DPT* dpt) 
{
	ulint i;

	for (i = 0; i < dpt->n; i++) {
		dpt->buckets[i] = NULL;
	}
	free (dpt->buckets);
	dpt->buckets = NULL;

	dpt->n = 0;
	free (dpt);
	dpt = NULL;
}
/*
 * Init the Transaction Table as the hashtable with n buckets
 * */
MEM_TT* alloc_TT(uint64_t n) {
	ulint i;
	MEM_TT* tt;

	tt = (MEM_TT*) malloc(sizeof(MEM_TT));
	tt->n = n;
	tt->buckets = (MEM_TT_ENTRY**) calloc(
			n, sizeof(MEM_TT_ENTRY*));
	for (i = 0; i < n; i++)
		tt->buckets[i] = NULL;

	return tt;
}
void
free_TT(MEM_TT* tt)
{

	ulint i;
	for (i = 0; i < tt->n; i++)
		tt->buckets[i] = NULL;
	free(tt->buckets);

	tt->n = 0;
	free (tt);
	tt = NULL;
}

/*
 * Allocate a dpt_entry
 * */
MEM_DPT_ENTRY*
alloc_dpt_entry(
		page_id_t pid,
		uint64_t start_lsn){

		MEM_DPT_ENTRY* new_entry = (MEM_DPT_ENTRY*) malloc(sizeof(MEM_DPT_ENTRY));	

		new_entry->id.copy_from(pid);
		new_entry->curLSN = start_lsn;
		new_entry->next = NULL;

		mutex_create(LATCH_ID_PL_DPT_ENTRY, &new_entry->lock);

		//new list and add the rec as the first item
		new_entry->list = (MEM_LOG_LIST*) malloc(sizeof(MEM_LOG_LIST));
		new_entry->list->head = new_entry->list->tail = NULL;
		new_entry->list->n_items = 0;

		return new_entry;
}

void
free_dpt_entry( MEM_DPT_ENTRY* entry)
{
	entry->list->head = entry->list->tail = NULL;
	entry->list->n_items = 0;
	free (entry->list);
	entry->list = NULL;
	
	mutex_destroy(&entry->lock);

	entry->next = NULL;
	free (entry);
	entry = NULL;
}

MEM_TT_ENTRY*
alloc_tt_entry(uint64_t tid){

	MEM_TT_ENTRY* new_entry = (MEM_TT_ENTRY*) malloc(sizeof(MEM_TT_ENTRY));	

	new_entry->tid = tid;
	new_entry->next = NULL;

	mutex_create(LATCH_ID_PL_TT_ENTRY, &new_entry->lock);

	//new list
	new_entry->list = (MEM_LOG_LIST*) malloc(sizeof(MEM_LOG_LIST));
	new_entry->list->head = new_entry->list->tail = NULL;
	new_entry->list->n_items = 0;

	//local dpt
	new_entry->local_dpt = alloc_DPT(MAX_DPT_ENTRIES);

}

void
free_tt_entry(
	   	MEM_TT_ENTRY* entry)
{
	entry->list->head = entry->list->tail = NULL;
	entry->list->n_items = 0;
	free (entry->list);
	entry->list = NULL;

	mutex_destroy(&entry->lock);

	entry->next = NULL;
	free (entry);
	entry = NULL;
}

MEM_LOG_REC* 
pmemlog_alloc_memrec(
		byte*				mem_addr,
		uint64_t			size,
		page_id_t			pid,
		uint64_t			tid
		)
{
	MEM_LOG_REC* memrec;

	memrec = (MEM_LOG_REC*) malloc(sizeof(MEM_LOG_REC));

	memrec->size = size;
	memrec->pid.copy_from(pid);
	memrec->tid = tid;

	memrec->dpt_next = NULL;
	memrec->dpt_prev = NULL;

	memrec->tt_next = NULL;
	memrec->tt_prev = NULL;
	memrec->trx_page_next = NULL;
	memrec->trx_page_prev = NULL;

	memrec->is_in_pmem = false;

	memrec->mem_addr = (byte*) calloc(size, sizeof(byte));
	memcpy(memrec->mem_addr, mem_addr, size);
	
	return memrec;
}
void
free_memrec(
		MEM_LOG_REC* memrec
		)
{
	assert(memrec);
	memrec->dpt_next = NULL;
	memrec->dpt_prev = NULL;

	memrec->tt_next = NULL;
	memrec->tt_prev = NULL;
	memrec->trx_page_next = NULL;
	memrec->trx_page_prev = NULL;

	memrec->is_in_pmem = false;

	free (memrec->mem_addr);
	memrec->mem_addr = NULL;
	free (memrec);

}
// ////////////////// END ALLOC / FREE ///////////////
//

//////////////////////// CONNECT WITH InnoDB//////////

/////////////// END CONNECT WITH InnoDB /////////////


//////////////////// Dirty Page Table functions//////////

/*
 * Add a log record into the dirty page table.
 * The same log record has added in Transaction Table
 * dpt (in): dirty page table pointer
 * rec (in/out): log record. If is_local_dpt is set, output is rec->lsn is set and corresponding pointers in rec are linked
 * is_local_dpt (in)
 * true: insert log record into the local dpt of transaction
 * false: insert log recrod into the global dpt
 * */
void add_log_to_DPT(
		PMEMobjpool*	pop,
		MEM_DPT* dpt,
	   	MEM_LOG_REC* rec,
		bool is_local_dpt){
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
			if (is_local_dpt)
				add_log_to_local_DPT_entry(pop, bucket, rec); 
			else 
				add_log_to_global_DPT_entry(pop, bucket, rec);
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the dirty page is not exist
	if (bucket == NULL){
		//(2.2.1) new bucket 
		MEM_DPT_ENTRY* new_entry = alloc_dpt_entry(rec->pid, 0);

		if (is_local_dpt)
			add_log_to_local_DPT_entry(pop, new_entry, rec); 
		else 
			add_log_to_global_DPT_entry(pop, new_entry, rec);


		//append the new_entry in the hashed line
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
 Generate next lsn and assign rec->lsn = lsn + 1
Output: dpt_next, dpt_prev pointers changed
Caller: add_log_to_DPT() with is_local_dpt = false
 * */
void
add_log_to_global_DPT_entry(
		PMEMobjpool*	pop,
		MEM_DPT_ENTRY* entry,
	   	MEM_LOG_REC* rec)
{
	MEM_LOG_REC*	it;
	
	assert(entry);
	assert(rec);

	mutex_enter(&entry->lock);
	//pmemobj_rwlock_wrlock(pop, &entry->pmem_lock);
	// (1) Generate the LSN
	rec->lsn = entry->curLSN + 1;
	entry->curLSN = rec->lsn;

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
//	printf("+++ add log to global dpt_entry page_no %zu space_no %zu tid %zu curLSN %zu rec->lsn %zu list->n_items %zu\n",
//			rec->pid.page_no(), rec->pid.space(), rec->tid, entry->curLSN, rec->lsn, entry->list->n_items );
#endif
	// (2) insert the log record to the list
	if (entry->list->n_items == 0){
		entry->list->head = entry->list->tail = rec;
		++entry->list->n_items;

		mutex_exit(&entry->lock);
		//pmemobj_rwlock_unlock(pop, &entry->pmem_lock);

		return;
	}
	//since the last rec always has max lsn, we only append on the list
	assert(rec->lsn > entry->list->tail->lsn);
	entry->list->tail->dpt_next = rec;
	rec->dpt_prev = entry->list->tail;
	entry->list->tail = rec;
	rec->dpt_next = NULL;

	entry->list->n_items++;
	mutex_exit(&entry->lock);
	//pmemobj_rwlock_unlock(pop, &entry->pmem_lock);
}
/*
 *Add a log record to a local DPT entry of a transaction (as sorted doubled linked list)
 Does not enerate next lsn
Output: trx_page_next and trx_page_prev pointers changed
Caller: add_log_to_DPT() with is_local_dpt = true
 * */
void
add_log_to_local_DPT_entry(
		PMEMobjpool*	pop,
		MEM_DPT_ENTRY* entry,
	   	MEM_LOG_REC* rec)
{
	MEM_LOG_REC*	it;
	uint64_t		lsn;
	
	assert(entry);
	assert(rec);

	mutex_enter(&entry->lock);
	//pmemobj_rwlock_wrlock(pop, &entry->pmem_lock);

	// (1) insert the log record to the list
	if (entry->list->n_items == 0){
		entry->list->head = entry->list->tail = rec;
		entry->list->n_items++;

		mutex_exit(&entry->lock);
		//pmemobj_rwlock_unlock(pop, &entry->pmem_lock);

		return;
	}
	//append
	entry->list->tail->trx_page_next = rec;
	rec->trx_page_prev = entry->list->tail;
	entry->list->tail = rec;
	rec->trx_page_next = NULL;

	entry->list->n_items++;
	mutex_exit(&entry->lock);
	//pmemobj_rwlock_unlock(pop, &entry->pmem_lock);
}
//////////////////// End Dirty Page Table functions//////////


//////////////////// Transaction Table functions//////////
/*
 * Remove a DPT entry from the global DPT
 * Call this function when flush a page
 * */
void
remove_dpt_entry(
		MEM_DPT* global_dpt,
		MEM_DPT_ENTRY* entry,
		MEM_DPT_ENTRY* prev_entry,
		ulint hashed)
{
	//(1) remove the entry from the hashed line
	if (prev_entry == NULL){
		global_dpt->buckets[hashed] = entry->next;
	}
	else{
		prev_entry->next = entry->next;
	}
	entry->next = NULL;

	// (2) Remove dpt_next, dpt_prev pointers of each log record in the entry and change is_in_pmem to true
	adjust_dpt_entry_on_flush(entry);

	// (3) Free resource of the entry	
	free_dpt_entry(entry);	
}

/*
 * Remove a transation entry when the corresponding transaction commit
 * tt (in): The global transaction table
 * entry (in): The tt entry to be removed
 * prev_entry (in): The tt entry right before the entry
 * hashed (in): Hashed value of the hashed line consists the entry
 *
 * Caller: trx_commit_TT()
 * */
void 
remove_TT_entry(
		MEM_TT* tt,
		MEM_DPT* global_dpt,
	   	MEM_TT_ENTRY* entry,
	   	MEM_TT_ENTRY* prev_entry,
		ulint hashed)
{
	MEM_DPT*			local_dpt;
	MEM_DPT_ENTRY*		local_dpt_entry;
	MEM_DPT_ENTRY*		prev_dpt_entry;
	ulint				i;

	local_dpt = entry->local_dpt;

	//(1) Remove entry out of the hashed line
	if (prev_entry != NULL){
		prev_entry->next = entry->next;
		entry->next = NULL;
	}
	else {
		//entry is the first item in the hashed line
		tt->buckets[hashed] = entry->next;
		entry->next = NULL;
	}

	//(2) Remove local DPT of the entry
	//(2.1) for each hashed line in the local DPT 
	for (i = 0; i < local_dpt->n; i++){
		//Get a hashed line
		local_dpt_entry = local_dpt->buckets[i];
		if (local_dpt_entry == NULL) 
			continue;
		assert(local_dpt_entry);
		//for each entry in the hashed line
		while (local_dpt_entry != NULL){
			/////////////////////
			//remove log records and their pointers in this dpt_entry
			//The pointers are removed from: (1) local dpt, (2) global dpt, and (3) tt entry
			remove_logs_on_remove_local_dpt_entry(global_dpt, local_dpt_entry);
			////////////////////////////////////
			
			//remove the dpt entry from the dpt hashed line
			prev_dpt_entry = local_dpt_entry;
			local_dpt_entry = local_dpt_entry->next;
			//free resource
			free_dpt_entry(prev_dpt_entry);
		}

		//Until this point, all dpt_entry in the hashed line is removed. Now, set the hashed line to NULL
		local_dpt->buckets[i] = NULL;
		//next hashed line
	}//end for each hashed line

	//(2.2) Until this point, all hashed lines in the local DPT are removed.
	free_DPT(entry->local_dpt);

	//(4) Free the tt log list
	free_tt_entry(entry);	
}
/*
 * Add a log record into the transaction table
 * Call this function when a transaction generate the log record in DRAM
 * (1) Add log record rec to the global DPT, result is rec->lsn is assigned
 * (2) Add log record rec to transaction entry, based on rec->tid, the corresponding transaction entry e is added
 * (3) Add log record to the local DPT of e 
 * */
void 
pmemlog_add_log_to_TT	(
				PMEMobjpool*	pop,
				MEM_TT* tt,
				MEM_DPT* dpt,
			   	MEM_LOG_REC* rec){

	ulint hashed;
	MEM_TT_ENTRY* bucket;
	MEM_TT_ENTRY* prev_bucket;

	//(1) Add log record to the global DPT, after the function completed, rec->lsn is assinged to next lsn in page
	add_log_to_DPT(pop, dpt, rec, false);

	//(2) Add log record to transaction entry
	//Get the hash by transaction id
	PMEM_LOG_HASH_KEY(hashed, rec->tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	prev_bucket = NULL;
	

	//Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->tid == rec->tid){
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
		//printf("++0 add rec to existed TT entry tid %zu n_items %zu\n", rec->tid, bucket->list->n_items);
#endif
			//(2.1) insert the log record into this bucket
			add_log_to_TT_entry(bucket, rec); 

			// (3) Add log record to the local DPT of trx entry
			add_log_to_DPT(pop, bucket->local_dpt, rec, true);
			return;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}
	// (2.2) the transaction entry (bucket) is not exist
	if (bucket == NULL){
		// new bucket 
		MEM_TT_ENTRY* new_entry = alloc_tt_entry(rec->tid);	
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
		printf("+++ add rec to new TT entry tid %zu \n", rec->tid);
#endif
		add_log_to_TT_entry(new_entry, rec); 
		// (3) Add log record to the local DPT of trx entry
		add_log_to_DPT(pop, new_entry->local_dpt, rec, true);

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
 *Add a log record to a transacton entry in the transaction table
Caller: add_log_to_TT
 * */
void
add_log_to_TT_entry(
	   	MEM_TT_ENTRY* entry,
	   	MEM_LOG_REC* rec){

	MEM_LOG_LIST*	list;

	assert(entry);
	assert(rec);
	//we don't need to acquire the lock because only this transaction can touch the entry
	mutex_enter(&entry->lock);
	//(1) Insert the log record at the tail of the list
	list = entry->list;
	//If the list is empty
	if (list->n_items == 0){
		list->head = list->tail = rec;
		rec->tt_next = rec->tt_prev = NULL;
		list->n_items++;
		mutex_exit(&entry->lock);
		return;
	}
	
	list->tail->tt_next = rec;
	rec->tt_prev = list->tail;
	list->tail = rec;
	rec->tt_next = NULL;

	list->n_items++;
	mutex_exit(&entry->lock);
	return;
}
/*
 * Handle changes in the transaction table when a transaction tid commit 
 * pop (in): The global pop
 * buf (in): The global buf
 * global_dpt (in): The global dpt
 * tt (in): The global tt
 * tid (in): Transaction id
 * */
int
pmemlog_trx_commit(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
	   	uint64_t		tid)
{
	ulint			hashed;
	ulint			i;
	MEM_TT_ENTRY*	bucket;
	
	MEM_DPT*		local_dpt;	
	MEM_DPT_ENTRY*	local_dpt_entry;

	//For remove
	MEM_TT_ENTRY*	prev_bucket; // for remove
	MEM_DPT_ENTRY*	prev_dpt_entry;
	MEM_LOG_LIST*	log_list;
	MEM_LOG_REC*	memrec;
	MEM_LOG_REC*	memrec_next;

	MEM_DPT*		global_dpt = buf->dpt;
	MEM_TT*			tt = buf->tt;

	//(1) Get transaction entry by the input tid 
	PMEM_LOG_HASH_KEY(hashed, tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	prev_bucket = NULL;
	
	while (bucket != NULL){
		if (bucket->tid == tid){
			break;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}

	if (bucket == NULL){
		printf("PMEM_LOG Error in trx_commit_TT(), tid %zu not found in transaction table\n", tid);
		return PMEM_ERROR;
	}

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("COMMIT ===> tid %zu commit\n", tid);
#endif

	//(2) For each DPT entry in the local dpt, write REDO log to corresponding page	
	local_dpt = bucket->local_dpt;
	assert(local_dpt);
	for (i = 0; i < local_dpt->n; i++){
		//Get a hashed line
		local_dpt_entry = local_dpt->buckets[i];
		if (local_dpt_entry == NULL) 
			continue;
		assert(local_dpt_entry);
		//for each entry in the same hashed line
		while (local_dpt_entry != NULL){
			pm_write_REDO_logs(pop, buf, local_dpt_entry);

			local_dpt_entry = local_dpt_entry->next;
		}
		//next hashed line
	}//end for each hashed line

	//(3) Remove tt entry and its corresponding resources
	remove_TT_entry (tt, global_dpt, bucket, prev_bucket, hashed);

	
}

/*
 * Handle changes in the transaction table when a transaction tid abort
 * pop (in): The global pop
 * buf (in): The global buf
 * global_dpt (in): The global dpt
 * tt (in): The global tt
 * tid (in): Transaction id
 * */
int
pmemlog_trx_abort(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
	   	uint64_t		tid)
{
	ulint			hashed;
	ulint			i;
	MEM_TT_ENTRY*	bucket;
	
	MEM_DPT*		local_dpt;	
	MEM_DPT_ENTRY*	dpt_entry;

	//For remove
	MEM_TT_ENTRY*	prev_bucket; // for remove
	MEM_DPT_ENTRY*	prev_dpt_entry;
	MEM_LOG_LIST*	log_list;
	MEM_LOG_REC*	memrec;
	MEM_LOG_REC*	memrec_next;
	
	MEM_DPT*		global_dpt = buf->dpt;
	MEM_TT*			tt = buf->tt;

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("ABORT ===> pmemlog_trx_abort tid %zu abort \n", tid);
#endif 
	//(1) Get transaction entry by the input tid 
	PMEM_LOG_HASH_KEY(hashed, tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	prev_bucket = NULL;
	
	while (bucket != NULL){
		if (bucket->tid == tid){
			break;
		}
		prev_bucket = bucket;
		bucket = bucket->next;
	}

	if (bucket == NULL){
		printf("PMEM_LOG Error in trx_abort_TT(), tid %zu not found in transaction table\n", tid);
		return PMEM_ERROR;
	}

	//(2) For each DPT entry in the local dpt, UNDOing to corresponding page
	
	local_dpt = bucket->local_dpt;
	assert(local_dpt);
	for (i = 0; i < local_dpt->n; i++){
		//Get a hashed line
		dpt_entry = local_dpt->buckets[i];
		if (dpt_entry == NULL) 
			continue;
		assert(dpt_entry);
		//for each entry in the same hashed line
		while (dpt_entry != NULL){
			MEM_LOG_REC* first_memrec;
			first_memrec = dpt_entry->list->head;
			assert(first_memrec);

			if(first_memrec->is_in_pmem){
				//TODO: UNDOing a page in NVM
			}
			else{
				//TODO: UNDOing a page in DRAM
			}

			dpt_entry = dpt_entry->next;
		}
		//next hashed line
	}//end for each hashed line
	//(3) Remove tt entry and its corresponding resources
	remove_TT_entry (tt, global_dpt, bucket, prev_bucket, hashed);

}

/*
 * write per-page REDO log records when a transaction commits
 * pop: global pop
 * buf: global PMEM_BUF wrapper
 * dpt_entry (in): local dpt_entry of the commited transaction
 *
 * caller: trx_commit_TT() 
 * This function use some part of pm_buf_write_with_flusher(), but it doesn't actually write data, just create the place-holder
 * */
int
pm_write_REDO_logs(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
		MEM_DPT_ENTRY*	dpt_entry
	   	) 
{
	ulint hashed;
	ulint i;
		
	TOID(PMEM_BUF_BLOCK_LIST) hash_list;
	PMEM_BUF_BLOCK_LIST* phashlist;
	PMEM_BUF_BLOCK* pblock;

	TOID(PMEM_BUF_BLOCK) free_block;
	PMEM_BUF_BLOCK* pfree_block;
	
	page_id_t page_id(dpt_entry->id);

	//(1) Get the bucket that has the page
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	PMEM_LESS_BUCKET_HASH_KEY(hashed,page_id.space(), page_id.page_no());
#else //EVEN_BUCKET
	PMEM_HASH_KEY(hashed, page_id.fold(), buf->PMEM_N_BUCKETS);
#endif

retry:
	TOID_ASSIGN(hash_list, (D_RW(buf->buckets)[hashed]).oid);

	phashlist = D_RW(hash_list);
	assert(phashlist);

	pmemobj_rwlock_wrlock(pop, &phashlist->lock);
	
	// (1) If the current hash list is flushing wait and retry
	if (phashlist->is_flush) {
		if (buf->is_recovery	&&
			(phashlist->cur_pages >= phashlist->max_pages * buf->PMEM_BUF_FLUSH_PCT)) {
			pmemobj_rwlock_unlock(pop, &phashlist->lock);
			pm_buf_handle_full_hashed_list(pop, buf, hashed);
			goto retry;
		}
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);

		goto retry;
	}

	//(2) Get the page in the bucket
	
	pblock = NULL;

	for (i = 0; i < D_RO(hash_list)->max_pages; i++) {
		pfree_block = D_RW(D_RW(phashlist->arr)[i]);
		if(pfree_block->state == PMEM_IN_USED_BLOCK &&
				pfree_block->id.equals_to(page_id)) {

			//Case A: Directly apply REDO log to the page and remove duplicate UNDO log records
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);
			pm_write_REDO_logs_to_pmblock(pop, pfree_block, dpt_entry); 	
			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}
		else if(pfree_block->state == PMEM_PLACE_HOLDER_BLOCK &&
				pfree_block->id.equals_to(page_id)) {

			//Case B: add this log record to exist REDO log			   
			pmemobj_rwlock_wrlock(pop, &pfree_block->lock);

			//pm_merge_REDO_logs_to_placeholder(pop, pfree_block, dpt_entry);
			pm_merge_logs_to_loglist(
					pop,
					D_RW(pfree_block->redolog_list),
					dpt_entry,
					PMEM_REDO_LOG,
					false);


			pmemobj_rwlock_unlock(pop, &pfree_block->lock);
			return PMEM_SUCCESS;
		}
		else if (pfree_block->state == PMEM_FREE_BLOCK) {
			//Case C: create the place-holder	and add the log
			break;
		}

	} //end for
	if ( i == phashlist->max_pages ) {
		//ALl blocks in this hash_list are either non-fre or locked
		//This is rarely happen but we still deal with it
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		os_event_wait(buf->flush_events[hashed]);
		goto retry;
	}
	//Case C: create the place-holder	and add the log
	//
	pmemobj_rwlock_wrlock(pop, &pfree_block->lock);

	// This code for recovery	
	fil_node_t*			node;
	node = pm_get_node_from_space(page_id.space());
	if (node == NULL) {
		printf("PMEM_ERROR node from space is NULL\n");
		assert(0);
	}
	strcpy(pfree_block->file_name, node->name);
	// end code
	// Handle similar to write to a new pmem block
	pfree_block->id.copy_from(page_id);

	assert(pfree_block->state == PMEM_FREE_BLOCK);
	pfree_block->state = PMEM_PLACE_HOLDER_BLOCK;

	//pm_merge_REDO_logs_to_placeholder(pop, pfree_block, dpt_entry);
	pm_merge_logs_to_loglist(
			pop,
			D_RW(pfree_block->redolog_list),
			dpt_entry,
			PMEM_REDO_LOG,
			false);
	pmemobj_rwlock_unlock(pop, &pfree_block->lock);
	
	//Create a place-holder also increase the cur_pages
	++(phashlist->cur_pages);

// HANDLE FULL LIST ////////////////////////////////////////////////////////////
	if (phashlist->cur_pages >= phashlist->max_pages * buf->PMEM_BUF_FLUSH_PCT) {
		//(3) The hashlist is (nearly) full, flush it and assign a free list 
		phashlist->hashed_id = hashed;
		phashlist->is_flush = true;
		//block upcomming writes into this bucket
		os_event_reset(buf->flush_events[hashed]);
		
#if defined (UNIV_PMEMOBJ_BUF_STAT)
	++buf->bucket_stats[hashed].n_flushed_lists;
#endif 

		pmemobj_rwlock_unlock(pop, &phashlist->lock);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n[1] BEGIN pm_buf_handle_full list_id %zu, hashed_id %zu\n", phashlist->list_id, hashed);
#endif 
		pm_buf_handle_full_hashed_list(pop, buf, hashed);
#if defined(UNIV_PMEMOBJ_BUF_RECOVERY_DEBUG)
		printf("\n[1] END pm_buf_handle_full list_id %zu, hashed_id %zu\n", phashlist->list_id, hashed);
#endif 

		//unblock the upcomming writes on this bucket
		os_event_set(buf->flush_events[hashed]);
	}
	else {
		//unlock the hashed list
		pmemobj_rwlock_unlock(pop, &phashlist->lock);
		//pmemobj_rwlock_unlock(pop, &D_RW(D_RW(buf->buckets)[hashed])->lock);
	}
	return PMEM_SUCCESS;
}
/*
 * Merge REDO/UNDO log records into a pmem log list
 * Important: The input log records are sorted by lsn
 * We start from the head and remember the inserted position for the next insert
 *
 * case A: merge REDO log when commit
 *		plog_list is REDO log
 *		type: PMEM_REDO_LOG
 *		is_global_dpt = false
 * case B: merge UNDO log when flush
 *		plog_list is UNDO log
 *		type: PMEM_UNDO_LOG
 *		is_global_dpt = true
 * */
void 
pm_merge_logs_to_loglist(
		PMEMobjpool*	pop,
		PMEM_LOG_LIST*	plog_list,
	   	MEM_DPT_ENTRY*	dpt_entry,
		PMEM_LOG_TYPE	type,
		bool			is_global_dpt)
{
		ulint i;	
		ulint count;
		MEM_LOG_REC* memrec;
		TOID(PMEM_LOG_REC) cur_insert;
		
		assert(plog_list);
		
		mutex_enter(&dpt_entry->lock);

		TOID_ASSIGN(cur_insert, (plog_list->head).oid);
		memrec = dpt_entry->list->head;
		
		count = 0;
		while(memrec != NULL){
			TOID(PMEM_LOG_REC) pmemrec;
			//(1) Allocate the pmem log record from in-mem log record
			pmemrec = alloc_pmemrec(pop, memrec, type);

			// (2) Add the pmem log record to the list, start from the cur_insert
			if (plog_list->n_items == 0){
				//The first item
				TOID_ASSIGN(plog_list->head, pmemrec.oid);
				TOID_ASSIGN(plog_list->tail, pmemrec.oid);
				TOID_ASSIGN(cur_insert, (plog_list->head).oid);
			}
			else{
				//Insert from the cur_insert
				while (! TOID_IS_NULL(cur_insert)){
					if (D_RW(cur_insert)->lsn > D_RW(pmemrec)->lsn){
						if (TOID_IS_NULL(D_RW(cur_insert)->prev)){
							//insert in head
							TOID_ASSIGN( D_RW(pmemrec)->next, cur_insert.oid);
							TOID_ASSIGN( D_RW(pmemrec)->prev, ( D_RW(cur_insert)->prev).oid);
							TOID_ASSIGN( D_RW(cur_insert)->prev, pmemrec.oid);
							TOID_ASSIGN( plog_list->head, pmemrec.oid);
							
							//pmemrec->next = cur_insert;
							//pmemrec->prev = cur_insert->prev;
							//cur_insert->prev = pmemrec;
							//plog_list->head = pmemrec;
						}
						else {
							//insert between cur_insert and its prev
							TOID_ASSIGN( D_RW(pmemrec)->next, cur_insert.oid);
							TOID_ASSIGN( D_RW(pmemrec)->prev, (D_RW(cur_insert)->prev).oid);
							TOID_ASSIGN( D_RW(D_RW(cur_insert)->prev)->next, pmemrec.oid);
							TOID_ASSIGN( D_RW(cur_insert)->prev, pmemrec.oid);
							//pmemrec->next = cur_insert;
							//pmemrec->prev = cur_insert->prev;
							//cur_insert->prev->next = pmemrec;
							//cur_insert->prev = pmemrec;
						}

						break;
					}
					//next log record in the list
					TOID_ASSIGN(cur_insert, (D_RW(cur_insert)->next).oid);
					//cur_insert = cur_insert->next;
				} //end while
				if (TOID_IS_NULL(cur_insert)){
					//insert in tail
					TOID_ASSIGN( D_RW(plog_list->tail)->next, pmemrec.oid);
					TOID_ASSIGN( D_RW(pmemrec)->prev, (plog_list->tail).oid);
					TOID_ASSIGN( plog_list->tail, pmemrec.oid);
					TOID_ASSIGN( D_RW(pmemrec)->next, OID_NULL);

					TOID_ASSIGN( cur_insert, (plog_list->tail).oid);

					//pmemrec->next = NULL;
					//pmemrec->prev = plog_list->tail;
					//plog_list->tail->next = pmemrec;
					//plog_list->tail = pmemrec;

					//cur_insert = plog_list->tail;
				}

			}//end else
			plog_list->n_items++;
			count++;	

			if (is_global_dpt){
				//next rec in the local dpt entry
				memrec = memrec->dpt_next;
			}
			else {
				//next rec in the local dpt entry
				memrec = memrec->trx_page_next;
			}

		}//end while
		
		if (count != dpt_entry->list->n_items){
			printf("PMEM_ERROR, pm_merges count = %zu differs from nitems %zu\n", count, dpt_entry->list->n_items);
			assert(0);
		}
		//plog_list->n_items = plog_list->n_items + count;

		mutex_exit(&dpt_entry->lock);
}

/*
 * Write REDO logs from a dpt entry into a pmem block when a transaction commit
 * pop (in): global pop
 * pblock (in): The pmem block
 * dpt_entry (in): The dpt entry that consists of REDO logs
 * this write reduce the total number of log records in UNDO log
 *
 * */
void
pm_write_REDO_logs_to_pmblock(
		PMEMobjpool*	pop,
		PMEM_BUF_BLOCK*	pblock,
	   	MEM_DPT_ENTRY*	dtp_entry
	   	)
{
	MEM_LOG_REC* memrec;
	PMEM_LOG_REC* pmemrec;
	PMEM_LOG_LIST* pundo_log_list;
	//when the pblock is a pmem page, the log_list is UNDO log
	pundo_log_list = D_RW(pblock->undolog_list);

	memrec = dtp_entry->list->head;

	while (memrec != NULL){
		if (memrec->is_in_pmem){
			//remove the corresponding log record in UNDO list
			pm_remove_UNDO_log_from_list(pop, pundo_log_list, memrec);
		}
		else{
			//TODO: directly apply REDO log to the pmem page
		}
		//next rec in the local dpt entry
		memrec = memrec->trx_page_next;
	}

}
/*
 * Remove an UNDO log record from the list
 * When a transaction commit, if a REDO log record has the associated UNDO log record in NVM, we remove the UNDO log record and doesn't apply the REDO log
 * */
void
pm_remove_UNDO_log_from_list(
		PMEMobjpool*	pop,
		PMEM_LOG_LIST* list,
		MEM_LOG_REC* memrec)
{
	TOID(PMEM_LOG_REC) pmemrec;
	PMEM_LOG_REC* ppmemrec;

	TOID_ASSIGN(pmemrec, list->head.oid) ;
	ppmemrec = D_RW(pmemrec);

	assert(ppmemrec);
	assert(ppmemrec->type == PMEM_UNDO_LOG);
	assert(memrec->is_in_pmem);

	while (ppmemrec != NULL){
		if (ppmemrec->lsn == memrec->lsn){
			//found, remove pmemrec from the list
			if ( !TOID_IS_NULL(ppmemrec->prev)) {
				TOID_ASSIGN( D_RW(ppmemrec->prev)->next,
						(ppmemrec->next).oid);
				if (! TOID_IS_NULL(ppmemrec->next))
					TOID_ASSIGN( D_RW(ppmemrec->next)->prev,
						(ppmemrec->prev).oid);
			}
			else {
				//pmem rec is the first UNDO Log record
				TOID_ASSIGN(list->head, (ppmemrec->next).oid);
				if (! TOID_IS_NULL(ppmemrec->next))
				TOID_ASSIGN( D_RW(ppmemrec->next)->prev, OID_NULL);
			}

			TOID_ASSIGN(ppmemrec->next, OID_NULL);
			TOID_ASSIGN(ppmemrec->prev, OID_NULL);

			free_pmemrec(pop, pmemrec);

			list->n_items--;
			return;	
		}
		//next pmem rec
		TOID_ASSIGN( pmemrec, (D_RW(pmemrec)->next).oid);
		ppmemrec = D_RW(pmemrec);
	} //end while

	// If memrec->is_in_pmem, its corresponding pmemrec must exist in the UNDO log list
	printf("PMEM_ERROR in pm_remove_UNDO_log_from_list, if memrec->is_in_pmem is true, its corresponding pmemrec must exist in the UNDO log list\n");
	assert(0);
}

/*
 * Remove all log records in REDO log list.
 * When flush a page, if that page has associate place-holder in NVM, we remove the REDO log list of that place-holder
 * */
void
pm_remove_REDO_log_list_when_flush(
		PMEMobjpool*	pop,
		PMEM_LOG_LIST* list) 
{
	TOID(PMEM_LOG_REC)	pmemrec;
	TOID(PMEM_LOG_REC)	pmemrec_next;
	PMEM_LOG_REC*		ppmemrec;	
	TOID_ASSIGN(pmemrec, list->head.oid);

	ppmemrec = D_RW(pmemrec);

	assert(ppmemrec);
	assert(ppmemrec->type == PMEM_REDO_LOG);

	while (ppmemrec != NULL){
		TOID_ASSIGN(pmemrec_next, (ppmemrec->next).oid);

		//remove pmemrec from the list
		TOID_ASSIGN(ppmemrec->next, OID_NULL);
		TOID_ASSIGN(ppmemrec->prev, OID_NULL);
		free_pmemrec(pop, pmemrec);

		list->n_items--;
		//next pmem rec
		TOID_ASSIGN( pmemrec, pmemrec_next.oid);
		ppmemrec = D_RW(pmemrec);
	}//end while
	
	TOID_ASSIGN(list->head, OID_NULL);
	TOID_ASSIGN(list->tail, OID_NULL);

	assert(list->n_items == 0);

}

/*
 * Allocate the pmem log record from in-mem log record
 * */
TOID(PMEM_LOG_REC) alloc_pmemrec(
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

	return pmem_rec;
}

/*
 * Free the pmem log record 
 * */
void 
free_pmemrec(
		PMEMobjpool*	pop,
		TOID(PMEM_LOG_REC)	pmem_rec
		)
{

	TX_BEGIN(pop) {
		//free the log content
		pmemobj_free (&(D_RW(pmem_rec)->log_data));
		D_RW(pmem_rec)->size = 0;
		
		//free the whole log record	
		POBJ_FREE(&pmem_rec);	

	}TX_ONABORT {
	}TX_END
}

/*
 * Remove log records and all of their pointers related to a local dpt entry of a transaction when it commit
 * global_dpt (in): The global dpt, used for remove the corresponding pointers
 * entry (in): The local dpt entry of the transaction
 * */
void 
remove_logs_on_remove_local_dpt_entry(
		MEM_DPT*	global_dpt,
		MEM_DPT_ENTRY*		entry)
{
	ulint hashed;
	ulint count;

	MEM_LOG_REC*	memrec;
	MEM_LOG_REC*	next_memrec;//next log rec of memrec in the entry
	MEM_DPT_ENTRY* global_DPT_entry;
	MEM_DPT_ENTRY* bucket;

	//(1) Get DPT entry in the global DPT
	PMEM_LOG_HASH_KEY(hashed, entry->id.fold(), global_dpt->n);
	global_DPT_entry = global_dpt->buckets[hashed];
	assert(global_DPT_entry);

	while (global_DPT_entry != NULL){
		if (global_DPT_entry->id.equals_to(entry->id)){
			break;
		}
		global_DPT_entry = global_DPT_entry->next;
	}

	if (global_DPT_entry == NULL){
		printf("PMEM ERROR! Cannot find corresponding DPT entry in remove_local_DPT_entry()\n");
		assert(0);
	}
	//(2) For each log record in the local dpt entry, remove pointers
	mutex_enter(&global_DPT_entry->lock);
	count = 0;
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	if (entry->list->head != NULL)
	printf("remove_logs_on_remove_local_dpt_entry before remove local nitems %zu global nitems %zu count %zu\n",
		entry->list->n_items,
		global_DPT_entry->list->n_items,
		count);
#endif 
	memrec = entry->list->head;

	while (memrec != NULL){
		next_memrec = memrec->trx_page_next;

		//(2.1) Remove from global DPT entry
		if (memrec->dpt_prev == NULL){
			//memrec is the first log record in the global DPT entry
			global_DPT_entry->list->head = memrec->dpt_next;
		}
		else{
			memrec->dpt_prev->dpt_next = memrec->dpt_next;
		}

		if (memrec->dpt_next != NULL){
			memrec->dpt_next->dpt_prev = memrec->dpt_prev;
		}

		free_memrec(memrec);
		count++;

		//global_DPT_entry->list->n_items--;
		//entry->list->n_items--;

		memrec = next_memrec;
	}//end while
	assert (count <= global_DPT_entry->list->n_items);
	assert (count <= entry->list->n_items);
	
	global_DPT_entry->list->n_items -= count;
	entry->list->n_items -= count;


#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	if (entry->list->head != NULL)
	printf("After remove local nitems %zu global nitems %zu count %zu\n",
		entry->list->n_items,
		global_DPT_entry->list->n_items,
		count);
#endif 

	mutex_exit(&global_DPT_entry->lock);
}

/*
 * Remove pointers and adjust status of each log record in a global dpt entry
 * (dpt_next, dpt_prev)
 * */
void adjust_dpt_entry_on_flush(
		MEM_DPT_ENTRY*		dpt_entry
		)
{
	MEM_LOG_REC*		memrec;
	MEM_LOG_REC*		next_memrec;

	if (dpt_entry->list->n_items == 0){
		// Nothing to do
		return;
	}
	memrec = dpt_entry->list->head;

	while (memrec != NULL){
		next_memrec = memrec->dpt_next;
		memrec->is_in_pmem = true;
		memrec->dpt_prev = NULL;
		memrec->dpt_next = NULL;
		
		dpt_entry->list->n_items--;
		memrec = next_memrec;
	} //end while
	
	assert(dpt_entry->list->n_items == 0);

}

/*
 *Copy blocks that has remain REDO logs or UNDO logs from a src list to des list
 This function is called in pm_buf_handle_full_hashed_list() when propgatation to disk
 * */
void
pm_copy_logs_pmemlist(
		PMEMobjpool*	pop,
		PMEM_BUF*		buf,
		PMEM_BUF_BLOCK_LIST* des, 
		PMEM_BUF_BLOCK_LIST* src)
{
	ulint			i;
	ulint			count;

	PMEM_BUF_BLOCK* pblock_src;	
	PMEM_BUF_BLOCK* pblock_des;	

	TOID(PMEM_BUF_BLOCK) temp;

	PMEM_LOG_LIST* plog_list;
	PMEM_LOG_LIST* plog_list_temp;


	des->hashed_id = src->hashed_id;
	count = 0;

	for (i = 0; i < src->cur_pages; i++){
		pblock_src = D_RW(D_RW(src->arr)[i]);
		if (pblock_src->state == PMEM_IN_USED_BLOCK){
			//Ensure the REDO log is empty
			plog_list_temp = D_RW(pblock_src->redolog_list);

			assert (plog_list_temp->n_items == 0);
			assert ( TOID_IS_NULL(plog_list_temp->head));

			plog_list = D_RW(pblock_src->undolog_list);
			if (plog_list->n_items > 0){
				//swap oid
				pm_swap_blocks( D_RW(src->arr)[i],
						D_RW(des->arr)[count]);
				des->cur_pages = count = count + 1;
			}
		}
		else if(pblock_src->state == PMEM_PLACE_HOLDER_BLOCK){
			//Ensure the UNDO log is empty
			plog_list_temp = D_RW(pblock_src->undolog_list);

			assert (plog_list_temp->n_items == 0);
			assert ( TOID_IS_NULL(plog_list_temp->head));
			plog_list = D_RW(pblock_src->redolog_list);

			assert(plog_list->n_items > 0);

			pm_swap_blocks( D_RW(src->arr)[i],
					D_RW(des->arr)[count]);

			des->cur_pages = count = count + 1;
		}
	}
}
void 
pm_swap_blocks(
		TOID(PMEM_BUF_BLOCK) a,
		TOID(PMEM_BUF_BLOCK) b)
{
	TOID(PMEM_BUF_BLOCK) temp;

	TOID_ASSIGN(temp, a.oid);
	TOID_ASSIGN(a, b.oid);
	TOID_ASSIGN(b, temp.oid);
}

/*
 * Seek a dpt entry in dpt by page id
 * dpt (in): The global dpt
 * page_id (in): The seek key
 * prev_dpt_entry (out): The previous entry of the seeked entry in the hashed line
 * hashed (out): The hashed value of the hashed line
 * */
MEM_DPT_ENTRY*
seek_dpt_entry(
		MEM_DPT* dpt,
	   	page_id_t page_id,
		MEM_DPT_ENTRY* prev_dpt_entry,
		ulint*	hashed)
{
	ulint h_val;
	MEM_DPT_ENTRY*	dpt_entry;

	PMEM_LOG_HASH_KEY(h_val, page_id.fold(), dpt->n);
	dpt_entry = dpt->buckets[h_val];
	prev_dpt_entry = NULL;

	while (dpt_entry != NULL){
		if (dpt_entry->id.equals_to(page_id)){
			break;
		}
		prev_dpt_entry = dpt_entry;
		dpt_entry = dpt_entry->next;
	}
	*hashed = h_val;
	return dpt_entry;
}

MEM_TT_ENTRY*
seek_tt_entry(
		MEM_TT*		tt,
		uint64_t	tid,
		MEM_TT_ENTRY* prev_tt_entry,
		ulint*		hashed)
{
	ulint			h_val;
	MEM_TT_ENTRY*	tt_entry;	
	PMEM_LOG_HASH_KEY(h_val, tid, tt->n);
	tt_entry = tt->buckets[h_val];
	prev_tt_entry = NULL;

	while (tt_entry != NULL){
		if (tt_entry->tid == tid){
			break;
		}
		prev_tt_entry = tt_entry;
		tt_entry = tt_entry->next;
	}
	*hashed = h_val;
	return tt_entry;
}

