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

#if defined (UNIV_PMEMOBJ_PL)
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

	dpt->tails = (MEM_DPT_ENTRY**) calloc(
			n, sizeof(MEM_DPT_ENTRY*));

	//dpt->hl_locks = (ib_mutex_t*) calloc(n, sizeof(ib_mutex_t));
	dpt->pmem_locks = (PMEMrwlock*) calloc(n, sizeof(PMEMrwlock));
	for (i = 0; i < n; i++) {
		dpt->buckets[i] = NULL;
		dpt->tails[i] = NULL;
		//mutex_create(LATCH_ID_PL_DPT_ENTRY, &dpt->hl_locks[i]);
	}

	return dpt;
}

void
free_DPT(MEM_DPT* dpt) 
{
	ulint i;

	for (i = 0; i < dpt->n; i++) {
		dpt->buckets[i] = NULL;
		dpt->tails[i] = NULL;
		//mutex_destroy(&dpt->hl_locks[i]);
	}
	free (dpt->buckets);
	free (dpt->tails);
	//free (dpt->hl_locks);

	dpt->buckets = NULL;
	dpt->tails = NULL;
	//dpt->hl_locks = NULL;

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
	tt->tails = (MEM_TT_ENTRY**) calloc(
			n, sizeof(MEM_TT_ENTRY*));

	tt->pmem_locks = (PMEMrwlock*) calloc(n, sizeof(PMEMrwlock));
	for (i = 0; i < n; i++)
	{
		tt->buckets[i] = NULL;
		tt->tails[i] = NULL;
	}

	return tt;
}
void
free_TT(MEM_TT* tt)
{

	ulint i;
	for (i = 0; i < tt->n; i++)
	{
		tt->buckets[i] = NULL;
		tt->tails[i] = NULL;
	}

	free(tt->buckets);
	free(tt->tails);

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

		//mutex_create(LATCH_ID_PL_DPT_ENTRY, &new_entry->lock);

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
	
	//mutex_destroy(&entry->lock);

	entry->next = NULL;
	free (entry);
	entry = NULL;
}

MEM_TT_ENTRY*
alloc_tt_entry(uint64_t tid){

	MEM_TT_ENTRY* new_entry = (MEM_TT_ENTRY*) malloc(sizeof(MEM_TT_ENTRY));

	new_entry->tid = tid;
	new_entry->next = NULL;

	//mutex_create(LATCH_ID_PL_TT_ENTRY, &new_entry->lock);

	//new list
	new_entry->list = (MEM_LOG_LIST*) malloc(sizeof(MEM_LOG_LIST));
	new_entry->list->head = new_entry->list->tail = NULL;
	new_entry->list->n_items = 0;

	//local dpt
	new_entry->local_dpt = alloc_DPT(MAX_DPT_ENTRIES);
	
	return new_entry;
}

void
free_tt_entry(
	   	MEM_TT_ENTRY* entry)
{
	entry->list->head = entry->list->tail = NULL;
	entry->list->n_items = 0;
	free (entry->list);
	entry->list = NULL;

	//mutex_destroy(&entry->lock);

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
	
	memrec->tid = 0;
	memrec->size = 0;

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
		MEM_DPT*		dpt,
	   	MEM_LOG_REC*	rec,
		bool			is_local_dpt)
{
	ulint hashed;
	MEM_DPT_ENTRY* bucket;
	
	//(1) Get the hash
	PMEM_LOG_HASH_KEY(hashed, rec->pid.fold(), dpt->n);
	assert (hashed < dpt->n);

	bucket = dpt->buckets[hashed];

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
		bucket = bucket->next;
	}
	// (2.2) the dirty page is not exist
	if (bucket == NULL){
		//(2.2.1) new bucket 
		MEM_DPT_ENTRY* new_entry = alloc_dpt_entry(rec->pid, 0);
		
		//(2.2.2) Add memrec to new entry
		if (is_local_dpt)
		{
			add_log_to_local_DPT_entry(pop, new_entry, rec); 
		}
		else 
		{
			add_log_to_global_DPT_entry(pop, new_entry, rec);
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
		printf("PMEM_INFO: CREATE GLOBAL dpt_entry space %zu page %zu at hashed %zu\n", rec->pid.space(), rec->pid.page_no(), hashed );
#endif
		}

		//mutex_enter(&dpt->hl_locks[hashed]);
		pmemobj_rwlock_wrlock(pop, &dpt->pmem_locks[hashed]);
		//(2.2.3) append the new_entry in the hashed line
		if (dpt->tails[hashed] == NULL){
			//this is the first bucket
			dpt->buckets[hashed] = new_entry;
			dpt->tails[hashed] = new_entry;
		}	
		else{
			//mutex_enter(&dpt->tails[hashed]->lock);
			dpt->tails[hashed]->next = new_entry;
			dpt->tails[hashed] = new_entry;
			//mutex_exit(&dpt->tails[hashed]->lock);
		}
		//mutex_exit(&dpt->hl_locks[hashed]);
		pmemobj_rwlock_unlock(pop, &dpt->pmem_locks[hashed]);

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

	//mutex_enter(&entry->lock);
	pmemobj_rwlock_wrlock(pop, &entry->pmem_lock);
	// (1) Generate the LSN
	rec->lsn = entry->curLSN + 1;
	entry->curLSN = rec->lsn;

	// (2) insert the log record to the list
	if (entry->list->n_items == 0){
		entry->list->head = entry->list->tail = rec;
		++entry->list->n_items;

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("+++ add log to GLOBAL dpt_entry space %zu page %zu tid %zu curLSN %zu rec->lsn %zu list->n_items %zu\n",
			rec->pid.space(), rec->pid.page_no(),rec->tid, entry->curLSN, rec->lsn, entry->list->n_items );
#endif
		//mutex_exit(&entry->lock);
		pmemobj_rwlock_unlock(pop, &entry->pmem_lock);

		return;
	}
	//since the last rec always has max lsn, we only append on the list
	assert(rec->lsn > entry->list->tail->lsn);
	entry->list->tail->dpt_next = rec;
	rec->dpt_prev = entry->list->tail;
	entry->list->tail = rec;
	rec->dpt_next = NULL;

	entry->list->n_items++;

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("+++ add log to GLOBAL dpt_entry space %zu page %zu tid %zu curLSN %zu rec->lsn %zu list->n_items %zu\n",
			rec->pid.space(), rec->pid.page_no(),rec->tid, entry->curLSN, rec->lsn, entry->list->n_items );
#endif
	//mutex_exit(&entry->lock);
	pmemobj_rwlock_unlock(pop, &entry->pmem_lock);
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

	//mutex_enter(&entry->lock);
	pmemobj_rwlock_wrlock(pop, &entry->pmem_lock);

	// (1) insert the log record to the list
	if (entry->list->n_items == 0){
		entry->list->head = entry->list->tail = rec;
		entry->list->n_items++;

		entry->curLSN = rec->lsn;

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("+++ add log to LOCAL dpt_entry space %zu page %zu tid %zu curLSN %zu rec->lsn %zu list->n_items %zu\n",
			rec->pid.space(), rec->pid.page_no(),rec->tid, entry->curLSN, rec->lsn, entry->list->n_items );
#endif

		//mutex_exit(&entry->lock);
		pmemobj_rwlock_unlock(pop, &entry->pmem_lock);

		return;
	}
	//append
	entry->list->tail->trx_page_next = rec;
	rec->trx_page_prev = entry->list->tail;
	entry->list->tail = rec;
	rec->trx_page_next = NULL;

	entry->curLSN = rec->lsn;

	entry->list->n_items++;
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("+++ add log to LOCAL dpt_entry space %zu page %zu tid %zu curLSN %zu rec->lsn %zu list->n_items %zu\n",
			rec->pid.space(), rec->pid.page_no(),rec->tid, entry->curLSN, rec->lsn, entry->list->n_items );
#endif
	//mutex_exit(&entry->lock);
	pmemobj_rwlock_unlock(pop, &entry->pmem_lock);
}
//////////////////// End Dirty Page Table functions//////////


//////////////////// Transaction Table functions//////////
/*
 * Remove a DPT entry from the global DPT
 * Call this function when flush a page
 * */
void
remove_dpt_entry(
		PMEMobjpool*		pop,
		MEM_DPT*			global_dpt,
		MEM_DPT_ENTRY*		entry,
		MEM_DPT_ENTRY*		prev_entry,
		ulint				hashed)
{
	//(1) remove the entry from the hashed line
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
			printf("PMEM_INFO REMOVE GLOBAL dpt entry when FLUSH space %zu page %zu hashed %zu\n", entry->id.space(), entry->id.page_no(), hashed);
#endif
	
	//Approach 1: Physical remove the entry and free the entry
	//////////////////////////////////////////////
//	pmemobj_rwlock_wrlock(pop, &global_dpt->pmem_locks[hashed]);
//	if (prev_entry == NULL){
//		global_dpt->buckets[hashed] = entry->next;
//
//	}
//	else{
//		prev_entry->next = entry->next;
//	}
//
//	if (entry->next == NULL){
//		//the entry is the tail of the hashed line
//		global_dpt->tails[hashed] = prev_entry;
//	}
//	else{
//		//do nothing
//	}
//	pmemobj_rwlock_unlock(pop, &global_dpt->pmem_locks[hashed]);
//	entry->next = NULL;
	/////////////////////////////////
	
		


	// (2) Remove dpt_next, dpt_prev pointers of each log record in the entry and change is_in_pmem to true
	//mutex_enter(&entry->lock);
	pmemobj_rwlock_wrlock(pop, &entry->pmem_lock);
	adjust_dpt_entry_on_flush(entry);

	//Approach 2: Just EMPTY the entry without remove and free it
	entry->list->head = NULL;
	entry->list->tail = NULL;
	entry->list->n_items = 0;
	//Do not reset id, curLSN, next
	//mutex_exit(&entry->lock);
	pmemobj_rwlock_unlock(pop, &entry->pmem_lock);


	//Approach 1: Physical remove the entry and free the entry
	// (3) Free resource of the entry	
	//free_dpt_entry(entry);	
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
remove_tt_entry(
		PMEMobjpool*	pop,
		MEM_TT*			tt,
		MEM_DPT*		global_dpt,
	   	MEM_TT_ENTRY*	entry,
	   	MEM_TT_ENTRY*	prev_entry,
		ulint			hashed)
{
	MEM_DPT*			local_dpt;
	MEM_DPT_ENTRY*		local_dpt_entry;
	MEM_DPT_ENTRY*		prev_dpt_entry;
	ulint				i;

	local_dpt = entry->local_dpt;

	//(1) Remove entry out of the hashed line
	pmemobj_rwlock_wrlock(pop, &tt->pmem_locks[hashed]);
	if (prev_entry == NULL){
		tt->buckets[hashed] = entry->next;
	}
	else{
		prev_entry->next = entry->next;
	}

	if (entry->next == NULL){
		//the entry is the tail of the hashed line
		tt->tails[hashed] = prev_entry;
	}
	else{
		//do nothing
	}
	pmemobj_rwlock_unlock(pop, &tt->pmem_locks[hashed]);

	entry->next = NULL;
#if !defined (UNIV_TEST_PL_P2)
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
#if !defined (UNIV_TEST_PL_P2)
			remove_logs_on_remove_local_dpt_entry(pop, global_dpt, local_dpt_entry);
#endif //!defined (UNIV_TEST_PL_P2)
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
#endif //!defined (UNIV_TEST_PL_P2)

#if !defined (UNIV_TEST_PL_P2)
	//(2.2) Until this point, all hashed lines in the local DPT are removed.
	free_DPT(entry->local_dpt);
#endif //!defined (UNIV_TEST_PL_P2)

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

#if !defined( UNIV_TEST_PL_P2)
	//(1) Add log record to the global DPT, after the function completed, rec->lsn is assinged to next lsn in page
	add_log_to_DPT(pop, dpt, rec, false);
#endif // UNIV_TEST_PL_P2

	//(2) Add log record to transaction entry
	//Get the hash by transaction id
	PMEM_LOG_HASH_KEY(hashed, rec->tid, tt->n);
	assert (hashed < tt->n);

	bucket = tt->buckets[hashed];
	

	//Multiple entries can have the same hashed value, Search for the right entry in the same hashed 
	while (bucket != NULL){
		if (bucket->tid == rec->tid){
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
		//printf("++0 add rec to existed TT entry tid %zu n_items %zu\n", rec->tid, bucket->list->n_items);
#endif
			//(2.1) insert the log record into this bucket
			add_log_to_TT_entry(pop, bucket, rec); 

#if !defined( UNIV_TEST_PL_P2)
			// (3) Add log record to the local DPT of trx entry
			add_log_to_DPT(pop, bucket->local_dpt, rec, true);
#endif // UNIV_TEST_PL_P2
			return;
		}
		bucket = bucket->next;
	}
	// (2.2) the transaction entry (bucket) is not exist
	if (bucket == NULL){
		// new bucket 
		MEM_TT_ENTRY* new_entry = alloc_tt_entry(rec->tid);	
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
		printf("+++ add rec to new TT entry tid %zu \n", rec->tid);
#endif
		add_log_to_TT_entry(pop, new_entry, rec); 

#if !defined( UNIV_TEST_PL_P2)
		// (3) Add log record to the local DPT of trx entry
		add_log_to_DPT(pop, new_entry->local_dpt, rec, true);
#endif
		pmemobj_rwlock_wrlock(pop, &tt->pmem_locks[hashed]);
		//link the new bucket
		if (tt->tails[hashed] == NULL){
			//this is the first bucket
			tt->buckets[hashed] = new_entry;
			tt->tails[hashed] = new_entry;
		}	
		else{
			tt->tails[hashed]->next = new_entry;
			tt->tails[hashed] = new_entry;
		}
		pmemobj_rwlock_unlock(pop, &tt->pmem_locks[hashed]);

	} //end if (bucket == NULL)

	return;
}

/*
 *Add a log record to a transacton entry in the transaction table
Caller: add_log_to_TT
 * */
void
add_log_to_TT_entry(
		PMEMobjpool*	pop,
	   	MEM_TT_ENTRY* entry,
	   	MEM_LOG_REC* rec){

	MEM_LOG_LIST*	list;

	assert(entry);
	assert(rec);
	//we don't need to acquire the lock because only this transaction can touch the entry
	//mutex_enter(&entry->lock);
	pmemobj_rwlock_wrlock(pop, &entry->pmem_lock);
	//(1) Insert the log record at the tail of the list
	list = entry->list;
	//If the list is empty
	if (list->n_items == 0){
		list->head = list->tail = rec;
		rec->tt_next = rec->tt_prev = NULL;
		list->n_items++;
		//mutex_exit(&entry->lock);
		pmemobj_rwlock_unlock(pop, &entry->pmem_lock);
		return;
	}
	
	list->tail->tt_next = rec;
	rec->tt_prev = list->tail;
	list->tail = rec;
	rec->tt_next = NULL;

	list->n_items++;
	//mutex_exit(&entry->lock);
	pmemobj_rwlock_unlock(pop, &entry->pmem_lock);
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
		PMEM_WRAPPER*		pmw,
	   	trx_t*			trx)
{

	PMEM_BUF*		buf = pmw->pbuf;
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
	
	ulint			tid = trx->id;
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
	printf("BEGIN COMMIT ===> tid %zu commit\n", tid);
#endif

	//(2) For each DPT entry in the local dpt, write REDO log to corresponding page	
	local_dpt = bucket->local_dpt;
	assert(local_dpt);
#if !defined (UNIV_TEST_PL_P2)
	for (i = 0; i < local_dpt->n; i++){
		//Get a hashed line
		local_dpt_entry = local_dpt->buckets[i];
		if (local_dpt_entry == NULL) 
			continue;
		assert(local_dpt_entry);
		//for each entry in the same hashed line
		while (local_dpt_entry != NULL){
			pm_write_REDO_logs(pop, pmw, local_dpt_entry);
			local_dpt_entry = local_dpt_entry->next;
		}
		//next hashed line
	}//end for each hashed line
#endif // UNIV_TEST_PL_P2

	//(3) Remove tt entry and its corresponding resources
	remove_tt_entry (pop,
			tt,
		   	global_dpt,
		   	bucket,
		   	prev_bucket,
		   	hashed);

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("END COMMIT ===> tid %zu commit\n", tid);
#endif
	
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
		PMEMobjpool*		pop,
		PMEM_WRAPPER*		pmw,
	   	uint64_t			tid)
{
	PMEM_BUF*		buf = pmw->pbuf;
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
#if defined (UNIV_TEST_PL_P2)
	for (i = 0; i < local_dpt->n; i++){
		//Get a hashed line
		dpt_entry = local_dpt->buckets[i];
		if (dpt_entry == NULL) 
			continue;
		assert(dpt_entry);
		//for each entry in the same hashed line
		while (dpt_entry != NULL){
			MEM_LOG_REC* first_memrec;
			first_memrec = dpt_entry->list->head; assert(first_memrec);

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
#endif //UNIV_TEST_PL_P2
	//(3) Remove tt entry and its corresponding resources
	remove_tt_entry (pop, tt, global_dpt, bucket, prev_bucket, hashed);

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
		PMEM_WRAPPER*		pmw,
		MEM_DPT_ENTRY*	dpt_entry
	   	) 
{
	PMEM_BUF* buf = pmw->pbuf;
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
	PMEM_LESS_BUCKET_HASH_KEY(buf, hashed,page_id.space(), page_id.page_no());
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
			pm_buf_handle_full_hashed_list(pop, pmw, hashed);
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
		
		//mutex_enter(&dpt_entry->lock);
		pmemobj_rwlock_wrlock(pop, &dpt_entry->pmem_lock);

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

		//mutex_exit(&dpt_entry->lock);
		pmemobj_rwlock_wrlock(pop, &dpt_entry->pmem_lock);
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
 *
 * For each logrec on the local dpt_entry:
 * (1) Remove corresponding logrec on the global dpt_entry
 * (2) Free the logrec
 * */
void 
remove_logs_on_remove_local_dpt_entry(
		PMEMobjpool*	pop,
		MEM_DPT*	global_dpt,
		MEM_DPT_ENTRY*		entry)
{
	ulint hashed;
	ulint count;

	MEM_LOG_REC*	first_memrec;
	MEM_LOG_REC*	memrec;
	MEM_LOG_REC*	next_memrec;//next log rec of memrec in the entry
	MEM_DPT_ENTRY* global_DPT_entry;
	MEM_DPT_ENTRY* prev_global_DPT_entry;
	MEM_DPT_ENTRY* bucket;
	bool is_page_in_pmem = false;

	// Get the first memrec in the entry
	assert (entry->list->n_items > 0);
	//first_memrec = entry->list->head;
	//assert(first_memrec);
	//is_page_in_pmem = first_memrec->is_in_pmem;
	
	// Since we never remove the global DPT entry, there always is the corresponding global DPT entry
	//(1) Get the corresponding DPT entry in the global DPT
	PMEM_LOG_HASH_KEY(hashed, entry->id.fold(), global_dpt->n);
	global_DPT_entry = global_dpt->buckets[hashed];
	prev_global_DPT_entry = NULL;
	assert(global_DPT_entry);

	while (global_DPT_entry != NULL){
		if (global_DPT_entry->id.equals_to(entry->id)){
			break;
		}
		prev_global_DPT_entry = global_DPT_entry;
		global_DPT_entry = global_DPT_entry->next;
	}

	if (global_DPT_entry == NULL){
		printf("PMEM ERROR! Cannot find corresponding DPT entry in remove_local_DPT_entry()\n");
		assert(0);
	}

	//(3) For each log record in the local dpt entry, remove pointers
	
	//mutex_enter(&global_DPT_entry->lock);
	pmemobj_rwlock_wrlock(pop, &global_DPT_entry->pmem_lock);
	//if (!is_page_in_pmem){
	//	mutex_enter(&global_DPT_entry->lock);
	//}
	count = 0;
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
//	if (entry->list->head != NULL)
//	printf("BEGIN remove_logs_on_remove_local_dpt_entry remove local nitems %zu global nitems %zu count %zu\n",
//		entry->list->n_items,
//		is_page_in_pmem ? 0 : global_DPT_entry->list->n_items,
//		count);
#endif 
	memrec = entry->list->head;
	//for each memrec in the local dpt_entry
	while (memrec != NULL){
		next_memrec = memrec->trx_page_next;

		//if (!is_page_in_pmem){
		if (!memrec->is_in_pmem){
			//(2.1) Remove corresponding memrec from global DPT entry

			assert (global_DPT_entry->list->n_items > 0);
			if (global_DPT_entry->list->n_items == 1){
				//Special case: memrec is the last one in the global dpt_entry
				global_DPT_entry->list->head = NULL;
				global_DPT_entry->list->tail = NULL;
			}
			else {
				if (memrec->dpt_prev == NULL){
					//Case A: memrec is the first log record in the global DPT entry
					global_DPT_entry->list->head = memrec->dpt_next;
					assert(memrec->dpt_next != NULL);
					memrec->dpt_next->dpt_prev = NULL;
				}
				else{
					if (memrec->dpt_next == NULL){
						//Case B: memrec is the last log record in the global DPT entry
						global_DPT_entry->list->tail = memrec->dpt_prev;
						assert(memrec->dpt_prev != NULL);
						memrec->dpt_prev->dpt_next = NULL;
					}
					else {
						//Case C: memrec is the normal log record in the global DTP entry
						memrec->dpt_prev->dpt_next = memrec->dpt_next;
						memrec->dpt_next->dpt_prev = memrec->dpt_prev;
						memrec->dpt_prev = NULL;
						memrec->dpt_next = NULL;
					}
				}

			}//end else n_times >= 2
			--global_DPT_entry->list->n_items;

#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("--- remove log from GLOBAL and LOCAL dpt_entry space %zu page %zu tid %zu curLSN %zu rec->lsn %zu global list->n_items %zu local list->n_items %zu\n",
			global_DPT_entry->id.space(), global_DPT_entry->id.page_no(),memrec->tid, global_DPT_entry->curLSN, memrec->lsn, global_DPT_entry->list->n_items, entry->list->n_items );
#endif
		}// end if (is_page_in_pmem)
		else {
			//memrec is in pmem by a flush
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
	printf("--- remove log from LOCAL dpt_entry (***rec is flush to pmem) space %zu page %zu tid %zu curLSN %zu rec->lsn %zu global list->n_items %zu local list->n_items %zu\n",
			global_DPT_entry->id.space(), global_DPT_entry->id.page_no(),memrec->tid, global_DPT_entry->curLSN, memrec->lsn, global_DPT_entry->list->n_items, entry->list->n_items );
#endif

		}
		free_memrec(memrec);
		--entry->list->n_items;
		count++;

		//global_DPT_entry->list->n_items--;
		//entry->list->n_items--;

		memrec = next_memrec;
	}//end while

	//if (!is_page_in_pmem){
		if (global_DPT_entry->list->n_items == 0){
			global_DPT_entry->list->head = NULL;
			global_DPT_entry->list->tail = NULL;
#if defined (UNIV_PMEMOBJ_PL_DEBUG)
			printf("PMEM_INFO EMPTY GLOBAL dpt entry space %zu page %zu hashed %zu\n", global_DPT_entry->id.space(), global_DPT_entry->id.page_no(), hashed);
#endif
		}
		//mutex_exit(&global_DPT_entry->lock);
		pmemobj_rwlock_unlock(pop, &global_DPT_entry->pmem_lock);
	//}

	//assert (count <= entry->list->n_items);
	//entry->list->n_items -= count;


#if defined (UNIV_PMEMOBJ_PL_DEBUG)
//	if (entry->list->head != NULL)
//	printf("After remove local nitems %zu global nitems %zu count %zu\n",
//		entry->list->n_items,
//		is_page_in_pmem ? 0 : global_DPT_entry->list->n_items,
//		count);
#endif 

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

	uint64_t i, j, n, k;
	uint64_t cur_bucket;
	size_t offset;

	PMEM_TX_LOG_HASHED_LINE* pline;
	PMEM_TX_LOG_BLOCK*	plog_block;

	offset = 0;
	cur_bucket = 0;

	n = n_buckets;
	k = n_blocks_per_bucket;
	
	//allocate the pointer array buckets
	POBJ_ALLOC(pop,
			&pl->buckets,
			TOID(PMEM_TX_LOG_HASHED_LINE),
			sizeof(TOID(PMEM_TX_LOG_HASHED_LINE)) * n,
			NULL,
			NULL);
	if (TOID_IS_NULL(pl->buckets)) {
		fprintf(stderr, "POBJ_ALLOC\n");
	}

	pl->pmem_tx_log_size = sizeof(TOID(PMEM_TX_LOG_HASHED_LINE)) * n;
	//for each hashed line
	for (i = 0; i < n; i++) {
		POBJ_ZNEW(pop,
				&D_RW(pl->buckets)[i],
				PMEM_TX_LOG_HASHED_LINE);
		if (TOID_IS_NULL(D_RW(pl->buckets)[i])) {
			fprintf(stderr, "POBJ_ZNEW\n");
		}

		pl->pmem_tx_log_size += sizeof(PMEM_TX_LOG_HASHED_LINE);
		pline = D_RW(D_RW(pl->buckets)[i]);

		pline->hashed_id = i;
		pline->n_blocks = k;

		//Allocate the log blocks
		POBJ_ALLOC(pop,
				&pline->arr,
				TOID(PMEM_TX_LOG_BLOCK),
				sizeof(TOID(PMEM_TX_LOG_BLOCK)) * k,
				NULL,
				NULL);
		pl->pmem_tx_log_size += sizeof(TOID(PMEM_TX_LOG_BLOCK)) * k;
		//for each log block
		for (j = 0; j < k; j++) {
			POBJ_ZNEW(pop,
					&D_RW(pline->arr)[j],
					PMEM_TX_LOG_BLOCK);
			if (TOID_IS_NULL(D_RW(pline->arr)[j])) {
				fprintf(stderr, "POBJ_ZNEW\n");
			}
		pl->pmem_tx_log_size += sizeof(PMEM_TX_LOG_BLOCK);
			plog_block = D_RW(D_RW(pline->arr)[j]);
			//assign the pmemaddr similar to the 2D array indexing
			//plog_block->pmemaddr = (i * k + j) * block_size; 
			plog_block->bid = i * k + j;
			plog_block->tid = 0;
			plog_block->cur_off = 0;
			plog_block->n_log_recs = 0;
			plog_block->state = PMEM_FREE_LOG_BLOCK;
			plog_block->count = 0;
			
			//array of dirty pages
			plog_block->n_dp_entries = 0;
			POBJ_ALLOC(pop,
					&plog_block->dp_array,
					TOID(PMEM_PAGE_REF),
					sizeof(TOID(PMEM_PAGE_REF)) * MAX_DIRTY_PAGES_PER_TX,
					NULL,
					NULL);
			uint64_t i_temp;
			for (i_temp = 0; i_temp < MAX_DIRTY_PAGES_PER_TX; i_temp++){
				POBJ_ZNEW(pop,
					&D_RW(plog_block->dp_array)[i_temp],
				PMEM_PAGE_REF);	
			}

		pl->pmem_tx_log_size += ( sizeof(TOID(PMEM_PAGE_REF)) + sizeof(PMEM_PAGE_REF)) * MAX_DIRTY_PAGES_PER_TX;

#if defined (UNIV_PMEMOBJ_PART_PL_STAT)
			plog_block->all_n_reused = 0;
			plog_block->all_avg_log_rec_size = 0;
			plog_block->all_min_log_rec_size = ULONG_MAX;
			plog_block->all_max_log_rec_size = 0;
			plog_block->all_avg_block_lifetime = 0;

			plog_block->all_max_log_buf_size = 0;
			plog_block->all_avg_log_buf_size = 0;
			
			plog_block->all_max_n_recs = 0;
			plog_block->all_avg_n_recs = 0;
			plog_block->all_max_n_pagerefs = 0;

			plog_block->n_small_log_recs = 0;
			plog_block->avg_log_rec_size = 0;
			plog_block->min_log_rec_size = ULONG_MAX;
			plog_block->max_log_rec_size = 0;
			plog_block->lifetime = 0;
#endif
		}
	}//end for each hashed line
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
//int64_t
//pm_ptxl_write(
//			PMEMobjpool*		pop,
//			PMEM_TX_PART_LOG*	ptxl,
//			uint64_t			tid,
//			byte*				log_src,
//			uint64_t			size,
//			uint64_t			n_recs,
//			uint64_t*			key_arr,
//			int64_t				block_id)
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
				__print_tx_blocks_state(debug_ptxl_file, ptxl);
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

		
		if (plog_block->n_dp_entries >= MAX_DIRTY_PAGES_PER_TX){
			printf("PMEM ERROR, in pm_ptxl_write(), n_dp_entries reach the max capacity %zu, change your setting \n", MAX_DIRTY_PAGES_PER_TX);
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

void
pm_wrapper_page_log_alloc_or_open(
		PMEM_WRAPPER*	pmw,
		uint64_t		n_buckets,
		uint64_t		n_blocks_per_bucket,
		uint64_t		n_log_bufs,
		uint64_t		log_buf_size) {
	
	// Get const variable from parameter and config value
	
	PMEM_N_LOG_BUCKETS = n_buckets;
	PMEM_N_BLOCKS_PER_BUCKET = n_blocks_per_bucket;

	PMEM_LOG_BUF_FLUSH_PCT = 0.9;
	PMEM_LOG_FLUSHER_WAKE_THRESHOLD = 5;
	PMEM_N_LOG_FLUSH_THREADS=32;
	
	if (!pmw->ppl) {
		pmw->ppl = alloc_pmem_page_part_log(
				pmw->pop,
				PMEM_N_LOG_BUCKETS,
				PMEM_N_BLOCKS_PER_BUCKET,
				n_log_bufs,
				log_buf_size);

		if (pmw->ppl == NULL){
			printf("PMEMOBJ_ERROR: error when allocate buffer in pm_wrapper_page_log_alloc_or_open()\n");
			exit(0);
		}
		printf("\n=================================\n Footprint of PAGE part-log:\n");
		//printf("Log area size \t\t\t %f (MB) \n", (n_buckets * n_blocks_per_bucket * block_size * 1.0)/(1024*1024) );
		printf("Log area %zu x %zu (B) = \t\t %f (MB) \n", n_log_bufs, log_buf_size, (n_log_bufs * log_buf_size * 1.0)/(1024*1024) );
		printf("PAGE-Log metadata: %zu x %zu, size %f (MB)\n", n_buckets, n_blocks_per_bucket, (pmw->ppl->pmem_page_log_size * 1.0)/(1024*1024));
		printf("TT metadata, %zu x %zu, size \t %f (MB)\n", MAX_TT_LINES, MAX_TT_ENTRIES_PER_LINE, (pmw->ppl->pmem_tt_size * 1.0) / (1024*1024));
		printf("Total allocated \t\t %f (MB)\n", (pmw->ppl->pmem_alloc_size * 1.0)/ (1024*1024));
		printf(" =================================\n");
	}
	else {
		//Case 2: Reused a buffer in PMEM
		printf("!!!!!!! [PMEMOBJ_INFO]: the server restart from a crash but the per-page log buffers are persist\n");
		//We need to re-align the p_align
		byte* p;
		p = static_cast<byte*> (pmemobj_direct(pmw->ppl->data));
		assert(p);
		pmw->ppl->p_align = static_cast<byte*> (ut_align(p, log_buf_size));
	}

	pmw->ppl->deb_file = fopen("part_log_debug.txt","a");
}

void
pm_wrapper_page_log_close(
		PMEM_WRAPPER*	pmw	)
{
	ulint i;
	
	PMEM_PAGE_PART_LOG* ppl = pmw->ppl;
	
	//Free resource allocated in DRAM
	os_event_destroy(ppl->free_log_pool_event);
	pm_log_flusher_close(ppl->flusher);

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
	ppl->free_log_pool_event = os_event_create("pm_free_log_pool_event");
	__init_page_log_free_pool(
			pop,
			ppl,
			n_free_log_bufs,
			log_buf_size,
			log_buf_id,
			log_buf_offset);

	ppl->pmem_alloc_size += ppl->pmem_page_log_free_pool_size;
	
	//(4) transaction table
	__init_tt(pop, ppl, MAX_TT_LINES, MAX_TT_ENTRIES_PER_LINE);

	ppl->pmem_alloc_size += ppl->pmem_tt_size;

	// (5) Flusher
	// defined in my_pmemobj.h, implement in buf0flu.cc
	ppl->flusher = pm_log_flusher_init(PMEM_N_LOG_FLUSH_THREADS);


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
		uint64_t				k) {

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

		pline->hashed_id = i;
		pline->n_entries = k;

		//Allocate the entries
		POBJ_ALLOC(pop,
				&pline->arr,
				TOID(PMEM_TT_ENTRY),
				sizeof(TOID(PMEM_TT_ENTRY)) * k,
				NULL,
				NULL);
		ppl->pmem_tt_size += sizeof(TOID(PMEM_TT_ENTRY))* k;
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
					sizeof(TOID(PMEM_PAGE_REF)) * MAX_DIRTY_PAGES_PER_TX,
					NULL,
					NULL);

			ppl->pmem_tt_size += sizeof(TOID(PMEM_PAGE_REF)) * MAX_DIRTY_PAGES_PER_TX;

			for (i_temp = 0; i_temp < MAX_DIRTY_PAGES_PER_TX; i_temp++){
				POBJ_ZNEW(pop,
					&D_RW(pe->dp_arr)[i_temp],
				PMEM_PAGE_REF);	

				D_RW(D_RW(pe->dp_arr)[i_temp])->key = 0;
				D_RW(D_RW(pe->dp_arr)[i_temp])->idx = -1;
				D_RW(D_RW(pe->dp_arr)[i_temp])->pageLSN = 0;
			}
			pe->n_dp_entries = 0;
			ppl->pmem_tt_size += sizeof(PMEM_PAGE_REF) * MAX_DIRTY_PAGES_PER_TX;
		} //end for each entry
	}// end for each line
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

		//Log buf, after the alloca call, log_buf_id and log_buf_offset increase
		POBJ_ZNEW(pop, &pline->logbuf, PMEM_PAGE_LOG_BUF);
		pl->pmem_page_log_size += sizeof(PMEM_PAGE_LOG_BUF);

		PMEM_PAGE_LOG_BUF* plogbuf = D_RW(pline->logbuf);

		plogbuf->pmemaddr = log_buf_offset;
		log_buf_offset += log_buf_size;

		plogbuf->id = log_buf_id;
		log_buf_id++;

		plogbuf->state = PMEM_LOG_BUF_FREE;
		plogbuf->size = log_buf_size;
		plogbuf->cur_off = 0;
		plogbuf->self = (pline->logbuf).oid;

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

			plog_block->cur_size = 0;
			plog_block->n_log_recs = 0;

			plog_block->pageLSN = 0;
			plog_block->lastLSN = 0;
			plog_block->start_off = 0;
			plog_block->start_diskaddr = 0;

			////array of active transaction
			//plog_block->n_tx_idx = 0;
			//POBJ_ALLOC(pop,
			//		&plog_block->tx_idx_arr,
			//		int64_t,
			//		sizeof(int64_t) * MAX_TX_PER_PAGE,
			//		NULL,
			//		NULL);
			//uint64_t i_temp;
			//for (i_temp = 0; i_temp < MAX_TX_PER_PAGE; i_temp++){
			//	D_RW(plog_block->tx_idx_arr)[i_temp] = -1;
			//}
			//pl->pmem_page_log_size +=  sizeof(int64_t) * MAX_TX_PER_PAGE;

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

		plogbuf->state = PMEM_LOG_BUF_FREE;
		plogbuf->size = log_buf_size;
		plogbuf->cur_off = 0;
		plogbuf->self = logbuf.oid;

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
	

	if (entry_id == -1) {
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
				__print_TT(debug_ptxl_file, ptt);
				__print_page_blocks_state(debug_ptxl_file, ppl);

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
		
		pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[bucket_id])->lock);
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
				LSN_arr,
				size_arr,
				pe,
				false);

		pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[bucket_id])->lock);

	//printf("END case B add log for tid %zu at entry %zu n_recs %zu bucket_id %zu local_id %zu \n", tid,  entry_id, n_recs, bucket_id, local_id);
		return ret;

	}//end case B

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

	PMEM_PAGE_REF* pref;
	// Now the pe point to the entry to write on and the ret is the entry id

	//printf("\t BEGIN handle_pm_ppl_write_by_entry tid %zu entry %zu n_recs %zu size %zu pe->npagerefs %zu \n", tid, pe->eid, n_recs, size, pe->n_dp_entries);	
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
	//printf("\t END handle_pm_ppl_write_by_entry tid %zu entry %zu n_recs %zu size %zu \n", tid, pe->eid, n_recs, size);	
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
	uint64_t n_try;

	int64_t idx;

	byte* pdata = ppl->p_align;
	byte* log_des;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	PMEM_PAGE_LOG_BUF* plogbuf;

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

		//(1) search for the exist log block
		for (i = 0; i < k; i++){
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
						pline,
						log_src,
						cur_off,
						rec_size,
						plog_block,
						false);
				//pmemobj_rwlock_wrlock(pop, &pline->lock);

				//plogbuf = D_RW(pline->logbuf);

				//log_des = pdata + plogbuf->pmemaddr + plogbuf->cur_off;
				//__pm_write_log_rec_low(pop,
				//		log_des,
				//		log_src + cur_off,
				//		rec_size);
				//plogbuf->cur_off += rec_size;

				//if (plogbuf->cur_off > plogbuf->size * PMEM_LOG_BUF_FLUSH_PCT) {
				//	//simply reset
				//	plogbuf->cur_off = 0;		
				//}
				//	
				//
				//pmemobj_rwlock_unlock(pop, &pline->lock);

				plog_block->n_log_recs++;

				//(3) update metadata
				plog_block->lastLSN = LSN;
				//because in this case block_id == -1, there is no txref in this log block
				//search for the first free to write
				//for each txref
				//for (j = 0; j < plog_block->n_tx_idx; j++){
				//	if(D_RW(plog_block->tx_idx_arr)[j] ==-1){
				//		D_RW(plog_block->tx_idx_arr)[j] = eid;
				//		break;
				//	}	

				//}//end for each txref

				//if (j == plog_block->n_tx_idx){
				//	D_RW(plog_block->tx_idx_arr)[j] = eid;
				//	plog_block->n_tx_idx++;
				//}
				//inc number of active tx on this page			
				plog_block->count++;

				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				return ret;
			}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			//next log block
		}//end search for exist log block

		//Case A2: If you reach here, then the log block is not exst, write the new one. During the scan, some log block may be reclaimed, search from the begin

		//search for a free block to write on
		for (i = 0; i < k; i++){
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
						pline,
						log_src,
						cur_off,
						rec_size,
						plog_block,
						true);
				//pmemobj_rwlock_wrlock(pop, &pline->lock);

				////Note that we save the offset and diskaddr for the first write of this block
				//plogbuf = D_RW(pline->logbuf);

				//plog_block->start_off = plogbuf->cur_off;
				//plog_block->start_diskaddr = pline->diskaddr;

				//log_des = pdata + plogbuf->pmemaddr + plogbuf->cur_off;
				//__pm_write_log_rec_low(pop,
				//		log_des,
				//		log_src + cur_off,
				//		rec_size);
				//plogbuf->cur_off += rec_size;
				//if (plogbuf->cur_off > plogbuf->size * PMEM_LOG_BUF_FLUSH_PCT) {
				//	//simply reset
				//	plogbuf->cur_off = 0;		
				//}

				//pmemobj_rwlock_unlock(pop, &pline->lock);

				plog_block->n_log_recs++;

				//(3) update metadata
				plog_block->lastLSN = LSN;

				plog_block->count=1;
				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				return ret;
			}
			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			//next
		}//end search for a free block to write on

		//If you reach here, then there is no free block
		printf("PMEM_ERROR in __update_page_log_block_on_write(), no free log block on hashed %zu\n", hashed);
		__print_page_blocks_state(debug_ptxl_file, ppl);
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
				pline,
				log_src,
				cur_off,
				rec_size,
				plog_block,
				false);
		//pmemobj_rwlock_wrlock(pop, &pline->lock);
		//plogbuf = D_RW(pline->logbuf);

		//log_des = pdata + plogbuf->pmemaddr + plogbuf->cur_off;
		//__pm_write_log_rec_low(pop,
		//		log_des,
		//		log_src + cur_off,
		//		rec_size);
		//plogbuf->cur_off += rec_size;

		//if (plogbuf->cur_off > plogbuf->size * PMEM_LOG_BUF_FLUSH_PCT) {
		//	//simply reset
		//	plogbuf->cur_off = 0;		
		//}

		//pmemobj_rwlock_unlock(pop, &pline->lock);
		plog_block->n_log_recs++;

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
			PMEM_PAGE_LOG_HASHED_LINE*	pline,
			byte*						log_src,
			uint64_t					src_off,
			uint64_t					rec_size,
			PMEM_PAGE_LOG_BLOCK*		plog_block,
			bool						is_first_write){

	byte* log_des;
	byte* pdata;
	PMEM_PAGE_LOG_FREE_POOL* pfreepool;
	PMEM_PAGE_LOG_BUF* plogbuf;


	pmemobj_rwlock_wrlock(pop, &pline->lock);
	//when a logbuf is full, we switch the pline->logbuf not the line, so it's not require to check full and goto as in PB-NVM (see pm_buf_write_with_flusher())
	
	plogbuf = D_RW(pline->logbuf);

	// (1) Write log rec to log buf
	if (is_first_write){
		//Note that we save the offset and diskaddr for the first write of this block
		plog_block->start_off = plogbuf->cur_off;
		plog_block->start_diskaddr = pline->diskaddr;
	}
	log_des = ppl->p_align + plogbuf->pmemaddr + plogbuf->cur_off;
	__pm_write_log_rec_low(pop,
			log_des,
			log_src + src_off,
			rec_size);
	plogbuf->cur_off += rec_size;

	// (2) Handle full log buf
	if (plogbuf->cur_off > plogbuf->size * PMEM_LOG_BUF_FLUSH_PCT) {

get_free_buf:
		// (2.1) Get a free log buf
		pfreepool = D_RW(ppl->free_pool);

		pmemobj_rwlock_wrlock(pop, &pfreepool->lock);

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
		
		// (2.2) switch the free log buf with the full log buf
		TOID_ASSIGN(pline->logbuf, free_buf.oid);
		D_RW(free_buf)->state = PMEM_LOG_BUF_IN_USED;

		// (2.3) assign a pointer in the flusher to the full log buf, this function return immediately 
		pm_log_buf_assign_flusher(ppl, plogbuf);

		//simply reset
		//plogbuf->cur_off = 0;		
	} //end handle full log buf

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
		PMEM_PAGE_LOG_BUF*	plogbuf) {
	
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
		PMEM_PAGE_LOG_BUF*		plogbuf) {

	PMEM_PAGE_LOG_FREE_POOL* pfree_pool;
	TOID(PMEM_PAGE_LOG_BUF) logbuf;

	assert(plogbuf);
	TOID_ASSIGN(logbuf, plogbuf->self);

	//current version just simply reset the logbuf and return to the free pool

	/*(1) Pre-flush*/
	
	/*(2) Flush	*/

	/*(3) Post-flush */
	//reset the log buf
	plogbuf->state = PMEM_LOG_BUF_FREE;
	plogbuf->cur_off = 0;
	//put back to the free pool

	pfree_pool = D_RW(ppl->free_pool);
	pmemobj_rwlock_wrlock(pop, &pfree_pool->lock);

	POBJ_LIST_INSERT_TAIL(pop, &pfree_pool->head, logbuf, list_entries);
	pfree_pool->cur_free_bufs++;
	//wakeup who is waitting for free_pool available
	os_event_set(ppl->free_log_pool_event);

	pmemobj_rwlock_unlock(pop, &pfree_pool->lock);

}


static inline void
__pm_write_log_rec_low(
			PMEMobjpool*			pop,
			byte*					log_des,
			byte*					log_src,
			uint64_t				rec_size)
{
	if (rec_size <= CACHELINE_SIZE){
		//Do not need a transaction for atomicity
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
		int64_t					eid) {
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
			//update the corresponding log block and try to reclaim it
			__update_page_log_block_on_commit(
					pop, ppl, pref, pe->eid);
		}

	} //end for each pageref in the entry

	//we reset the TT entry on commit 
	__reset_TT_entry(pe);

	pmemobj_rwlock_unlock(pop, &pe->lock);
}

void __reset_TT_entry(PMEM_TT_ENTRY* pe)
{
	uint64_t i;
	PMEM_PAGE_REF* pref;
	
	pe->tid = 0;
	pe->state = PMEM_TX_FREE;

	for (i = 0; i < pe->n_dp_entries; i++) {
		pref = D_RW(D_RW(pe->dp_arr)[i]);
		pref->key = 0;
		pref->idx = -1;
		pref->pageLSN = 0;
	}
	pe->n_dp_entries = 0;
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
	//if (plog_block->count <= 0)
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

	uint64_t i;
	
	plog_block->is_free = true;
	plog_block->key = 0;
	plog_block->count = 0;

	plog_block->cur_size = 0;
	plog_block->n_log_recs = 0;

	plog_block->pageLSN = 0;
	plog_block->lastLSN = 0;
	plog_block->start_off = 0;
	plog_block->start_diskaddr = 0;

//	for (i = 0; i < plog_block->n_tx_idx; i++){
//		D_RW(plog_block->tx_idx_arr)[i] = -1;
//	}
//	plog_block->n_tx_idx = 0;
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
		uint64_t			key,
		uint64_t			pageLSN) 
{
//return;
	ulint hashed;
	uint32_t n, k, i, j;

	int64_t free_idx;

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
	
	//find the log block by hashing key
	for (i = 0; i < k; i++){
			pmemobj_rwlock_wrlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
			plog_block = D_RW(D_RW(pline->arr)[i]);

			if (!plog_block->is_free &&
				plog_block->key == key){
				// Case A: found
				//(1) no need to check and reclaim the corresponding entries in TT

				//update the pageLSN on flush page
				plog_block->pageLSN = pageLSN;
				assert(plog_block->lastLSN <= pageLSN);
				// (2) Check to recliam this log block
				if (plog_block->count <= 0){
					__reset_page_log_block(plog_block);
				}
				pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
				return;

			}//end found the right log block

			pmemobj_rwlock_unlock(pop, &D_RW(D_RW(pline->arr)[i])->lock);
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
	fprintf(f, "%zu %zu %.2f %.2f %zu %zu %.2f	%zu %zu %zu %zu %zu %zu %zu %zu %zu %zu \n", 
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
			plog_block->n_log_recs,
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
