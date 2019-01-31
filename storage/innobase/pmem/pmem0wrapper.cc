/* 
 * Author; Trong-Dat Nguyen
 * MySQL REDO log with NVDIMM
 * Using libpmemobj
 * Copyright (c) 2017 VLDB Lab - Sungkyunkwan University
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
//#include "pmem0buf.h"

//global variable
PMEM_WRAPPER* gb_pmw = NULL;

/*
 *@param[in] path	name of file created in pmem
 @parma[in] pool_size	size of pmemobjpool in Byte
 * */

PMEM_WRAPPER* pm_wrapper_create(const char* path, const size_t pool_size){
	PMEM_WRAPPER* pmw;
	PMEMobjpool* pop = NULL;
	int check_pmem;

	const char* reason;

	reason = pmemobj_check_version(PMEMOBJ_MAJOR_VERSION,
			                               PMEMOBJ_MINOR_VERSION);
	if (reason != NULL) {
			/* version check failed, reason string tells you why */
		fprintf(stderr, "PMEM_ERROR: checking version fail: %s \n", pmemobj_errormsg());
	}


	pmw =  (PMEM_WRAPPER*) malloc(sizeof(PMEM_WRAPPER));
	if (!pmw)
		goto err;
	pmw->is_new = true;

	//Force use pmem
	setenv("PMEM_IS_PMEM_FORCE", "1", 1);

	/*create new or open existed PMEMobjpool*/
//	size =  PMEM_MAX_LOG_BUF_SIZE +
//				   (PMEM_MAX_DBW_PAGES + 1) * PMEM_PAGE_SIZE  ;

	if (file_exists(path) != 0) {
		if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(my_pmemobj),
						pool_size, S_IWRITE | S_IREAD)) == NULL) {
			printf("[PMEMOBJ_ERROR] failed to create pool\n");
			goto err;
		}
	} else {
		pmw->is_new = false;

		if ((pop = pmemobj_open(path, POBJ_LAYOUT_NAME(my_pmemobj)))
				== NULL) {
			printf("[PMEMOBJ_ERROR] failed to open pool\n");
			goto err;
		}
	}
	//Checking
	check_pmem = pmemobj_check(path, POBJ_LAYOUT_NAME(my_pmemobj));
	if (check_pmem == -1) {
		printf("PMEM_ERROR: check_pmem = -1, errno is %d\n", errno);
		fprintf(stderr, "PMEM_ERROR: check_pmem = -1, detail: %s \n", pmemobj_errormsg());
		//assert(0);
	}
	printf ("!!!!!!   PMEM_INFO: CHECK CONSISTENCE check_pmem = %d (1: consistent, 0 not-consistent !!!!!!)\n", check_pmem);		
	pmw->pop = pop;

//	pmemw->pop = pm_create_PMEMobjpool(path);
	/*name */
	strncpy(pmw->name, path, PMEM_MAX_FILE_NAME_LENGTH);
	pmw->name[PMEM_MAX_FILE_NAME_LENGTH - 1] = '\0';

	/* log buffer */
	pmw->plogbuf = NULL;
	pmw->pdbw = NULL;
#if defined (UNIV_PMEMOBJ_BUF)
	pmw->pbuf = NULL;
#endif 
#if defined (UNIV_PMEMOBJ_PART_PL)
	pmw->ppl = NULL;
#endif

	/*If we have persistent data structures, get them*/
	if(!pmw->is_new) {
		pmw->plogbuf = pm_pop_get_logbuf(pop);
		if(!pmw->plogbuf){
			printf("[PMEMOBJ_INFO] the pmem log buffer is empty. The server've shutdown normally\n");
		}
		pmw->pdbw = pm_pop_get_dbw(pop);
		if(!pmw->pdbw){
			printf("[PMEMOBJ_INFO] the pmem double write buffer is empty. The database is new or the previous double write buffer is in disk instead of PMEM\n");
		}

#if defined (UNIV_PMEMOBJ_BUF)
		pmw->pbuf = pm_pop_get_buf(pop);
		if(!pmw->pbuf){
			printf("[PMEMOBJ_INFO] the pmem buf is empty. The database is new\n");
		}	
#endif 
#if defined (UNIV_PMEMOBJ_PART_PL)
		pmw->ppl = pm_pop_get_ppl(pop);
		if(!pmw->ppl){
			printf("[PMEMOBJ_INFO] the pmem ppl is empty. The database is new\n");
		}
		pmw->ppl->is_new = pmw->is_new;
#endif
	}

	return pmw;

err:
	printf("PMEMOBJ_ERROR: error in pm_create_PMEMwrapper");
	if(pmw)
		pm_wrapper_free(pmw);
	return NULL;
}

/*
 *This function free things allocated in DRAM
 * */
void pm_wrapper_free(PMEM_WRAPPER* pmw){
#if defined (UNIV_PMEMOBJ_BUF)

#if defined (UNIV_PMEMOBJ_LSB)
	pm_wrapper_lsb_close(pmw);
	pm_lsb_hashtable_free(pmw->pop, pmw->plsb);
#else
	//PMEM buf
	pm_wrapper_buf_close(pmw);
	pmw->pbuf = NULL;
#endif //UNIV_PMEMOBJ_LSB

#endif //UNIV_PMEMOBJ_BUF

#if defined (UNIV_PMEMOBJ_PART_PL)
	pm_wrapper_page_log_close(pmw);
	pmw->ppl = NULL;
#endif //UNIV_PMEMOBJ_PART_PL

	if(pmw->pop)
		pm_pop_free(pmw->pop);
	pmw->plogbuf = NULL;
	pmw->pop = NULL;

	printf("PMEMOBJ_INFO: free PMEM_WRAPPER from heap allocated\n");
	free(pmw);

}

/*
 * Allocate a range of persistent memory 
 * */
PMEMoid pm_pop_alloc_bytes(PMEMobjpool* pop, size_t size){
	TOID(char) array;

	POBJ_ALLOC(pop, &array, char, sizeof(char) * size,	NULL, NULL);

	if (TOID_IS_NULL(array)) {
		fprintf(stderr, "POBJ_ALLOC\n");
		return OID_NULL;
	}

	pmemobj_persist(pop, D_RW(array), size * sizeof(*D_RW(array)));
	printf("PMEMOBJ_INFO: allocate PMEMobjpool from pmem with size %zu MB\n", (size/(1024*1024)));

	//Check
	char* p = (char*) pmemobj_direct(array.oid);
	if(!p){
			printf("PMEMOBJ_ERROR: message: %s\n",  pmemobj_errormsg() );
	}
	return array.oid;
}
void pm_pop_free(PMEMobjpool* pop){
	TOID(PMEM_LOG_BUF) logbuf;
	TOID(PMEM_DBW) dbw;
	TOID(char) data1;
	TOID(char) data2;
	
	/* Free log buffer*/
	POBJ_FOREACH_TYPE(pop, logbuf) {
		TOID_ASSIGN(data1, D_RW(logbuf)->data);
		POBJ_FREE(&data1);	

		D_RW(logbuf)->lsn = 0;
		D_RW(logbuf)->buf_free = 0;
		D_RW(logbuf)->need_recv = false;

		POBJ_FREE(&logbuf);	
	}
	printf("PMEMOBJ_INFO: free PMEMobjpool from pmem\n");

	/*Free DBW*/
	/*We kept the double write buffer in PMEM when the server shutdown
	 *It use for recovery tone pages when the server start up
	 * */
	/*
	POBJ_FOREACH_TYPE(pop, dbw) {
		TOID_ASSIGN(data2, D_RW(dbw)->data);
		POBJ_FREE(&data2);	

		D_RW(dbw)->block1 = 0;
		D_RW(dbw)->block2 = 0;

		POBJ_FREE(&logbuf);	
	}
	*/

	pmemobj_close(pop);
}

