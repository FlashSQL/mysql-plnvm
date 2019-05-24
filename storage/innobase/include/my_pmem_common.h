
#ifndef __PMEM_COMMON_H__
#define __PMEM_COMMON_H__

#define CACHELINE_SIZE 64

#define PMEM_MAX_FILES 1000
#define PMEM_MAX_FILE_NAME_LENGTH 10000
#define PMEM_HASH_MASK 1653893711

#define PMEM_ID_NONE -1 //ID not defined

//random number for checking AIO
#define PMEM_AIO_CHECK 7988

//wait for a aio write, call when there is no free block
#define PMEM_WAIT_FOR_WRITE 200
#define PMEM_WAIT_FOR_FREE_LIST 10000

//error handler
#define PMEM_SUCCESS 0
#define PMEM_ERROR -1

// Partitioned-Log //////////////////////////
#define PMEM_PART_LOG_BUF_THRESHOLD 90 //90%
#define PMEM_PART_LOG_BUF_SIZE 64*4*1024 // bytes


#define JUMP_STEP 1
#define MAX_DPT_ENTRIES 8192
#define MAX_TT_ENTRIES 8192

//#define MAX_DPT_LINES 128 
//#define MAX_DPT_LINES 8192 
#define MAX_DPT_LINES 512 
//#define MAX_DPT_ENTRIES_PER_LINE 64 //max number of DPT entries per hashed line
#define MAX_DPT_ENTRIES_PER_LINE 1024 //max number of DPT entries per hashed line
#define MAX_TX_PER_PAGE 256 //max number transaction could access on a page at the same time 
//#define MAX_TX_PER_PAGE 64 
//

#define PMEM_CPU_FREQ 2.2

////////////// End Partitioned-Log ////////////

#define TOID_ARRAY(x) TOID(x)

#define PMEMOBJ_FILE_NAME "pmemobjfile"
//OS_FILE_LOG_BLOCK_SIZE =512 is defined in os0file.h
static const size_t PMEM_MB = 1024 * 1024;
static const size_t PMEM_MAX_LOG_BUF_SIZE = 1 * 1024 * PMEM_MB;
static const size_t PMEM_PAGE_SIZE = 16*1024; //16KB
static const size_t PMEM_MAX_DBW_PAGES= 128; // 2 * extent_size

//static const size_t PMEM_GROUP_PARTITION_TIME= 1000000;
//static const size_t PMEM_GROUP_PARTITION_SIZE= 4196;

static const size_t PMEM_GROUP_PARTITION_TIME= 10000;
static const size_t PMEM_GROUP_PARTITION_SIZE= 512;


//#define PMEM_N_BUCKETS 128 
//#define PMEM_USED_FREE_RATIO 0.2
#define PMEM_MAX_LISTS_PER_BUCKET 2
//#define PMEM_BUF_THRESHOLD 0.8

//#define PMEM_LOG_BUF_HEADER_SIZE 4
#define PMEM_LOG_BUF_HEADER_SIZE 8 /*4-byte real_len, 4-byte n_recs*/

enum {
	PMEM_READ = 1,
	PMEM_WRITE = 2
};
enum PMEM_OBJ_TYPES {
	UNKNOWN_TYPE,
	LOG_BUF_TYPE,
	DBW_TYPE,
	BUF_TYPE,
	META_DATA_TYPE
};

//use in pm_ppl_write()
#define PMEM_EID_NEW 0
#define PMEM_EID_REVISIT 1
#define PMEM_EID_UNDEFINED 2


enum PMEM_BLOCK_STATE {
    PMEM_FREE_BLOCK = 1,
    PMEM_IN_USED_BLOCK = 2,
    PMEM_IN_FLUSH_BLOCK=3,
	PMEM_PLACE_HOLDER_BLOCK=4,
	PMEM_DEL_MARK_BLOCK=5
};
enum PMEM_LOG_BUF_STATE{
	PMEM_LOG_BUF_FREE = 1,
	PMEM_LOG_BUF_IN_USED = 2,
	PMEM_LOG_BUF_IN_FLUSH = 3,
	PMEM_LOG_BUF_IN_PART = 4,
};

enum PMEM_REDO_PHASE{
	PMEM_REDO_PHASE1 = 1,
	PMEM_REDO_PHASE2 = 2,
};

enum PMEM_PARSE_RESULT {
	PMEM_PARSE_NEED = 1,
	PMEM_PARSE_BLOCK_NOT_EXISTED = 2,
	PMEM_PARSE_LSN_OLD = 3,
};

enum PMEM_LOG_BLOCK_STATE {
    PMEM_FREE_LOG_BLOCK = 1, //the log block is free, a transaction can write its log records
    PMEM_COMMIT_LOG_BLOCK = 2, // the transaction is either commit or abort
    PMEM_ACTIVE_LOG_BLOCK = 3, // the transaction is active
};

enum PMEM_TX_STATE {
    PMEM_TX_FREE = 1, 
    PMEM_TX_COMMIT = 2, 
    PMEM_TX_ACTIVE = 3,
};

enum PMEM_LOG_TYPE {
	PMEM_REDO_LOG = 1,
	PMEM_UNDO_LOG = 2
};

enum FLUSHER_TYPE{
	CATCHER_LOG_BUF = 1,
	FLUSHER_LOG_BUF = 2,
};

enum pm_list_cleaner_state {
	/** Not requested any yet.
	Moved from FINISHED by the coordinator. */
	LIST_CLEANER_STATE_NONE = 0,
	/** Requested but not started flushing.
	Moved from NONE by the coordinator. */
	LIST_CLEANER_STATE_REQUESTED,
	/** Flushing is on going.
	Moved from REQUESTED by the worker. */
	LIST_CLEANER_STATE_FLUSHING,
	/** Flushing was finished.
	Moved from FLUSHING by the worker. */
	LIST_CLEANER_STATE_FINISHED
};

//struct __pmem_aio_param;
//typedef struct __pmem_aio_param PMEM_AIO_PARAM;
//
//struct __pmem_aio_param {
//	const char* name;
//	void*		file;	
//	void*		buf;
//	uint64_t	offset;
//	uint64_t		n;	
//	void*		m1;
//	void*		m2;
//};

static inline int file_exists(char const *file);

/*
 *  * file_exists -- checks if file exists
 *   */
static inline int file_exists(char const *file)
{
	    return access(file, F_OK);
}

#endif
